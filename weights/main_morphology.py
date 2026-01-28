import sqlite3
import json
import torch
import torch.nn as nn
from torchvision import models, transforms
from PIL import Image
import requests
from io import BytesIO
import numpy as np
from contextlib import contextmanager
from typing import Dict, Tuple

import config
from logger import setup_logger

logger = setup_logger(__name__, config.LOG_FILE, config.LOG_LEVEL)

# ==========================================
# DATABASE CONTEXT MANAGER
# ==========================================
@contextmanager
def get_db_connection(db_name: str = None):
    """Context manager for database connections."""
    db_name = db_name or config.DB_NAME
    conn = sqlite3.connect(db_name)
    try:
        yield conn
    finally:
        conn.close()

# ==========================================
# DATABASE MANAGER
# ==========================================
class DatabaseManager:
    def __init__(self, db_name: str = None):
        self.db_name = db_name or config.DB_NAME
        self._create_tables()
        logger.info("DatabaseManager initialized")

    def _create_tables(self):
        """Creates necessary tables if they don't exist."""
        with get_db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            
            # Stations table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stations (
                    station_id TEXT PRIMARY KEY,
                    latitude REAL,
                    longitude REAL,
                    morphology_class TEXT,
                    morphology_weights_json TEXT
                )
            ''')
            
            # Measurements table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS measurements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_id TEXT,
                    timestamp DATETIME,
                    pm25 REAL,
                    pm10 REAL,
                    no2 REAL,
                    so2 REAL,
                    co REAL,
                    o3 REAL,
                    FOREIGN KEY(station_id) REFERENCES stations(station_id)
                )
            ''')
            conn.commit()
        logger.debug("Database tables created/verified")

    def register_station(self, name: str, lat: float, lon: float):
        """Ensures station exists in DB."""
        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO stations (station_id, latitude, longitude) VALUES (?, ?, ?)", 
                    (name, lat, lon)
                )
                conn.commit()
            logger.debug(f"Station registered: {name}")
        except Exception as e:
            logger.error(f"DB Error registering station: {e}")

    def update_morphology_weight(self, station_id: str, morphology_class: str, weights_dict: Dict):
        """Updates the morphology column for a specific station."""
        weights_json = json.dumps(weights_dict)
        
        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE stations 
                    SET morphology_class = ?, morphology_weights_json = ? 
                    WHERE station_id = ?
                ''', (morphology_class, weights_json, station_id))
                conn.commit()
            logger.info(f"Saved '{morphology_class}' weights for {station_id}")
        except Exception as e:
            logger.error(f"DB Update Error: {e}")
            raise

    def get_stored_weight(self, station_id: str) -> Dict:
        """Checks if we already have weights calculated."""
        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT morphology_weights_json FROM stations WHERE station_id = ?", 
                    (station_id,)
                )
                result = cursor.fetchone()
            
            if result and result[0]:
                return json.loads(result[0])
            return None
        except Exception as e:
            logger.error(f"Error retrieving stored weights: {e}")
            return None

    def insert_measurement(self, station_id: str, timestamp: str, data: Dict):
        """Helper to insert measurement data."""
        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO measurements (station_id, timestamp, pm25, pm10, no2, so2, co, o3)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    station_id, timestamp, 
                    data.get('PM2.5'), data.get('PM10'), 
                    data.get('NO2'), data.get('SO2'),
                    data.get('CO'), data.get('O3')
                ))
                conn.commit()
            logger.debug(f"Measurement inserted for {station_id}")
        except Exception as e:
            logger.error(f"Error inserting measurement: {e}")

    def get_data_for_processing(self, station_id: str, target_hour: str) -> Dict:
        """
        Joins Measurements with Station Morphology to get the full context.
        """
        query = '''
            SELECT 
                m.timestamp, m.pm25, m.pm10, m.no2, m.so2, m.co, m.o3,
                s.morphology_weights_json
            FROM measurements m
            JOIN stations s ON m.station_id = s.station_id
            WHERE m.station_id = ? AND m.timestamp = ?
        '''
        
        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute(query, (station_id, target_hour))
                row = cursor.fetchone()
            
            if row:
                return {
                    "timestamp": row[0],
                    "raw_readings": {
                        "PM2.5": row[1], "PM10": row[2], 
                        "NO2": row[3], "SO2": row[4],
                        "CO": row[5], "O3": row[6]
                    },
                    "morphology_weights": json.loads(row[7]) if row[7] else {}
                }
            return None
        except Exception as e:
            logger.error(f"Error retrieving data for processing: {e}")
            return None

# ==========================================
# MORPHOLOGY ENGINE (CNN)
# ==========================================
class UrbanMorphologyEngine:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Using device: {self.device}")
        
        self.model = self._build_model()
        self.preprocess = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])
        logger.info("UrbanMorphologyEngine initialized")

    def _build_model(self):
        """Builds the CNN model."""
        try:
            model = models.resnet18(pretrained=True)
            num_ftrs = model.fc.in_features
            model.fc = nn.Linear(num_ftrs, len(config.CLASS_NAMES))
            model.eval()
            return model.to(self.device)
        except Exception as e:
            logger.error(f"Model building error: {e}")
            raise

    def _fetch_satellite_image(self, lat: float, lon: float) -> Image.Image:
        """Fetches satellite image from Google Maps API."""
        
        # Check if API key is configured
        if not config.GOOGLE_MAPS_API_KEY:
            logger.warning("Google Maps API key not configured, using mock image")
            return Image.fromarray(
                np.random.randint(0, 255, (256, 256, 3), dtype=np.uint8)
            )
        
        try:
            url = "https://maps.googleapis.com/maps/api/staticmap"
            params = {
                'center': f"{lat},{lon}",
                'zoom': 16,
                'size': '400x400',
                'maptype': 'satellite',
                'key': config.GOOGLE_MAPS_API_KEY
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            return Image.open(BytesIO(response.content)).convert('RGB')
        
        except requests.RequestException as e:
            logger.error(f"Failed to fetch satellite image: {e}")
            logger.warning("Using mock image")
            return Image.fromarray(
                np.random.randint(0, 255, (256, 256, 3), dtype=np.uint8)
            )

    def process_station(self, station_name: str) -> Dict:
        """
        Orchestrates the Logic:
        1. Check DB.
        2. If missing, Fetch Image -> Run CNN -> Update DB.
        3. Return Weights.
        """
        # 1. Check DB first (Optimization)
        stored_weights = self.db.get_stored_weight(station_name)
        if stored_weights:
            logger.info(f"Cache hit: Found existing weights for {station_name}")
            return stored_weights

        # 2. Get Coords
        if station_name not in config.STATION_LOCATIONS:
            logger.warning(f"Unknown station: {station_name}, using Residential defaults")
            return config.MORPHOLOGY_WEIGHTS["Residential"]
        
        lat = config.STATION_LOCATIONS[station_name]['lat']
        lon = config.STATION_LOCATIONS[station_name]['lon']

        # 3. Fetch & Analyze
        logger.info(f"Processing visuals for {station_name}...")
        
        try:
            img = self._fetch_satellite_image(lat, lon)
            
            input_tensor = self.preprocess(img).unsqueeze(0).to(self.device)
            
            with torch.no_grad():
                output = self.model(input_tensor)
            
            # Get Class
            probs = torch.nn.functional.softmax(output[0], dim=0)
            top_prob, top_catid = torch.topk(probs, 1)
            predicted_class = config.CLASS_NAMES[top_catid.item()]
            
            logger.info(f"Predicted morphology for {station_name}: {predicted_class} ({top_prob.item():.2%})")
            
        except Exception as e:
            logger.error(f"CNN processing error: {e}, using Residential defaults")
            predicted_class = "Residential"
        
        # 4. Get Weights & Update DB
        weights = config.MORPHOLOGY_WEIGHTS.get(predicted_class, config.MORPHOLOGY_WEIGHTS["Residential"])
        
        try:
            self.db.update_morphology_weight(station_name, predicted_class, weights)
        except Exception as e:
            logger.error(f"Failed to update DB with morphology weights: {e}")
        
        return weights

# ==========================================
# MODULE INITIALIZATION
# ==========================================
_db = DatabaseManager()
_morph_engine = UrbanMorphologyEngine(_db)

# ==========================================
# PUBLIC INTERFACE FOR MAIN.PY
# ==========================================
def get_weight(lat: float, lon: float, pollutant: str) -> float:
    """
    Returns the morphology weight for a given pollutant at a location.
    Used by main.py in the AQI calculation pipeline.
    
    Args:
        lat: Latitude
        lon: Longitude
        pollutant: Pollutant name
    
    Returns:
        float: Morphology weight
    """
    # Use Residential as default for unknown locations
    default_weights = config.MORPHOLOGY_WEIGHTS.get("Residential", {})
    weight = default_weights.get(pollutant, 1.0)
    logger.debug(f"Morphology weight for {pollutant}: {weight}")
    return weight

# ==========================================
# MAIN EXECUTION FLOW
# ==========================================
if __name__ == "__main__":
    db = DatabaseManager()
    morph_engine = UrbanMorphologyEngine(db)
    
    # Register and process a station
    station_name = "Wanliu"
    
    if station_name in config.STATION_LOCATIONS:
        loc = config.STATION_LOCATIONS[station_name]
        db.register_station(station_name, loc['lat'], loc['lon'])
        
        # Step 1: Morphology Analysis
        print("\n--- Step 1: Morphology Analysis ---")
        weights = morph_engine.process_station(station_name)
        print(f"Weights: {weights}")
        
        # Step 2: Data Ingestion
        print("\n--- Step 2: Ingesting Hourly Data ---")
        target_time = "2024-03-01 14:00:00"
        sample_data = {"PM2.5": 55, "PM10": 120, "NO2": 85, "SO2": 15, "CO": 800, "O3": 40}
        db.insert_measurement(station_name, target_time, sample_data)
        print(f"Data stored for {target_time}")
        
        # Step 3: Retrieval
        print("\n--- Step 3: Main Function Retrieval ---")
        context_data = db.get_data_for_processing(station_name, target_time)
        
        if context_data:
            print(f"Retrieved Context:")
            print(f"  Time: {context_data['timestamp']}")
            print(f"  Raw Readings: {context_data['raw_readings']}")
            print(f"  Morphology Modifiers: {context_data['morphology_weights']}")
            
            if context_data['morphology_weights']:
                final_risk = context_data['raw_readings']['PM2.5'] * context_data['morphology_weights']['PM2.5']
                print(f"  Final Risk (PM2.5): {final_risk:.2f}")
    else:
        logger.error(f"Station {station_name} not found in configuration")