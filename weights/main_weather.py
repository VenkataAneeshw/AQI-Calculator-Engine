import sqlite3
import json
import math
import pandas as pd
import numpy as np
from contextlib import contextmanager
from typing import Dict, Any

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
# WEATHER ENGINE CLASS
# ==========================================
class WeatherWeightEngine:
    def __init__(self, db_name: str = None):
        self.db_name = db_name or config.DB_NAME
        self._prepare_db()
        logger.info("WeatherWeightEngine initialized")

    def _prepare_db(self):
        """Adds the weather_weights_json column to measurements if missing."""
        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute("ALTER TABLE measurements ADD COLUMN weather_weights_json TEXT")
                conn.commit()
            logger.info("Column 'weather_weights_json' added to DB")
        except sqlite3.OperationalError:
            logger.debug("Column 'weather_weights_json' already exists")

    def _regularized_weight(self, raw_score: float) -> float:
        """
        THE REGULARIZATION FUNCTION.
        Input: A raw 'tendency' score (negative = clean, positive = dirty).
        Output: A safe multiplicative weight centered around 1.0.
        
        Formula: Weight = exp( Reg_Strength * tanh( raw_score ) )
        """
        damped_score = math.tanh(raw_score)
        scaled_score = damped_score * config.WEATHER_REGULARIZATION_STRENGTH
        return round(math.exp(scaled_score), 3)

    def calculate_weights(self, row: Dict[str, Any]) -> str:
        """
        Determines weights based on Physics + Regularization.
        
        Args:
            row: Dictionary with 'TEMP', 'RAIN', 'WSPM' keys
        
        Returns:
            JSON string of weights
        """
        # Unpack Data with safe defaults
        temp = row.get('TEMP', row.get('temp', 20))
        rain = row.get('RAIN', row.get('rain', 0))
        wspm = row.get('WSPM', row.get('wspm', 1.0))
        
        # Ensure numeric types
        try:
            temp = float(temp)
            rain = float(rain)
            wspm = float(wspm)
        except (TypeError, ValueError) as e:
            logger.warning(f"Weather data conversion error: {e}, using defaults")
            temp, rain, wspm = 20.0, 0.0, 1.0
        
        # --- 1. WIND SCORE (Ventilation vs Stagnation) ---
        if wspm < 0.1:
            wspm = 0.1
        
        wind_score = -1.0 * math.log(wspm / 2.0)

        # --- 2. RAIN SCORE (Washout) ---
        rain_score = 0
        if rain > 0:
            rain_score = -1.0 * math.log(rain + 1.0) * 2.0

        # --- 3. TEMP SCORE (Photochemistry) ---
        temp_score_o3 = 0
        if temp > config.TEMP_OZONE_TRIGGER:
            temp_score_o3 = (temp - config.TEMP_OZONE_TRIGGER) / 10.0

        # ==========================================
        # CONSTRUCTING THE WEIGHT VECTOR
        # ==========================================
        
        pm_raw = wind_score + rain_score
        gas_raw = wind_score + (rain_score * 0.3)
        ozone_raw = wind_score + temp_score_o3

        weights = {
            "PM2.5": self._regularized_weight(pm_raw),
            "PM10": self._regularized_weight(pm_raw),
            "NO2": self._regularized_weight(gas_raw),
            "SO2": self._regularized_weight(gas_raw),
            "O3": self._regularized_weight(ozone_raw),
            "CO": self._regularized_weight(gas_raw)
        }
        
        logger.debug(f"Weather weights calculated: {weights}")
        return json.dumps(weights)

    def run_batch_update(self):
        """
        Batch update weather weights for all measurements in database.
        """
        logger.info("Starting Weather Weight Calculation...")
        
        try:
            with get_db_connection(self.db_name) as conn:
                df = pd.read_sql("SELECT id, timestamp FROM measurements", conn)
        except Exception as e:
            logger.error(f"Failed to read measurements: {e}")
            return
        
        if df.empty:
            logger.warning("No measurements found")
            return

        logger.info(f"Processing {len(df)} rows...")
        
        updates = []
        
        for index, row in df.iterrows():
            # Simulation: Generate weather data
            # In production, fetch from weather API or database
            sim_weather = {
                'TEMP': np.random.uniform(-5, 35),
                'RAIN': 0 if np.random.random() > 0.1 else np.random.uniform(0, 10),
                'WSPM': np.random.uniform(0.1, 5)
            }
            
            weights_json = self.calculate_weights(sim_weather)
            updates.append((weights_json, int(row['id'])))
            
            if index % 5000 == 0 and index > 0:
                logger.info(f"Calculated {index} rows...")

        # Batch Update DB
        logger.info("Saving to Database...")
        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    "UPDATE measurements SET weather_weights_json = ? WHERE id = ?",
                    updates
                )
                conn.commit()
            logger.info("Weather Weights Updated Successfully")
        except Exception as e:
            logger.error(f"Database update failed: {e}")
            raise

# ==========================================
# MODULE INITIALIZATION
# ==========================================
_weather_engine = WeatherWeightEngine()

# ==========================================
# PUBLIC INTERFACE FOR MAIN.PY
# ==========================================
def get_weight(weather_data: Dict[str, Any], pollutant: str) -> float:
    """
    Returns the weather weight for a given pollutant.
    Used by main.py in the AQI calculation pipeline.
    
    Args:
        weather_data: Contains 'temp', 'humidity', 'wspm', 'rain' etc.
        pollutant: The pollutant name (PM2.5, NO2, O3, etc.)
    
    Returns:
        float: The weather weight for this pollutant (typically 0.5 to 2.0)
    """
    try:
        weights_json = _weather_engine.calculate_weights(weather_data)
        weights_dict = json.loads(weights_json)
        weight = weights_dict.get(pollutant, 1.0)
        logger.debug(f"Weather weight for {pollutant}: {weight}")
        return weight
    except Exception as e:
        logger.error(f"Error calculating weather weight for {pollutant}: {e}")
        return 1.0

# ==========================================
# EXECUTION
# ==========================================
if __name__ == "__main__":
    engine = WeatherWeightEngine()
    engine.run_batch_update()
    
    # Verification
    print("\n\U0001F50D Verification: Random Sample")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, weather_weights_json FROM measurements ORDER BY RANDOM() LIMIT 3")
            for r in cursor.fetchall():
                print(f"ID {r[0]}: {r[1]}")
    except Exception as e:
        logger.error(f"Verification failed: {e}")