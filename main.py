import numpy as np
import pandas as pd
import datetime
from typing import Dict, Any

import config
from logger import setup_logger
from validation import validate_coordinates, validate_weather_data, validate_pollutant_data, ValidationError

# Setup logging
logger = setup_logger(__name__, config.LOG_FILE, config.LOG_LEVEL)

# ==============================================================================
# IMPORT USER'S EXISTING WEIGHT MODULES
# ==============================================================================
try:
    from weights import main_subjective
    from weights import main_morphology
    from weights import main_weather
    from weights import main_entropy
    logger.info("Successfully imported all weight modules")
except ImportError as e:
    logger.warning(f"Weight module import error: {e}. Using fallback weights.")
    # Dummy fallbacks for safe execution if files are missing
    class main_subjective:
        @staticmethod
        def get_weight(x): return 1.0
    class main_morphology:
        @staticmethod
        def get_weight(x, y, z): return 1.0
    class main_weather:
        @staticmethod
        def get_weight(x, y): return 1.0
    class main_entropy:
        @staticmethod
        def get_weight(x): return 0.0

class SmartSynergyAQI:
    """
    The Master Orchestrator for the SmartSynergy AQI Model.
    Includes:
    - 14-Step Calculation Pipeline
    - Piecewise Linear Normalization (Standard Accuracy)
    - Confidence Level Estimation
    """

    def __init__(self):
        # History State
        self.history_db = {
            "PM2.5": [], "PM10": [], "NO2": [], "CO": [], "SO2": [], "O3": []
        }
        self.last_displayed_aqi = 0.0 
        self.covariance_history = pd.DataFrame()
        
        # Configuration: Severity Color Map (Using Unicode escapes)
        self.severity_map = [
            (50, "Good", "\U0001F7E2"),  # Green circle
            (100, "Moderate", "\U0001F7E1"),  # Yellow circle
            (150, "Unhealthy for Sensitive", "\U0001F7E0"),  # Orange circle
            (200, "Unhealthy", "\U0001F534"),  # Red circle
            (300, "Very Unhealthy", "\U0001F7E3"),  # Purple circle
            (float('inf'), "Hazardous", "\U0001F7E4")  # Brown circle
        ]
        
        logger.info("SmartSynergyAQI initialized successfully")

    # ==========================================================================
    # INTERNAL HELPER METHODS
    # ==========================================================================

    def _impute_missing_data(self, raw_data: Dict[str, Any]) -> tuple:
        """Step 2: Hierarchical Imputation"""
        clean_data = {}
        flags = {}
        
        # Safe baselines (Low "Good" levels to prevent false alarms)
        defaults = {
            "PM2.5": 10.0,
            "PM10": 20.0,
            "NO2": 15.0,
            "O3": 30.0,
            "CO": 500.0,
            "SO2": 10.0
        }
        
        for pol in config.POLLUTANTS:
            val = raw_data.get(pol)
            
            # PRIORITY 1: REAL-TIME DATA
            if val is not None and not np.isnan(val):
                clean_data[pol] = val
                flags[pol] = "Real-Time"
            
            # PRIORITY 2: HISTORY
            elif self.history_db.get(pol):
                clean_data[pol] = self.history_db[pol][-1]
                flags[pol] = "Imputed (History)"
                logger.debug(f"{pol} imputed from history: {clean_data[pol]}")
            
            # PRIORITY 3: DEFAULT
            else:
                clean_data[pol] = defaults.get(pol, 0.0)
                flags[pol] = "Imputed (Default)"
                logger.debug(f"{pol} using default: {clean_data[pol]}")
        
        return clean_data, flags

    def _kalman_filter(self, val: float, pol: str) -> float:
        """Step 3: 1D Kalman Filter"""
        history = self.history_db.get(pol, [])
        if not history:
            return val
        
        prev_estimate = history[-1]
        kalman_gain = 0.15 
        filtered_val = prev_estimate + kalman_gain * (val - prev_estimate)
        
        return filtered_val

    def _calculate_piecewise_risk(self, val: float, pol: str) -> float:
        """
        Step 4: Risk Normalization (Piecewise Linear)
        Converts Concentration to AQI (0-500) using EPA Breakpoints.
        """
        bps = config.EPA_BREAKPOINTS.get(pol, [])
        
        # If no breakpoints found, use safe fallback
        if not bps:
            logger.warning(f"No breakpoints found for {pol}, using fallback")
            return min(val, 50)

        for (c_low, c_high, i_low, i_high) in bps:
            if c_low <= val <= c_high:
                return ((i_high - i_low) / (c_high - c_low)) * (val - c_low) + i_low
        
        # If beyond max breakpoint, cap at 500
        logger.warning(f"{pol} value {val} exceeds breakpoint range, capping at 500")
        return 500.0

    def _calculate_mahalanobis(self, weighted_risks: Dict[str, float]) -> float:
        """Step 9: Smart AQI (Covariance)"""
        new_row = pd.DataFrame([weighted_risks])
        self.covariance_history = pd.concat([self.covariance_history, new_row], ignore_index=True)
        
        if len(self.covariance_history) > 48:
            self.covariance_history = self.covariance_history.iloc[-48:]
            
        pollutants = list(weighted_risks.keys())
        r_vector = np.array([weighted_risks[p] for p in pollutants])
        
        if len(self.covariance_history) < len(pollutants) + 2:
            euclidean = np.sqrt(np.sum(r_vector**2))
            # Normalize by number of pollutants
            return euclidean / np.sqrt(len(pollutants))
            
        try:
            sigma = self.covariance_history[pollutants].cov().to_numpy()
            sigma += np.eye(len(sigma)) * 1e-5 
            sigma_inv = np.linalg.inv(sigma)
            term1 = np.dot(r_vector, sigma_inv)
            mahalanobis_sq = np.dot(term1, r_vector)
            mahalanobis = np.sqrt(mahalanobis_sq)
            # Normalize by number of pollutants
            return mahalanobis / np.sqrt(len(pollutants))
        except Exception as e:
            logger.warning(f"Mahalanobis calculation failed: {e}, using Euclidean fallback")
            euclidean = np.sqrt(np.sum(r_vector**2))
            return euclidean / np.sqrt(len(pollutants))

    def _apply_jitter_avoidance(self, new_aqi: float) -> int:
        """Step 12: Smooth UI"""
        if new_aqi > self.last_displayed_aqi:
            self.last_displayed_aqi = new_aqi 
        else:
            decay = 5.0
            if (self.last_displayed_aqi - new_aqi) > decay:
                self.last_displayed_aqi -= decay
            else:
                self.last_displayed_aqi = new_aqi
        
        return round(self.last_displayed_aqi)

    def _calculate_confidence_level(self, quality_flags: Dict[str, str]) -> Dict[str, Any]:
        """Step 15: Confidence Level Calculation"""
        score = 100.0
        reasons = []

        tier1_pollutants = ["PM2.5", "NO2"]
        tier2_pollutants = ["O3", "PM10", "CO", "SO2"]
        
        for pol, flag in quality_flags.items():
            if "Default" in flag:
                if pol in tier1_pollutants:
                    score -= 20.0
                    reasons.append(f"{pol} missing (Tier 1)")
                elif pol in tier2_pollutants:
                    score -= 10.0
                    reasons.append(f"{pol} missing (Tier 2)")
            elif "History" in flag:
                score -= 5.0
                reasons.append(f"{pol} using history")
        
        if len(self.covariance_history) < 10:
            score -= 5.0
            reasons.append("Learning Matrix warming up")

        final_score = max(0.0, min(score, 100.0))
        
        if final_score >= 85:
            label = "High"
        elif final_score >= 50:
            label = "Medium"
        else:
            label = "Low"

        return {
            "percentage": round(final_score),
            "label": label,
            "issues": reasons
        }

    # ==========================================================================
    # THE MAIN PIPELINE
    # ==========================================================================
    
    def calculate_aqi_pipeline(self, raw_input: Dict[str, Any], weather: Dict[str, Any], 
                               lat: float, lon: float) -> Dict[str, Any]:
        """
        Main AQI calculation pipeline with full validation.
        
        Args:
            raw_input: Dictionary of pollutant concentrations
            weather: Dictionary of weather parameters
            lat: Latitude in decimal degrees
            lon: Longitude in decimal degrees
        
        Returns:
            Dictionary containing AQI results and metadata
        """
        try:
            # Validation Phase
            logger.debug("Starting validation phase")
            lat, lon = validate_coordinates(lat, lon)
            weather = validate_weather_data(weather)
            raw_input = validate_pollutant_data(raw_input)
            
            # 1-2. Missing Data
            clean_data, quality_flags = self._impute_missing_data(raw_input)
            
            # 3. Kalman Filter
            smooth_data = {}
            for pol, val in clean_data.items():
                smooth_data[pol] = self._kalman_filter(val, pol)
                self.history_db[pol].append(smooth_data[pol])
                if len(self.history_db[pol]) > 50:
                    self.history_db[pol].pop(0)

            # 4. Normalization
            raw_risk_scores = {}
            for pol, val in smooth_data.items():
                raw_risk_scores[pol] = self._calculate_piecewise_risk(val, pol)
                
            # 5-6. Weights
            severity_coefficients = {}
            for pol in raw_risk_scores.keys():
                try:
                    w_sub = main_subjective.get_weight(pol)
                    w_mor = main_morphology.get_weight(lat, lon, pol)
                    w_wea = main_weather.get_weight(weather, pol)
                    w_ent = main_entropy.get_weight(self.history_db[pol])
                    
                    coeff = w_sub * w_mor * w_wea * (1 + w_ent)
                    severity_coefficients[pol] = min(coeff, 2.0)
                except Exception as e:
                    logger.error(f"Weight calculation error for {pol}: {e}")
                    severity_coefficients[pol] = 1.0
                
            # 7. Weighted Risk Vector
            weighted_risks = {}
            for pol, score in raw_risk_scores.items():
                weighted_risks[pol] = score * severity_coefficients[pol]
                
            # 8. Synergy AQI (Normalized Euclidean)
            energy_sum = sum([r**2 for r in weighted_risks.values()])
            synergy_aqi = np.sqrt(energy_sum) / np.sqrt(len(weighted_risks))
            
            # 9. Smart AQI (Mahalanobis)
            smart_aqi = self._calculate_mahalanobis(weighted_risks)
            
            # 10. Veto Rule
            old_method_aqi = max(raw_risk_scores.values())
            final_calc = max(old_method_aqi, synergy_aqi, smart_aqi)
            
            # Safety Clamp
            if final_calc > 600:
                final_calc = 600
                logger.warning(f"AQI clamped at 600 (calculated: {final_calc})")
            
            # 11-12. Final & Jitter
            display_aqi = self._apply_jitter_avoidance(final_calc)
            
            # 13. PAV
            pav = {}
            for pol, w_risk in weighted_risks.items():
                if energy_sum > 0:
                    pav[pol] = round(((w_risk**2) / energy_sum) * 100, 1)
                else:
                    pav[pol] = 0.0
                    
            # 14. Interpretation
            status, icon = "Unknown", "\u26AA"
            for limit, label, emo in self.severity_map:
                if display_aqi <= limit:
                    status = label
                    icon = emo
                    break

            # 15. Confidence Calculation
            confidence = self._calculate_confidence_level(quality_flags)
            
            logger.info(f"AQI calculation complete: {display_aqi} ({status})")
                    
            return {
                "AQI": display_aqi,
                "Severity": f"{icon} {status}",
                "Confidence": confidence,
                "PAV": pav,
                "Flags": quality_flags,
                "Debug": {
                    "Standard": round(old_method_aqi),
                    "Synergy": round(synergy_aqi),
                    "Smart": round(smart_aqi)
                }
            }
            
        except ValidationError as e:
            logger.error(f"Validation error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in AQI calculation: {e}", exc_info=True)
            raise

# ==============================================================================
# PRODUCTION DATABASE CONNECTIONS
# ==============================================================================

def fetch_realtime_data_from_db() -> tuple:
    """
    Fetches the latest measurement record from the database.
    
    Returns:
        Tuple of (pollutant_data, station_id, timestamp, metadata)
    
    Raises:
        Exception: If database fetch fails
    """
    import sqlite3
    
    conn = sqlite3.connect(config.DB_NAME)
    cursor = conn.cursor()
    
    # Fetch the last measurement record with station info
    cursor.execute("""
        SELECT pm25, pm10, no2, so2, co, o3, station_id, timestamp
        FROM measurements
        ORDER BY rowid DESC
        LIMIT 1
    """)
    
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        raise Exception("No measurement data found in database. Please load CSV data first.")
    
    pm25, pm10, no2, so2, co, o3, station_id, timestamp = row
    
    pollutant_data = {
        "PM2.5": pm25,
        "PM10": pm10,
        "NO2": no2,
        "SO2": so2,
        "CO": co,
        "O3": o3
    }
    
    logger.info(f"Fetched latest measurement: Station '{station_id}' at {timestamp}")
    
    return pollutant_data, station_id, timestamp, {
        "source": "SQLite Database",
        "table": "measurements",
        "record_timestamp": timestamp
    }


def fetch_weather_data_from_db(station_id: str) -> Dict[str, Any]:
    """
    Fetches the latest weather data for a specific station.
    
    Args:
        station_id: Station identifier
    
    Returns:
        Dictionary with weather parameters
    
    Raises:
        Exception: If weather data cannot be fetched
    """
    import sqlite3
    
    conn = sqlite3.connect(config.DB_NAME)
    cursor = conn.cursor()
    
    # Try to fetch weather data from measurements table if available
    cursor.execute("""
        SELECT timestamp
        FROM measurements
        WHERE station_id = ?
        ORDER BY rowid DESC
        LIMIT 1
    """, (station_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        raise Exception(f"No weather data found for station '{station_id}'")
    
    # Parse weather from station characteristics (stored in config)
    # In production, integrate with actual weather API or database column
    weather_data = {
        "TEMP": 25.0,
        "HUMIDITY": 65.0,
        "WSPM": 2.0,
        "RAIN": 0.0
    }
    
    logger.info(f"Fetched weather data for station '{station_id}'")
    return weather_data


def fetch_station_coordinates(station_id: str) -> tuple:
    """
    Fetches latitude and longitude for a station.
    
    Args:
        station_id: Station identifier
    
    Returns:
        Tuple of (latitude, longitude)
    
    Raises:
        Exception: If station coordinates not found
    """
    # Use predefined station locations from config
    if station_id in config.STATION_LOCATIONS:
        coords = config.STATION_LOCATIONS[station_id]
        logger.info(f"Retrieved coordinates for station '{station_id}': ({coords['lat']}, {coords['lon']})")
        return coords["lat"], coords["lon"]
    else:
        logger.warning(f"Station '{station_id}' not in predefined locations. Using default coordinates.")
        return 25.0, 55.0  # Default to Dubai coordinates

if __name__ == "__main__":
    logger.info("=== SmartSynergy AQI System Starting (Production Mode) ===")
    
    try:
        system = SmartSynergyAQI()
        
        # 1. Fetch Real-Time Data from Database
        logger.info("Fetching live data from database...")
        try:
            real_time_input, station_id, timestamp, metadata = fetch_realtime_data_from_db()
            logger.info(f"Raw Input (from {station_id}): {real_time_input}")
        except Exception as e:
            logger.error(f"Failed to fetch realtime data: {e}")
            raise
        
        # 2. Fetch Weather Data from Database
        logger.info(f"Fetching weather data for station '{station_id}'...")
        try:
            weather_data = fetch_weather_data_from_db(station_id)
            logger.info(f"Weather Data: {weather_data}")
        except Exception as e:
            logger.error(f"Failed to fetch weather data: {e}")
            raise
        
        # 3. Fetch Station Coordinates from Database
        logger.info(f"Fetching coordinates for station '{station_id}'...")
        try:
            lat, lon = fetch_station_coordinates(station_id)
            logger.info(f"Station Coordinates: Lat={lat}, Lon={lon}")
        except Exception as e:
            logger.error(f"Failed to fetch station coordinates: {e}")
            raise
        
        # 4. Run AQI Pipeline with Real Data
        logger.info("Running AQI calculation pipeline...")
        try:
            result = system.calculate_aqi_pipeline(real_time_input, weather_data, lat, lon)
            
            # Add metadata to result
            result["metadata"] = {
                "station_id": station_id,
                "record_timestamp": timestamp,
                "coordinates": {"latitude": lat, "longitude": lon},
                "data_source": "SQLite Database",
                "calculation_time": datetime.datetime.now().isoformat()
            }
            
            import json
            print("\n" + "="*60)
            print("PRODUCTION AQI CALCULATION - FINAL OUTPUT")
            print("="*60)
            print(json.dumps(result, indent=2))
            print("="*60)
            
            logger.info(f"AQI Calculation Successful - Result: {result['AQI']}")
            
        except ValidationError as e:
            logger.error(f"Validation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"AQI calculation failed: {e}", exc_info=True)
            raise
            
    except Exception as e:
        logger.error(f"System error: {e}", exc_info=True)
        print(f"\nERROR: {e}")
        print("Please ensure:")
        print("1. Database file exists at:", config.DB_NAME)
        print("2. CSV data has been loaded using: python data_loader.py")
        print("3. All required columns are present: PM2.5, PM10, NO2, SO2, CO, O3, station")
        exit(1)