import sqlite3
import json
import pandas as pd
import numpy as np
from contextlib import contextmanager
from typing import List

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
# ENTROPY WEIGHT ENGINE CLASS
# ==========================================
class EntropyWeightEngine:
    def __init__(self, db_name: str = None):
        self.db_name = db_name or config.DB_NAME
        self._prepare_db()
        logger.info("EntropyWeightEngine initialized")

    def _prepare_db(self):
        """Ensures the storage column exists."""
        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute("ALTER TABLE measurements ADD COLUMN entropy_weights_json TEXT")
                conn.commit()
            logger.info("Column 'entropy_weights_json' added to DB")
        except sqlite3.OperationalError:
            logger.debug("Column 'entropy_weights_json' already exists")

    def run_batch_processing(self):
        """
        Batch process entropy weights for all measurements.
        """
        logger.info("Starting Entropy Calculation...")
        
        # 1. LOAD DATA
        query = """
            SELECT id, station_id, timestamp, 
                   pm25 as `PM2.5`, pm10 as `PM10`, no2 as `NO2`, 
                   so2 as `SO2`, co as `CO`, o3 as `O3`
            FROM measurements 
            ORDER BY station_id, timestamp
        """
        
        try:
            with get_db_connection(self.db_name) as conn:
                df = pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Critical Error loading data: {e}")
            return

        if df.empty:
            logger.warning("No data found in 'measurements' table")
            return

        logger.info(f"Loaded {len(df)} rows. Processing...")

        updates = []
        
        # 2. GROUP BY STATION
        grouped = df.groupby('station_id')
        
        for station_name, station_data in grouped:
            logger.debug(f"Processing station: {station_name}")
            
            # 3. CALCULATE (Vectorized)
            rolling_std = station_data[config.POLLUTANTS].rolling(window=config.WINDOW_SIZE).std()
            rolling_mean = station_data[config.POLLUTANTS].rolling(window=config.WINDOW_SIZE).mean() + 1e-6
            
            rolling_cv = (rolling_std / rolling_mean).fillna(0)
            rolling_cv = rolling_cv + config.REGULARIZATION_LAMBDA
            
            row_sums = rolling_cv.sum(axis=1)
            row_sums[row_sums == 0] = 1.0
            
            num_pol = len(config.POLLUTANTS)
            weights_df = (rolling_cv.div(row_sums, axis=0) * num_pol) ** config.ENTROPY_SENSITIVITY
            weights_df = weights_df.clip(lower=config.MIN_WEIGHT_CLAMP, upper=config.MAX_WEIGHT_CLAMP)
            
            # 4. PREPARE UPDATES (with explicit type conversion)
            for idx, row in weights_df.iterrows():
                db_id = int(station_data.loc[idx, 'id'])
                
                w_dict = {
                    "PM2.5": round(float(row['PM2.5']), 3),
                    "PM10": round(float(row['PM10']), 3),
                    "NO2": round(float(row['NO2']), 3),
                    "SO2": round(float(row['SO2']), 3),
                    "CO": round(float(row['CO']), 3),
                    "O3": round(float(row['O3']), 3)
                }
                
                updates.append((json.dumps(w_dict), db_id))

        # 5. EXECUTE UPDATES
        if updates:
            logger.info(f"Generated {len(updates)} weight vectors")
            logger.info("Committing to Database...")
            
            try:
                with get_db_connection(self.db_name) as conn:
                    cursor = conn.cursor()
                    cursor.executemany(
                        "UPDATE measurements SET entropy_weights_json = ? WHERE id = ?",
                        updates
                    )
                    conn.commit()
                    logger.info(f"Success! {cursor.rowcount} rows updated")
            except Exception as e:
                logger.error(f"SQL Update Failed: {e}")
                raise
        else:
            logger.warning("No updates generated")

    def verify(self):
        """Checks if data actually landed in the DB."""
        logger.info("FINAL VERIFICATION")
        
        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM measurements WHERE entropy_weights_json IS NOT NULL")
                count = cursor.fetchone()[0]
                logger.info(f"Rows with Entropy Weights: {count}")
                
                if count > 0:
                    cursor.execute("SELECT entropy_weights_json FROM measurements WHERE entropy_weights_json IS NOT NULL LIMIT 1")
                    sample = cursor.fetchone()[0]
                    logger.info(f"Sample Data: {sample}")
                else:
                    logger.warning("Table is still empty of weights")
        except Exception as e:
            logger.error(f"Verification failed: {e}")

# ==========================================
# MODULE INITIALIZATION
# ==========================================
_engine = EntropyWeightEngine()

# ==========================================
# PUBLIC INTERFACE FOR MAIN.PY
# ==========================================
def get_weight(history_list: List[float]) -> float:
    """
    Returns the entropy weight based on historical data volatility.
    Used by main.py in the AQI calculation pipeline.
    
    Args:
        history_list: Historical pollutant values
    
    Returns:
        float: Entropy weight (0.1 to 3.0), where higher = more volatile/unstable
    """
    if not history_list or len(history_list) < 2:
        logger.debug("Insufficient history for entropy calculation, using default")
        return 0.5
    
    try:
        arr = np.array(history_list, dtype=float)
        mean = np.mean(arr)
        
        if mean == 0:
            return 0.5
        
        std = np.std(arr)
        cv = std / mean
        
        weight = ((cv + config.REGULARIZATION_LAMBDA) ** config.ENTROPY_SENSITIVITY)
        weight = np.clip(weight, config.MIN_WEIGHT_CLAMP, config.MAX_WEIGHT_CLAMP)
        
        logger.debug(f"Entropy weight calculated: {float(weight):.3f}")
        return float(weight)
    except Exception as e:
        logger.error(f"Error calculating entropy weight: {e}")
        return 0.5

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    engine = EntropyWeightEngine()
    engine.run_batch_processing()
    engine.verify()