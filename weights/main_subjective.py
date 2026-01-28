import sqlite3
import json
from contextlib import contextmanager

import config
from logger import setup_logger

logger = setup_logger(__name__, config.LOG_FILE, config.LOG_LEVEL)

# ==========================================
# DATABASE CONTEXT MANAGER
# ==========================================
@contextmanager
def get_db_connection(db_name: str = None):
    """
    Context manager for database connections.
    Ensures proper connection cleanup.
    """
    db_name = db_name or config.DB_NAME
    conn = sqlite3.connect(db_name)
    try:
        yield conn
    finally:
        conn.close()

# ==========================================
# SUBJECTIVE WEIGHT MANAGER CLASS
# ==========================================
class SubjectiveWeightManager:
    def __init__(self, db_name: str = None):
        self.db_name = db_name or config.DB_NAME
        self._prepare_db()
        logger.info("SubjectiveWeightManager initialized")

    def _prepare_db(self):
        """
        Creates a dedicated reference table for biological constants.
        """
        with get_db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS global_biological_weights (
                    pollutant TEXT PRIMARY KEY,
                    weight REAL,
                    description TEXT
                )
            ''')
            conn.commit()
        logger.debug("Database schema prepared")

    def update_db_weights(self):
        """
        Updates the reference table with the hardcoded standards.
        """
        logger.info("Updating Biological Weights in Database...")
        
        descriptions = {
            "PM2.5": "Systemic toxicity; enters bloodstream",
            "O3": "Oxidative stress; lung tissue damage",
            "NO2": "Inflammatory; asthma trigger",
            "SO2": "Irritant; upper respiratory constriction",
            "PM10": "Obstructive; trapped in upper airways",
            "CO": "Asphyxiant; binds hemoglobin"
        }

        records = []
        for pollutant, weight in config.BIOLOGICAL_STANDARDS.items():
            desc = descriptions.get(pollutant, "Standard Toxicity")
            records.append((pollutant, weight, desc))

        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    "INSERT OR REPLACE INTO global_biological_weights (pollutant, weight, description) VALUES (?, ?, ?)",
                    records
                )
                conn.commit()
            logger.info(f"Successfully updated {len(records)} biological constants")
        except Exception as e:
            logger.error(f"Database update error: {e}")
            raise

    def get_weights_as_dict(self) -> dict:
        """
        Helper for the Main Calculator to fetch these easily.
        Returns: {'PM2.5': 1.5, 'NO2': 1.3 ...}
        """
        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT pollutant, weight FROM global_biological_weights")
                rows = cursor.fetchall()
            
            weights = {row[0]: row[1] for row in rows}
            logger.debug(f"Retrieved {len(weights)} biological weights from database")
            return weights
        except Exception as e:
            logger.error(f"Error retrieving weights: {e}")
            return config.BIOLOGICAL_STANDARDS.copy()

# ==========================================
# MODULE INITIALIZATION
# ==========================================
_manager = SubjectiveWeightManager()
_manager.update_db_weights()
_weights_cache = _manager.get_weights_as_dict()

# ==========================================
# PUBLIC INTERFACE FOR MAIN.PY
# ==========================================
def get_weight(pollutant: str) -> float:
    """
    Returns the biological weight for a given pollutant.
    Used by main.py in the AQI calculation pipeline.
    
    Args:
        pollutant: Name of the pollutant (PM2.5, NO2, O3, etc.)
    
    Returns:
        float: Biological weight (0.5 to 1.5)
    """
    weight = _weights_cache.get(pollutant, 1.0)
    logger.debug(f"Biological weight for {pollutant}: {weight}")
    return weight

# ==========================================
# EXECUTION
# ==========================================
if __name__ == "__main__":
    manager = SubjectiveWeightManager()
    
    # 1. Update the DB
    manager.update_db_weights()
    
    # 2. Verify what is stored
    print("\n\U0001F50D Verification (Reading from DB):")
    stored_weights = manager.get_weights_as_dict()
    print(json.dumps(stored_weights, indent=4))