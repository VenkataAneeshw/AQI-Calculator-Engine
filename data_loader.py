import pandas as pd
import sqlite3
import os
import glob
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager

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
# DATA LOADER CLASS
# ==========================================
class DataLoader:
    def __init__(self, csv_folder_path: str = None, db_name: str = None):
        self.csv_folder = Path(csv_folder_path or config.CSV_FOLDER_PATH)
        self.db_name = db_name or config.DB_NAME
        logger.info(f"DataLoader initialized with CSV path: {self.csv_folder}")
    
    def ensure_csv_folder_exists(self):
        """Validates that the CSV folder exists."""
        if not self.csv_folder.exists():
            logger.error(f"CSV folder does not exist: {self.csv_folder}")
            logger.info(f"Creating folder: {self.csv_folder}")
            self.csv_folder.mkdir(parents=True, exist_ok=True)
            return False
        return True
    
    def _create_tables(self, cursor):
        """Creates the database schema if it doesn't exist."""
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stations (
                station_id TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS measurements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                pm25 REAL,
                pm10 REAL,
                no2 REAL,
                so2 REAL,
                co REAL,
                o3 REAL,
                FOREIGN KEY (station_id) REFERENCES stations(station_id)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_station_timestamp 
            ON measurements(station_id, timestamp)
        ''')
        
        logger.info("Database tables created/verified")

    def process_and_load_csvs(self):
        """
        Main method to process and load CSV files into the database.
        """
        logger.info("=== Starting Data Ingestion ===")
        
        # Validate folder
        if not self.ensure_csv_folder_exists():
            logger.warning(f"No CSV files to process. Place CSV files in: {self.csv_folder}")
            return
        
        # Get all CSV files
        csv_files = list(self.csv_folder.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {self.csv_folder}")
            return

        logger.info(f"Found {len(csv_files)} CSV files. Starting ingestion...")
        
        total_inserted = 0
        
        with get_db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            self._create_tables(cursor)
            
            for file_path in csv_files:
                logger.info(f"Processing: {file_path.name}")
                
                try:
                    # Read CSV
                    df = pd.read_csv(file_path)
                    logger.debug(f"Loaded {len(df)} rows from {file_path.name}")
                    
                    # Validate required columns
                    required_cols = ['PM2.5', 'PM10', 'SO2', 'NO2', 'CO', 'O3', 'station']
                    missing_cols = [col for col in required_cols if col not in df.columns]
                    
                    if missing_cols:
                        logger.error(f"Missing columns in {file_path.name}: {missing_cols}")
                        continue
                    
                    # Clean Data
                    original_count = len(df)
                    df = df.dropna(subset=required_cols)
                    dropped_count = original_count - len(df)
                    
                    if dropped_count > 0:
                        logger.info(f"Dropped {dropped_count} rows with missing values")
                    
                    if df.empty:
                        logger.warning(f"No valid data remaining in {file_path.name}")
                        continue
                    
                    # Prepare records
                    records_to_insert = []
                    
                    for _, row in df.iterrows():
                        try:
                            # Construct timestamp
                            timestamp_str = f"{int(row['year'])}-{int(row['month']):02d}-{int(row['day']):02d} {int(row['hour']):02d}:00:00"
                            
                            records_to_insert.append((
                                row['station'],
                                timestamp_str,
                                float(row['PM2.5']),
                                float(row['PM10']),
                                float(row['NO2']),
                                float(row['SO2']),
                                float(row['CO']),
                                float(row['O3'])
                            ))
                        except (KeyError, ValueError) as e:
                            logger.debug(f"Skipping row due to error: {e}")
                            continue
                    
                    # Bulk Insert
                    if records_to_insert:
                        cursor.executemany('''
                            INSERT INTO measurements (station_id, timestamp, pm25, pm10, no2, so2, co, o3)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', records_to_insert)
                        
                        inserted = len(records_to_insert)
                        total_inserted += inserted
                        logger.info(f"Inserted {inserted} records from {file_path.name}")
                    
                    # Register stations
                    unique_stations = df['station'].unique()
                    for station in unique_stations:
                        cursor.execute(
                            "INSERT OR IGNORE INTO stations (station_id) VALUES (?)", 
                            (station,)
                        )
                    
                except Exception as e:
                    logger.error(f"Error processing {file_path.name}: {e}", exc_info=True)
                    continue
            
            # Commit all changes
            conn.commit()
        
        logger.info(f"=== Data Ingestion Complete: {total_inserted} total records inserted ===")
    
    def verify_data(self):
        """Verification step to check loaded data."""
        logger.info("=== Verifying Loaded Data ===")
        
        try:
            with get_db_connection(self.db_name) as conn:
                cursor = conn.cursor()
                
                # Count measurements
                cursor.execute("SELECT COUNT(*) FROM measurements")
                count = cursor.fetchone()[0]
                logger.info(f"Total measurements in database: {count}")
                
                # Count stations
                cursor.execute("SELECT COUNT(*) FROM stations")
                station_count = cursor.fetchone()[0]
                logger.info(f"Total stations registered: {station_count}")
                
                # Show sample
                cursor.execute("SELECT station_id, timestamp, pm25, no2 FROM measurements LIMIT 3")
                samples = cursor.fetchall()
                
                if samples:
                    logger.info("Sample data:")
                    for sample in samples:
                        logger.info(f"  Station: {sample[0]}, Time: {sample[1]}, PM2.5: {sample[2]}, NO2: {sample[3]}")
                
        except Exception as e:
            logger.error(f"Verification error: {e}")

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    loader = DataLoader()
    loader.process_and_load_csvs()
    loader.verify_data()