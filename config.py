import os
from pathlib import Path

# ==========================================
# DATABASE CONFIGURATION
# ==========================================
DB_NAME = os.getenv('SMARTAQI_DB_NAME', 'smartsynergy_aqi.db')

# ==========================================
# API KEYS (Load from environment)
# ==========================================
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')

# ==========================================
# DATA PATHS
# ==========================================
CSV_FOLDER_PATH = os.getenv('SMARTAQI_CSV_PATH', './data_source')

# ==========================================
# ENTROPY CONFIGURATION
# ==========================================
WINDOW_SIZE = int(os.getenv('ENTROPY_WINDOW_SIZE', '4'))
ENTROPY_SENSITIVITY = float(os.getenv('ENTROPY_SENSITIVITY', '1.5'))
REGULARIZATION_LAMBDA = float(os.getenv('REGULARIZATION_LAMBDA', '0.2'))
MIN_WEIGHT_CLAMP = float(os.getenv('MIN_WEIGHT_CLAMP', '0.1'))
MAX_WEIGHT_CLAMP = float(os.getenv('MAX_WEIGHT_CLAMP', '3.0'))

# ==========================================
# WEATHER CONFIGURATION
# ==========================================
WEATHER_REGULARIZATION_STRENGTH = float(os.getenv('WEATHER_REG_STRENGTH', '0.4'))
WIND_STAGNATION_THRESHOLD = float(os.getenv('WIND_STAGNATION', '1.0'))
WIND_VENTILATION_THRESHOLD = float(os.getenv('WIND_VENTILATION', '3.0'))
TEMP_OZONE_TRIGGER = float(os.getenv('TEMP_OZONE_TRIGGER', '25.0'))

# ==========================================
# POLLUTANTS LIST
# ==========================================
POLLUTANTS = ["PM2.5", "PM10", "NO2", "SO2", "CO", "O3"]

# ==========================================
# BIOLOGICAL STANDARDS
# ==========================================
BIOLOGICAL_STANDARDS = {
    "PM2.5": 1.5,
    "O3": 1.4,
    "NO2": 1.3,
    "SO2": 1.1,
    "PM10": 0.8,
    "CO": 0.5
}

# ==========================================
# MORPHOLOGY WEIGHTS
# ==========================================
MORPHOLOGY_WEIGHTS = {
    "Industrial": {"PM2.5": 1.2, "PM10": 1.3, "SO2": 1.5, "NO2": 1.1, "O3": 1.0, "CO": 1.0},
    "Traffic": {"PM2.5": 1.2, "PM10": 1.2, "SO2": 0.8, "NO2": 1.6, "O3": 1.0, "CO": 1.2},
    "Residential": {"PM2.5": 1.0, "PM10": 1.0, "SO2": 1.0, "NO2": 1.0, "O3": 1.0, "CO": 1.0},
    "Greenbelt": {"PM2.5": 0.8, "PM10": 0.7, "SO2": 0.6, "NO2": 0.6, "O3": 0.7, "CO": 0.7},
    "Commercial": {"PM2.5": 1.0, "PM10": 1.0, "SO2": 0.9, "NO2": 1.2, "O3": 1.0, "CO": 1.0}
}

CLASS_NAMES = ["Industrial", "Traffic", "Residential", "Greenbelt", "Commercial"]

# ==========================================
# STATION LOCATIONS (Beijing Dataset)
# ==========================================
STATION_LOCATIONS = {
    "Aotizhongxin": {"lat": 39.982, "lon": 116.397},
    "Changping": {"lat": 40.195, "lon": 116.230},
    "Dingling": {"lat": 40.283, "lon": 115.995},
    "Dongsi": {"lat": 39.929, "lon": 116.416},
    "Guanyuan": {"lat": 39.929, "lon": 116.305},
    "Gucheng": {"lat": 39.884, "lon": 116.176},
    "Huairou": {"lat": 40.360, "lon": 116.628},
    "Nongzhanguan": {"lat": 39.937, "lon": 116.461},
    "Shunyi": {"lat": 40.129, "lon": 116.654},
    "Tiantan": {"lat": 39.886, "lon": 116.407},
    "Wanliu": {"lat": 39.987, "lon": 116.287},
    "Wanshouxigong": {"lat": 39.943, "lon": 116.285}
}

# ==========================================
# EPA BREAKPOINTS
# ==========================================
EPA_BREAKPOINTS = {
    "PM2.5": [
        (0, 12, 0, 50),
        (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 500.4, 301, 500)
    ],
    "PM10": [
        (0, 54, 0, 50),
        (55, 154, 51, 100),
        (155, 254, 101, 150),
        (255, 354, 151, 200),
        (355, 424, 201, 300),
        (425, 604, 301, 500)
    ],
    "NO2": [
        (0, 53, 0, 50),
        (54, 100, 51, 100),
        (101, 360, 101, 150),
        (361, 649, 151, 200),
        (650, 1249, 201, 300),
        (1250, 2049, 301, 500)
    ],
    "O3": [
        (0, 54, 0, 50),
        (55, 70, 51, 100),
        (71, 85, 101, 150),
        (86, 105, 151, 200),
        (106, 200, 201, 300),
        (201, 400, 301, 500)
    ],
    "CO": [
        (0, 4400, 0, 50),
        (4401, 9400, 51, 100),
        (9401, 12400, 101, 150),
        (12401, 15400, 151, 200),
        (15401, 30400, 201, 300),
        (30401, 50400, 301, 500)
    ],
    "SO2": [
        (0, 35, 0, 50),
        (36, 75, 51, 100),
        (76, 185, 101, 150),
        (186, 304, 151, 200),
        (305, 604, 201, 300),
        (605, 1004, 301, 500)
    ]
}

# ==========================================
# LOGGING CONFIGURATION
# ==========================================
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', 'smartaqi.log')

# ==========================================
# VALIDATION RANGES
# ==========================================
VALID_LAT_RANGE = (-90, 90)
VALID_LON_RANGE = (-180, 180)
VALID_TEMP_RANGE = (-50, 60)  # Celsius
VALID_HUMIDITY_RANGE = (0, 100)  # Percentage
VALID_WIND_RANGE = (0, 50)  # m/s