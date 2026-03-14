# SmartSynergy AQI System

A next-generation air quality assessment engine that quantifies the cumulative health risk of breathing multiple pollutants simultaneously.

## description

The Smart Air Quality Index (AQI) algorithm computes a health-aware air quality score by processing raw pollutant and meteorological data through a structured, risk-based pipeline. Incoming sensor readings are first validated, smoothed to reduce noise (Kalman Filter), and completed using principled imputation methods (linear interpolation and historical averaging), after which all pollutant concentrations are normalized to a common scale (min–max normalization). Each pollutant is then mapped to a continuous health risk value using a nonlinear risk function that reflects gradual increases in harm rather than rigid threshold jumps (sigmoid risk mapping). To account for differing health impacts and environmental volatility, the algorithm assigns adaptive weights to pollutants by combining established health importance with recent variability measures (entropy-based weighting). The weighted risks are aggregated into a single AQI score using a composite formulation that captures cumulative multi-pollutant effects (weighted p-norm aggregation), while a safety override ensures that extreme levels of any individual pollutant are never masked by averaging. Finally, the resulting score is translated into standard air quality categories and health guidance, producing an interpretable and responsive air quality indicator suitable for real-time monitoring.

## Features

- **Synergistic Risk Calculation**: Vector mathematics for cumulative pollutant effects
- **Context-Aware**: Integrates urban morphology, weather physics, and temporal entropy
- **Self-Diagnostic**: Built-in confidence metrics and data quality tracking
- **Production-Ready**: Proper error handling, logging, and database management

## Quick Start

### 1. Installation

```bash

cd smartsynergy-aqi

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env and add your configuration
# Required: GOOGLE_MAPS_API_KEY (for morphology analysis)
```

### 3. Database Setup

```bash
# Initialize the database and load data
python data_loader.py

# Pre-calculate weight matrices (optional)
python weights/main_subjective.py
python weights/main_entropy.py
python weights/main_weather.py
python weights/main_morphology.py
```

### 4. Run the System

```bash
python main.py
```

## Project Structure

```
smartsynergy-aqi/
├── config.py                 # Central configuration
├── logger.py                 # Logging utilities
├── validation.py             # Input validation
├── main.py                   # Master pipeline
├── data_loader.py            # CSV ingestion
├── weights/                  # Weight calculation modules
│   ├── main_subjective.py    # Biological toxicity weights
│   ├── main_morphology.py    # Urban morphology (CNN)
│   ├── main_weather.py       # Weather physics
│   └── main_entropy.py       # Temporal volatility
├── .env.example              # Configuration template
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

## Configuration

All configuration is managed through environment variables (see `.env.example`):

- **GOOGLE_MAPS_API_KEY**: Google Maps API key for satellite imagery
- **SMARTAQI_DB_NAME**: Database filename (default: smartsynergy_aqi.db)
- **SMARTAQI_CSV_PATH**: Path to CSV data files (default: ./data_source)
- **LOG_LEVEL**: Logging verbosity (DEBUG, INFO, WARNING, ERROR)

## API Usage

### Basic Example

```python
from main import SmartSynergyAQI

# Initialize system
system = SmartSynergyAQI()

# Prepare input data
pollutant_data = {
    "PM2.5": 55.0,
    "NO2": 45.0,
    "PM10": 30.0,
    "O3": 40.0,
    "CO": 800.0,
    "SO2": 12.0
}

weather_data = {
    "temp": 32,      # Celsius
    "humidity": 85,  # Percentage
    "wspm": 1.5,     # Wind speed (m/s)
    "rain": 0        # Rainfall (mm)
}

# Calculate AQI
result = system.calculate_aqi_pipeline(
    raw_input=pollutant_data,
    weather=weather_data,
    lat=25.0,  # Latitude
    lon=55.0   # Longitude
)

print(f"AQI: {result['AQI']}")
print(f"Severity: {result['Severity']}")
print(f"Confidence: {result['Confidence']}")
```

### Output Format

```json
{
  "AQI": 174,
  "Severity": "🔴 Unhealthy",
  "Confidence": {
    "percentage": 85,
    "label": "High",
    "issues": ["O3 using history"]
  },
  "PAV": {
    "PM2.5": 73.0,
    "NO2": 5.9,
    "O3": 21.0,
    "CO": 0.1,
    "SO2": 0.0,
    "PM10": 0.0
  },
  "Flags": {
    "PM2.5": "Real-Time",
    "NO2": "Real-Time",
    "O3": "Imputed (History)"
  },
  "Debug": {
    "Standard": 149,
    "Synergy": 174,
    "Smart": 172
  }
}
```

## Data Format

### CSV Input (Beijing Dataset Compatible)

Required columns:
- `year`, `month`, `day`, `hour` - Timestamp components
- `station` - Station identifier
- `PM2.5`, `PM10`, `NO2`, `SO2`, `CO`, `O3` - Pollutant concentrations (µg/m³)

Optional columns:
- `TEMP` - Temperature (°C)
- `PRES` - Pressure (hPa)
- `DEWP` - Dew point (°C)
- `RAIN` - Rainfall (mm)
- `WSPM` - Wind speed (m/s)

## Scientific Background

### Three Core Problems Solved

1. **Cocktail Effect**: Uses vector mathematics instead of max() operator
2. **Context Blindness**: Integrates morphology, weather, and temporal patterns
3. **Redundancy Flaw**: Mahalanobis distance for correlated pollutants

### 15-Step Pipeline

1. Data Entry
2. Hierarchical Imputation
3. Kalman Filtering
4. Risk Normalization (EPA breakpoints)
5. Weight Retrieval (4 weight types)
6. Severity Coefficient Calculation
7. Weighted Risk Vector
8. Synergy AQI (Euclidean)
9. Smart AQI (Mahalanobis)
10. Safety Veto Rule
11. Final Calculation
12. Jitter Avoidance
13. Pollutant Attribution Vector (PAV)
14. Severity Interpretation
15. Confidence Level

## Security Notes

- **Never commit the `.env` file** - It contains sensitive API keys
- Store API keys in environment variables only
- Use `.gitignore` to exclude `.env` and `*.log` files
- In production, use secure secrets management (AWS Secrets Manager, Azure Key Vault)

## Troubleshooting

### "No module named 'config'"
- Ensure all files are in the same directory or add to PYTHONPATH

### "Google Maps API Error"
- Check that `GOOGLE_MAPS_API_KEY` is set in `.env`
- Enable "Maps Static API" in Google Cloud Console
- Check API key restrictions and billing

### "Database locked"
- Close any other connections to the database
- Use the provided context managers (`get_db_connection`)

### Missing data warnings
- Check CSV file format matches expected columns
- Review data_loader.py logs for specific issues

## Performance Optimization

For large datasets:
- Use batch processing in weight modules
- Index database tables on `station_id` and `timestamp`
- Consider PostgreSQL for production deployments
- Enable connection pooling for concurrent requests

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request


```
SmartSynergy AQI Model
Venkata Aneesh W
Birla Institute Of Technology And Sciences , Pilani Dubai Campus
```

## Contact

For questions or support:
- Email: Venkataaneeshw@gmail.com

## Acknowledgments

- EPA for AQI breakpoint standards
- WHO for biological toxicity research
- Beijing Municipal Environmental Monitoring Center for dataset
