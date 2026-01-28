# SmartSynergy AQI System - Initial Setup Instructions

## 1. Prerequisites
- Python 3.8 or higher
- pip (Python package manager)

## 2. Install Dependencies
Run the following command to install all required Python packages:

```
pip install -r requirements_txt.txt
```

## 3. Configure Environment Variables
- Copy the provided `.env` file or create a new one in the project root.
- Set the database path and any required API keys. Example:
  ```
  DB_PATH=smartsynergy_aqi.db
  ```

## 4. Load Data into the Database
If the database file (`smartsynergy_aqi.db`) does not exist, populate it from the CSV data:

```
python data_loader.py
```
- This will read all CSV files in the `data_source/` folder and create the database.

## 5. Verify the Setup
Run the verification script to ensure everything is ready:

```
python verify_production.py
```
- You should see: `✅ ALL CHECKS PASSED - SYSTEM READY FOR PRODUCTION`

## 6. Run the AQI System
Start the main AQI calculation pipeline:

```
python main.py
```
- This will fetch the latest data, calculate AQI, and output the results.

## 7. Monitor Logs (Optional)
To view real-time logs for troubleshooting:

```
tail -f smartaqi.log
```

---

**You are now ready to use the AQI system in production!**
