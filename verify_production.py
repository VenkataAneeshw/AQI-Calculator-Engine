#!/usr/bin/env python3
"""
SmartSynergy AQI System - Production Verification Script

This script verifies that all production systems are ready for deployment.
"""

import sqlite3
import sys
import os
from pathlib import Path

import config
from logger import setup_logger

logger = setup_logger(__name__, config.LOG_FILE, config.LOG_LEVEL)


def check_database():
    """Check if database exists and has data."""
    print("\nDATABASE CHECK")
    print("-" * 50)
    
    if not os.path.exists(config.DB_NAME):
        print(f"Database not found: {config.DB_NAME}")
        return False
    
    try:
        conn = sqlite3.connect(config.DB_NAME)
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in database")
            return False
        
        print(f"Database found: {config.DB_NAME}")
        print(f"   Tables: {', '.join([t[0] for t in tables])}")
        
        # Check record count
        cursor.execute("SELECT COUNT(*) FROM measurements")
        count = cursor.fetchone()[0]
        print(f"Records in database: {count:,}")
        
        # Check stations
        cursor.execute("SELECT COUNT(DISTINCT station_id) FROM measurements")
        station_count = cursor.fetchone()[0]
        print(f"Unique stations: {station_count}")
        
        # Show latest record
        cursor.execute("""
            SELECT station_id, timestamp, pm25, no2
            FROM measurements
            ORDER BY rowid DESC
            LIMIT 1
        """)
        latest = cursor.fetchone()
        if latest:
            print(f"✅ Latest record: {latest[0]} @ {latest[1]}")
            print(f"   PM2.5={latest[2]}, NO2={latest[3]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False


def check_configuration():
    """Check if configuration is properly loaded."""
    print("\n⚙️  CONFIGURATION CHECK")
    print("-" * 50)
    
    try:
        print(f"✅ Database name: {config.DB_NAME}")
        print(f"✅ CSV path: {config.CSV_FOLDER_PATH}")
        print(f"✅ Log level: {config.LOG_LEVEL}")
        print(f"✅ Pollutants configured: {len(config.POLLUTANTS)}")
        print(f"✅ Stations configured: {len(config.STATION_LOCATIONS)}")
        print(f"✅ EPA breakpoints configured: {len(config.EPA_BREAKPOINTS)}")
        return True
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False


def check_modules():
    """Check if all required modules are importable."""
    print("\n📦 MODULE CHECK")
    print("-" * 50)
    
    modules = {
        "numpy": "Numerical computing",
        "pandas": "Data manipulation",
        "scipy": "Scientific computing",
        "torch": "Neural networks",
        "config": "System configuration",
        "logger": "Logging utility",
        "validation": "Input validation"
    }
    
    all_ok = True
    for module_name, description in modules.items():
        try:
            __import__(module_name)
            print(f"✅ {module_name}: {description}")
        except ImportError as e:
            print(f"❌ {module_name}: {description} - {e}")
            all_ok = False
    
    # Check weight modules
    print("\n   Weight Modules:")
    weight_modules = [
        "weights.main_subjective",
        "weights.main_morphology",
        "weights.main_weather",
        "weights.main_entropy"
    ]
    
    for module_name in weight_modules:
        try:
            __import__(module_name)
            print(f"   ✅ {module_name}")
        except ImportError:
            print(f"   ⚠️  {module_name} (using fallbacks)")
    
    return all_ok


def check_data_quality():
    """Check data quality and completeness."""
    print("\n🔍 DATA QUALITY CHECK")
    print("-" * 50)
    
    try:
        conn = sqlite3.connect(config.DB_NAME)
        cursor = conn.cursor()
        
        # Check for NULL values
        pollutants = ["pm25", "pm10", "no2", "so2", "co", "o3"]
        for pol in pollutants:
            cursor.execute(f"SELECT COUNT(*) FROM measurements WHERE {pol} IS NULL")
            null_count = cursor.fetchone()[0]
            total = cursor.execute("SELECT COUNT(*) FROM measurements").fetchone()[0]
            pct = (null_count / total * 100) if total > 0 else 0
            
            status = "✅" if pct < 10 else "⚠️ " if pct < 30 else "❌"
            print(f"{status} {pol.upper()}: {null_count:,} NULL values ({pct:.1f}%)")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Data quality check error: {e}")
        return False


def main():
    """Run all checks."""
    print("=" * 50)
    print("🔧 SMARTSYNERGY AQI - PRODUCTION VERIFICATION")
    print("=" * 50)
    
    checks = [
        ("Database", check_database),
        ("Configuration", check_configuration),
        ("Modules", check_modules),
        ("Data Quality", check_data_quality)
    ]
    
    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            logger.error(f"Check '{name}' failed: {e}", exc_info=True)
            results.append((name, False))
    
    print("\n" + "=" * 50)
    print("📋 SUMMARY")
    print("=" * 50)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {name}")
    
    all_passed = all(result for _, result in results)
    
    print("=" * 50)
    if all_passed:
        print("✅ ALL CHECKS PASSED - SYSTEM READY FOR PRODUCTION")
        print("\nRun the system with: python main.py")
        return 0
    else:
        print("❌ SOME CHECKS FAILED - PLEASE FIX ISSUES ABOVE")
        print("\nTroubleshooting:")
        print("1. Load data: python data_loader.py")
        print("2. Check database: sqlite3 smartsynergy_aqi.db '.tables'")
        print("3. Check logs: tail -f smartaqi.log")
        return 1


if __name__ == "__main__":
    sys.exit(main())
