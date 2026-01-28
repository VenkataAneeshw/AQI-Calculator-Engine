from typing import Dict, Any, Tuple
import config

class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass

def validate_coordinates(lat: float, lon: float) -> Tuple[float, float]:
    """
    Validates latitude and longitude values.
    
    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees
    
    Returns:
        Tuple of validated (lat, lon)
    
    Raises:
        ValidationError: If coordinates are out of valid range
    """
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        raise ValidationError(f"Coordinates must be numeric: lat={lat}, lon={lon}")
    
    min_lat, max_lat = config.VALID_LAT_RANGE
    min_lon, max_lon = config.VALID_LON_RANGE
    
    if not (min_lat <= lat <= max_lat):
        raise ValidationError(f"Latitude {lat} out of range [{min_lat}, {max_lat}]")
    
    if not (min_lon <= lon <= max_lon):
        raise ValidationError(f"Longitude {lon} out of range [{min_lon}, {max_lon}]")
    
    return lat, lon

def validate_weather_data(weather: Dict[str, Any]) -> Dict[str, float]:
    """
    Validates and sanitizes weather data.
    
    Args:
        weather: Dictionary containing weather parameters
    
    Returns:
        Validated weather dictionary
    
    Raises:
        ValidationError: If weather data is invalid
    """
    if not isinstance(weather, dict):
        raise ValidationError(f"Weather data must be a dictionary, got {type(weather)}")
    
    validated = {}
    
    # Temperature validation
    temp = weather.get('temp', weather.get('TEMP', 20))
    try:
        temp = float(temp)
        min_temp, max_temp = config.VALID_TEMP_RANGE
        if not (min_temp <= temp <= max_temp):
            raise ValidationError(f"Temperature {temp}°C out of range [{min_temp}, {max_temp}]")
        validated['TEMP'] = temp
    except (TypeError, ValueError):
        raise ValidationError(f"Invalid temperature value: {temp}")
    
    # Humidity validation
    humidity = weather.get('humidity', weather.get('HUMIDITY', 50))
    try:
        humidity = float(humidity)
        min_hum, max_hum = config.VALID_HUMIDITY_RANGE
        if not (min_hum <= humidity <= max_hum):
            raise ValidationError(f"Humidity {humidity}% out of range [{min_hum}, {max_hum}]")
        validated['HUMIDITY'] = humidity
    except (TypeError, ValueError):
        raise ValidationError(f"Invalid humidity value: {humidity}")
    
    # Wind speed validation
    wspm = weather.get('wspm', weather.get('WSPM', 1.0))
    try:
        wspm = float(wspm)
        min_wind, max_wind = config.VALID_WIND_RANGE
        if not (min_wind <= wspm <= max_wind):
            raise ValidationError(f"Wind speed {wspm} m/s out of range [{min_wind}, {max_wind}]")
        validated['WSPM'] = wspm
    except (TypeError, ValueError):
        raise ValidationError(f"Invalid wind speed value: {wspm}")
    
    # Rain validation (optional)
    rain = weather.get('rain', weather.get('RAIN', 0))
    try:
        rain = float(rain)
        if rain < 0:
            raise ValidationError(f"Rain cannot be negative: {rain}")
        validated['RAIN'] = rain
    except (TypeError, ValueError):
        raise ValidationError(f"Invalid rain value: {rain}")
    
    return validated

def validate_pollutant_data(data: Dict[str, Any]) -> Dict[str, float]:
    """
    Validates pollutant concentration data.
    
    Args:
        data: Dictionary of pollutant concentrations
    
    Returns:
        Validated pollutant dictionary (None values preserved)
    """
    if not isinstance(data, dict):
        raise ValidationError(f"Pollutant data must be a dictionary, got {type(data)}")
    
    validated = {}
    
    for pollutant in config.POLLUTANTS:
        value = data.get(pollutant)
        
        if value is None:
            validated[pollutant] = None
            continue
        
        try:
            value = float(value)
            if value < 0:
                raise ValidationError(f"{pollutant} concentration cannot be negative: {value}")
            if value > 10000:  # Sanity check for extremely high values
                raise ValidationError(f"{pollutant} concentration suspiciously high: {value}")
            validated[pollutant] = value
        except (TypeError, ValueError):
            raise ValidationError(f"Invalid {pollutant} value: {value}")
    
    return validated