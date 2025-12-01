"""
Weather client for fetching game-day weather from NOAA/weather.gov API.
"""

import requests
from typing import Optional, Dict
from datetime import datetime
from config.logging_config import get_logger

logger = get_logger(__name__)

# MLB ballpark coordinates (lat, lon)
BALLPARK_COORDS = {
    'Yankee Stadium': (40.8296, -73.9262),
    'Fenway Park': (42.3467, -71.0972),
    'Camden Yards': (39.2839, -76.6217),
    'Tropicana Field': (27.7682, -82.6534),
    'Rogers Centre': (43.6414, -79.3894),
    'Progressive Field': (41.4962, -81.6852),
    'Target Field': (44.9817, -93.2776),
    'Guaranteed Rate Field': (41.8299, -87.6338),
    'Comerica Park': (42.3391, -83.0485),
    'Kauffman Stadium': (39.0517, -94.4803),
    'Minute Maid Park': (29.7573, -95.3555),
    'Globe Life Field': (32.7471, -97.0825),
    'T-Mobile Park': (47.5914, -122.3325),
    'Angel Stadium': (33.8003, -117.8827),
    'Oakland Coliseum': (37.7516, -122.2005),
    'Truist Park': (33.8906, -84.4677),
    'Citizens Bank Park': (39.9061, -75.1665),
    'Citi Field': (40.7571, -73.8458),
    'loanDepot park': (25.7781, -80.2197),
    'Nationals Park': (38.8730, -77.0074),
    'American Family Field': (43.0280, -87.9712),
    'Busch Stadium': (38.6226, -90.1928),
    'Wrigley Field': (41.9484, -87.6553),
    'Great American Ball Park': (39.0974, -84.5068),
    'PNC Park': (40.4469, -80.0057),
    'Dodger Stadium': (34.0739, -118.2400),
    'Petco Park': (32.7076, -117.1566),
    'Oracle Park': (37.7786, -122.3893),
    'Coors Field': (39.7559, -104.9942),
    'Chase Field': (33.4453, -112.0667),
}


class WeatherClient:
    """Client for fetching weather data from NOAA weather.gov API."""

    BASE_URL = "https://api.weather.gov"

    def __init__(self):
        """Initialize weather client with user agent (required by NOAA)."""
        self.session = requests.Session()
        # NOAA requires a user agent
        self.session.headers.update({
            'User-Agent': '(Baseball Stats Preview Generator, contact@example.com)'
        })

    def get_forecast_temperature(
        self,
        venue: str,
        game_datetime: Optional[datetime] = None
    ) -> Optional[int]:
        """
        Get temperature forecast for a venue.

        Args:
            venue: Ballpark name (e.g., 'Yankee Stadium')
            game_datetime: Game date/time (optional, defaults to current time)

        Returns:
            Temperature in Fahrenheit (int) or None if unavailable

        Example:
            >>> client = WeatherClient()
            >>> temp = client.get_forecast_temperature('Yankee Stadium')
            >>> print(f"{temp}°F")
        """
        # Get venue coordinates
        coords = BALLPARK_COORDS.get(venue)
        if not coords:
            logger.warning(f"No coordinates found for venue: {venue}")
            return None

        lat, lon = coords

        try:
            # Step 1: Get grid point data (converts lat/lon to forecast grid)
            points_url = f"{self.BASE_URL}/points/{lat},{lon}"
            logger.info(f"Fetching grid point for {venue} ({lat}, {lon})")

            points_response = self.session.get(points_url, timeout=10)
            points_response.raise_for_status()
            points_data = points_response.json()

            # Step 2: Get forecast URL from grid point
            forecast_url = points_data.get('properties', {}).get('forecast')
            if not forecast_url:
                logger.error(f"No forecast URL in grid point response for {venue}")
                return None

            # Step 3: Get hourly forecast
            # Use hourly forecast for more precise timing
            forecast_hourly_url = points_data.get('properties', {}).get('forecastHourly')
            if forecast_hourly_url:
                forecast_url = forecast_hourly_url

            logger.info(f"Fetching forecast from: {forecast_url}")
            forecast_response = self.session.get(forecast_url, timeout=10)
            forecast_response.raise_for_status()
            forecast_data = forecast_response.json()

            # Step 4: Extract temperature from first period (current/upcoming)
            periods = forecast_data.get('properties', {}).get('periods', [])
            if not periods:
                logger.warning(f"No forecast periods found for {venue}")
                return None

            # Get first period temperature
            first_period = periods[0]
            temperature = first_period.get('temperature')

            if temperature is not None:
                logger.info(f"Weather for {venue}: {temperature}°F")
                return int(temperature)
            else:
                logger.warning(f"No temperature in forecast for {venue}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch weather for {venue}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Error processing weather for {venue}: {e}", exc_info=True)
            return None
