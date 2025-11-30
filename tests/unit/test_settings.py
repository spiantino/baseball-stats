"""
Unit tests for configuration settings.
"""

import pytest
from unittest.mock import patch, mock_open
import os

from config.settings import (
    DatabaseConfig,
    APIConfig,
    ScraperConfig,
    CacheConfig,
    AppConfig,
    Settings,
    get_settings
)


def test_database_config_from_env():
    """Test DatabaseConfig loads from environment variables."""
    with patch.dict(os.environ, {
        'MONGODB_URL': 'mongodb://localhost:27017/test',
        'DATABASE_NAME': 'test_db'
    }):
        config = DatabaseConfig()
        assert config.mongodb_url == 'mongodb://localhost:27017/test'
        assert config.database_name == 'test_db'


def test_database_config_defaults():
    """Test DatabaseConfig uses defaults when env vars not set."""
    with patch.dict(os.environ, {
        'MONGODB_URL': 'mongodb://localhost:27017/test'
    }, clear=True):
        config = DatabaseConfig()
        assert config.database_name == 'baseball_stats'


def test_api_config_defaults():
    """Test APIConfig default values."""
    config = APIConfig()
    assert config.mlb_api_base_url == 'https://statsapi.mlb.com/api/v1'
    assert config.mlb_api_timeout == 30
    assert 'open-meteo' in config.weather_api_url


def test_scraper_config_defaults():
    """Test ScraperConfig default values."""
    config = ScraperConfig()
    assert 'BaseballStatsBot' in config.user_agent
    assert config.request_timeout == 30
    assert config.retry_attempts == 3
    assert config.retry_backoff == 2.0
    assert config.rate_limit_delay == 1.0


def test_scraper_config_from_env():
    """Test ScraperConfig loads from environment variables."""
    with patch.dict(os.environ, {
        'SCRAPER_TIMEOUT': '60',
        'SCRAPER_RETRY_ATTEMPTS': '5'
    }):
        config = ScraperConfig()
        assert config.request_timeout == 60
        assert config.retry_attempts == 5


def test_cache_config_defaults():
    """Test CacheConfig default values."""
    config = CacheConfig()
    assert config.enable_cache is True
    assert config.cache_ttl_hours == 24
    assert config.cache_backend == 'mongodb'


def test_app_config_defaults():
    """Test AppConfig default values."""
    config = AppConfig()
    assert config.environment == 'development'
    assert config.debug is False
    assert config.log_level == 'INFO'


def test_settings_initialization():
    """Test Settings class initializes all config objects."""
    with patch.dict(os.environ, {
        'MONGODB_URL': 'mongodb://localhost:27017/test'
    }):
        settings = Settings()

        assert isinstance(settings.database, DatabaseConfig)
        assert isinstance(settings.api, APIConfig)
        assert isinstance(settings.scraper, ScraperConfig)
        assert isinstance(settings.cache, CacheConfig)
        assert isinstance(settings.app, AppConfig)


def test_get_settings_singleton():
    """Test that get_settings returns the same instance."""
    with patch.dict(os.environ, {
        'MONGODB_URL': 'mongodb://localhost:27017/test'
    }):
        # Clear singleton
        import config.settings
        config.settings._settings = None

        settings1 = get_settings()
        settings2 = get_settings()

        assert settings1 is settings2


def test_settings_access_pattern():
    """Test the intended usage pattern for settings."""
    with patch.dict(os.environ, {
        'MONGODB_URL': 'mongodb://test:27017/db',
        'SCRAPER_TIMEOUT': '45'
    }):
        # Clear singleton
        import config.settings
        config.settings._settings = None

        settings = get_settings()

        # Should be able to access nested config
        assert settings.database.mongodb_url == 'mongodb://test:27017/db'
        assert settings.scraper.request_timeout == 45
        assert settings.api.mlb_api_timeout == 30
