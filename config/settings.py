"""
Configuration management for baseball stats application.

Uses pydantic-settings for type-safe configuration with .env file support.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class DatabaseConfig(BaseSettings):
    """Database configuration settings."""

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    mongodb_url: str
    database_name: str = 'baseball_stats'


class APIConfig(BaseSettings):
    """External API configuration settings."""

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    # MLB Stats API (free, official)
    mlb_api_base_url: str = "https://statsapi.mlb.com/api/v1"
    mlb_api_timeout: int = 30

    # Weather API (Open-Meteo - free, no key required)
    weather_api_url: str = "https://api.open-meteo.com/v1/forecast"
    weather_api_timeout: int = 10


class ScraperConfig(BaseSettings):
    """Web scraping configuration settings."""

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    user_agent: str = "Mozilla/5.0 (compatible; BaseballStatsBot/1.0)"
    request_timeout: int = 30
    retry_attempts: int = 3
    retry_backoff: float = 2.0
    rate_limit_delay: float = 1.0


class CacheConfig(BaseSettings):
    """Caching configuration settings."""

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    enable_cache: bool = True
    cache_ttl_hours: int = 24
    cache_backend: str = 'mongodb'


class AppConfig(BaseSettings):
    """Application-wide configuration."""

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    environment: str = 'development'
    debug: bool = False
    log_level: str = 'INFO'


class Settings:
    """
    Centralized settings management.

    Usage:
        from config.settings import get_settings

        settings = get_settings()
        db_url = settings.database.mongodb_url
    """

    def __init__(self):
        self.database = DatabaseConfig()
        self.api = APIConfig()
        self.scraper = ScraperConfig()
        self.cache = CacheConfig()
        self.app = AppConfig()


# Singleton instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Get the singleton settings instance.

    Returns:
        Settings: The application settings
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
