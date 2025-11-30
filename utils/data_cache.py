"""
Data cache manager for game preview data.

Provides file-based JSON caching to separate data fetching from report generation.
This enables fast iteration on report design without expensive API calls.
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from config.logging_config import get_logger

logger = get_logger(__name__)


class DataCache:
    """Manages cached game preview data as JSON files."""

    def __init__(self, cache_dir: str = "data/cache"):
        """
        Initialize cache manager.

        Args:
            cache_dir: Directory to store cache files
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Cache directory: {self.cache_dir}")

    def get_cache_key(self, away_team: str, home_team: str, game_date: str) -> str:
        """
        Generate cache key for a game.

        Args:
            away_team: Away team abbreviation (e.g., 'NYY')
            home_team: Home team abbreviation (e.g., 'BOS')
            game_date: Game date in YYYY-MM-DD format

        Returns:
            Cache key string
        """
        return f"{away_team}_{home_team}_{game_date}"

    def get_cache_path(self, away_team: str, home_team: str, game_date: str) -> Path:
        """
        Get path to cache file for a game.

        Args:
            away_team: Away team abbreviation
            home_team: Home team abbreviation
            game_date: Game date in YYYY-MM-DD format

        Returns:
            Path to cache file
        """
        cache_key = self.get_cache_key(away_team, home_team, game_date)
        return self.cache_dir / f"{cache_key}.json"

    def has_cache(self, away_team: str, home_team: str, game_date: str) -> bool:
        """
        Check if cached data exists for a game.

        Args:
            away_team: Away team abbreviation
            home_team: Home team abbreviation
            game_date: Game date in YYYY-MM-DD format

        Returns:
            True if cache exists
        """
        cache_path = self.get_cache_path(away_team, home_team, game_date)
        return cache_path.exists()

    def get(self, away_team: str, home_team: str, game_date: str) -> Optional[Dict[str, Any]]:
        """
        Load cached data for a game.

        Args:
            away_team: Away team abbreviation
            home_team: Home team abbreviation
            game_date: Game date in YYYY-MM-DD format

        Returns:
            Cached data dict or None if not found
        """
        cache_path = self.get_cache_path(away_team, home_team, game_date)

        if not cache_path.exists():
            logger.info(f"Cache miss: {cache_path.name}")
            return None

        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)

            logger.info(f"Cache hit: {cache_path.name} (fetched at {data.get('fetched_at', 'unknown')})")
            return data

        except Exception as e:
            logger.error(f"Error reading cache {cache_path}: {e}")
            return None

    def set(self, away_team: str, home_team: str, game_date: str, data: Dict[str, Any]) -> Path:
        """
        Save data to cache.

        Args:
            away_team: Away team abbreviation
            home_team: Home team abbreviation
            game_date: Game date in YYYY-MM-DD format
            data: Data dict to cache

        Returns:
            Path to cache file
        """
        cache_path = self.get_cache_path(away_team, home_team, game_date)

        # Add metadata
        data['cache_metadata'] = {
            'away_team': away_team,
            'home_team': home_team,
            'game_date': game_date,
            'fetched_at': datetime.now().isoformat()
        }

        try:
            with open(cache_path, 'w') as f:
                json.dump(data, f, indent=2)

            logger.info(f"Cached data: {cache_path.name} ({cache_path.stat().st_size / 1024:.1f} KB)")
            return cache_path

        except Exception as e:
            logger.error(f"Error writing cache {cache_path}: {e}")
            raise

    def invalidate(self, away_team: str, home_team: str, game_date: str) -> bool:
        """
        Delete cached data for a game.

        Args:
            away_team: Away team abbreviation
            home_team: Home team abbreviation
            game_date: Game date in YYYY-MM-DD format

        Returns:
            True if cache was deleted
        """
        cache_path = self.get_cache_path(away_team, home_team, game_date)

        if cache_path.exists():
            cache_path.unlink()
            logger.info(f"Invalidated cache: {cache_path.name}")
            return True

        return False

    def list_cached_games(self) -> list[Dict[str, str]]:
        """
        List all cached games.

        Returns:
            List of dicts with away_team, home_team, game_date, fetched_at
        """
        cached_games = []

        for cache_file in self.cache_dir.glob("*.json"):
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)

                metadata = data.get('cache_metadata', {})
                cached_games.append({
                    'away_team': metadata.get('away_team', ''),
                    'home_team': metadata.get('home_team', ''),
                    'game_date': metadata.get('game_date', ''),
                    'fetched_at': metadata.get('fetched_at', ''),
                    'file_size_kb': cache_file.stat().st_size / 1024
                })

            except Exception as e:
                logger.warning(f"Error reading {cache_file.name}: {e}")

        return sorted(cached_games, key=lambda x: x['game_date'], reverse=True)
