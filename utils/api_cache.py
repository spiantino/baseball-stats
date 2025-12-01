"""
Raw API response cache with TTL support.

Caches individual API responses to allow format/transform changes
without re-fetching data from external APIs.
"""

import json
import hashlib
from pathlib import Path
from typing import Any, Optional, Callable
from datetime import datetime, timedelta
from functools import wraps
from config.logging_config import get_logger

logger = get_logger(__name__)

# Default TTL: 6 hours
DEFAULT_TTL_HOURS = 6


class APICache:
    """
    Caches raw API responses with TTL expiration.

    Cache keys are based on: source + endpoint + params_hash
    Example: mlb_schedule_a1b2c3d4.json
    """

    def __init__(self, cache_dir: str = "data/api_cache", ttl_hours: int = DEFAULT_TTL_HOURS):
        """
        Initialize API cache.

        Args:
            cache_dir: Directory to store cache files
            ttl_hours: Time-to-live in hours (default: 6)
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl = timedelta(hours=ttl_hours)
        logger.info(f"APICache initialized: {self.cache_dir} (TTL: {ttl_hours}h)")

    def _hash_params(self, params: dict) -> str:
        """Create a short hash of parameters for cache key."""
        # Sort keys for consistent hashing
        param_str = json.dumps(params, sort_keys=True, default=str)
        return hashlib.md5(param_str.encode()).hexdigest()[:8]

    def _get_cache_path(self, source: str, endpoint: str, params: dict) -> Path:
        """
        Generate cache file path.

        Args:
            source: API source (e.g., 'mlb', 'statcast', 'bref')
            endpoint: Endpoint name (e.g., 'schedule', 'pitch_mix')
            params: Request parameters

        Returns:
            Path to cache file
        """
        params_hash = self._hash_params(params)
        filename = f"{source}_{endpoint}_{params_hash}.json"
        return self.cache_dir / filename

    def _is_expired(self, cache_path: Path) -> bool:
        """Check if cache file has expired."""
        if not cache_path.exists():
            return True

        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)

            cached_at = datetime.fromisoformat(data.get('_cached_at', '2000-01-01'))
            return datetime.now() - cached_at > self.ttl

        except Exception:
            return True

    def get(self, source: str, endpoint: str, params: dict) -> Optional[Any]:
        """
        Get cached response if available and not expired.

        Args:
            source: API source
            endpoint: Endpoint name
            params: Request parameters

        Returns:
            Cached data or None if not found/expired
        """
        cache_path = self._get_cache_path(source, endpoint, params)

        if not cache_path.exists():
            logger.debug(f"Cache miss: {cache_path.name}")
            return None

        if self._is_expired(cache_path):
            logger.debug(f"Cache expired: {cache_path.name}")
            return None

        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)

            # Return the actual response data, not metadata
            logger.debug(f"Cache hit: {cache_path.name}")
            return data.get('_response')

        except Exception as e:
            logger.warning(f"Error reading cache {cache_path.name}: {e}")
            return None

    def set(self, source: str, endpoint: str, params: dict, response: Any) -> Path:
        """
        Cache an API response.

        Args:
            source: API source
            endpoint: Endpoint name
            params: Request parameters
            response: Response data to cache

        Returns:
            Path to cache file
        """
        cache_path = self._get_cache_path(source, endpoint, params)

        cache_data = {
            '_cached_at': datetime.now().isoformat(),
            '_source': source,
            '_endpoint': endpoint,
            '_params': params,
            '_response': response
        }

        try:
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f, indent=2, default=str)

            logger.debug(f"Cached: {cache_path.name} ({cache_path.stat().st_size / 1024:.1f} KB)")
            return cache_path

        except Exception as e:
            logger.error(f"Error writing cache {cache_path.name}: {e}")
            raise

    def invalidate(self, source: str, endpoint: str, params: dict) -> bool:
        """Delete a specific cache entry."""
        cache_path = self._get_cache_path(source, endpoint, params)
        if cache_path.exists():
            cache_path.unlink()
            logger.info(f"Invalidated: {cache_path.name}")
            return True
        return False

    def clear_expired(self) -> int:
        """Remove all expired cache files. Returns count of removed files."""
        removed = 0
        for cache_file in self.cache_dir.glob("*.json"):
            if self._is_expired(cache_file):
                cache_file.unlink()
                removed += 1
        if removed:
            logger.info(f"Cleared {removed} expired cache files")
        return removed

    def clear_all(self) -> int:
        """Remove all cache files. Returns count of removed files."""
        removed = 0
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink()
            removed += 1
        if removed:
            logger.info(f"Cleared all {removed} cache files")
        return removed

    def stats(self) -> dict:
        """Get cache statistics."""
        total = 0
        expired = 0
        total_size = 0

        for cache_file in self.cache_dir.glob("*.json"):
            total += 1
            total_size += cache_file.stat().st_size
            if self._is_expired(cache_file):
                expired += 1

        return {
            'total_files': total,
            'expired_files': expired,
            'valid_files': total - expired,
            'total_size_kb': total_size / 1024
        }


# Global cache instance
_cache: Optional[APICache] = None


def get_api_cache() -> APICache:
    """Get or create the global API cache instance."""
    global _cache
    if _cache is None:
        _cache = APICache()
    return _cache


def cached(source: str, endpoint: str, param_keys: list[str] = None):
    """
    Decorator to cache function results based on parameters.

    Args:
        source: API source name (e.g., 'mlb', 'statcast')
        endpoint: Endpoint/function name
        param_keys: List of parameter names to include in cache key.
                   If None, uses all kwargs.

    Usage:
        @cached('mlb', 'schedule', ['team', 'start_date', 'end_date'])
        def get_schedule(team, start_date, end_date):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache = get_api_cache()

            # Build params dict for cache key
            if param_keys:
                params = {k: kwargs.get(k) for k in param_keys if k in kwargs}
            else:
                params = kwargs.copy()

            # Include positional args by position
            import inspect
            sig = inspect.signature(func)
            param_names = list(sig.parameters.keys())
            for i, arg in enumerate(args):
                if i < len(param_names):
                    # Skip 'self' for methods
                    if param_names[i] != 'self':
                        params[param_names[i]] = arg

            # Check cache
            cached_result = cache.get(source, endpoint, params)
            if cached_result is not None:
                return cached_result

            # Call function and cache result
            result = func(*args, **kwargs)
            if result is not None:
                cache.set(source, endpoint, params, result)

            return result

        return wrapper
    return decorator
