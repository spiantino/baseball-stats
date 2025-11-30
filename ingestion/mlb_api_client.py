"""
MLB Stats API client for official baseball data.

Provides a clean interface to the MLB Stats API with automatic
retry logic and caching support.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import statsapi

from config.settings import APIConfig
from config.logging_config import get_logger
from ingestion.resilient_fetcher import ResilientFetcher

logger = get_logger(__name__)


class MLBStatsAPIClient:
    """
    Client for the official MLB Stats API.

    Uses the statsapi library as a base with additional
    error handling and caching.
    """

    def __init__(self, config: Optional[APIConfig] = None):
        """
        Initialize MLB Stats API client.

        Args:
            config: API configuration (if None, uses global settings)
        """
        from config.settings import get_settings

        if config is None:
            config = get_settings().api

        self.config = config
        self.base_url = config.mlb_api_base_url
        self.fetcher = ResilientFetcher()

    def get_schedule(
        self,
        team: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        team_id: Optional[int] = None
    ) -> List[Dict]:
        """
        Get game schedule.

        Args:
            team: Team abbreviation (e.g., 'NYY')
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            team_id: Team ID (alternative to abbreviation)

        Returns:
            List of game dictionaries

        Example:
            >>> client = MLBStatsAPIClient()
            >>> games = client.get_schedule(team='NYY', start_date='2024-06-15')
        """
        try:
            # If team_id not provided, convert team abbreviation to ID
            if team and not team_id:
                team_id = self._get_team_id(team)

            # Use statsapi library
            schedule = statsapi.schedule(
                start_date=start_date,
                end_date=end_date,
                team=team_id if team_id else ''
            )

            logger.info(
                f"Retrieved {len(schedule)} games for team={team} "
                f"from {start_date} to {end_date}"
            )

            return schedule

        except Exception as e:
            logger.error(f"Failed to get schedule: {e}", exc_info=True)
            return []

    def get_game(self, game_id: int) -> Optional[Dict]:
        """
        Get detailed game data by game ID.

        Args:
            game_id: MLB game PK (primary key)

        Returns:
            Game data dict or None on failure

        Example:
            >>> client = MLBStatsAPIClient()
            >>> game = client.get_game(717159)
        """
        try:
            game_data = statsapi.get('game', {'gamePk': game_id})
            logger.info(f"Retrieved game data for gamePk={game_id}")
            return game_data

        except Exception as e:
            logger.error(f"Failed to get game {game_id}: {e}", exc_info=True)
            return None

    def get_game_boxscore(self, game_id: int) -> Optional[str]:
        """
        Get game boxscore as formatted text.

        Args:
            game_id: MLB game PK

        Returns:
            Boxscore string or None on failure
        """
        try:
            boxscore = statsapi.boxscore(game_id)
            logger.info(f"Retrieved boxscore for gamePk={game_id}")
            return boxscore

        except Exception as e:
            logger.error(f"Failed to get boxscore {game_id}: {e}", exc_info=True)
            return None

    def get_player_stats(
        self,
        player_id: int,
        stat_type: str = 'season',
        season: Optional[int] = None
    ) -> Optional[Dict]:
        """
        Get player statistics.

        Args:
            player_id: MLB player ID
            stat_type: Type of stats ('season', 'career', 'lastXGames')
            season: Specific season year

        Returns:
            Player stats dict or None on failure
        """
        try:
            params = {
                'personId': player_id,
                'hydrate': f'stats(group=[{stat_type}])'
            }

            if season:
                params['season'] = season

            player_data = statsapi.get('person', params)
            logger.info(f"Retrieved stats for player {player_id}")
            return player_data

        except Exception as e:
            logger.error(f"Failed to get player stats {player_id}: {e}", exc_info=True)
            return None

    def get_team_roster(self, team_id: int, season: Optional[int] = None) -> List[Dict]:
        """
        Get team roster.

        Args:
            team_id: MLB team ID
            season: Specific season year (default: current)

        Returns:
            List of player dictionaries
        """
        try:
            if season is None:
                season = datetime.now().year

            roster = statsapi.roster(team_id, season=season)
            logger.info(f"Retrieved roster for team {team_id}, season {season}")
            return roster if roster else []

        except Exception as e:
            logger.error(f"Failed to get roster for team {team_id}: {e}", exc_info=True)
            return []

    def get_standings(self, league: str = 'MLB', season: Optional[int] = None) -> str:
        """
        Get standings as formatted text.

        Args:
            league: League code ('MLB', 'AL', 'NL')
            season: Season year (default: current)

        Returns:
            Standings as formatted string
        """
        try:
            if season is None:
                season = datetime.now().year

            standings = statsapi.standings(leagueId=league, season=season)
            logger.info(f"Retrieved standings for {league} {season}")
            return standings

        except Exception as e:
            logger.error(f"Failed to get standings: {e}", exc_info=True)
            return ""

    def search_players(self, search_term: str, sport_id: int = 1) -> List[Dict]:
        """
        Search for players by name.

        Args:
            search_term: Player name or partial name
            sport_id: Sport ID (1 = MLB)

        Returns:
            List of matching player dictionaries
        """
        try:
            url = f"{self.base_url}/sports/{sport_id}/players"
            params = {'search': search_term}

            response = self.fetcher.get_json(url, params=params)

            if response and 'people' in response:
                players = response['people']
                logger.info(f"Found {len(players)} players matching '{search_term}'")
                return players

            return []

        except Exception as e:
            logger.error(f"Failed to search players '{search_term}': {e}", exc_info=True)
            return []

    def get_todays_games(self) -> List[Dict]:
        """
        Get today's games.

        Returns:
            List of today's game dictionaries
        """
        today = datetime.now().strftime('%Y-%m-%d')
        return self.get_schedule(start_date=today, end_date=today)

    def _get_team_id(self, team_abbr: str) -> Optional[int]:
        """
        Convert team abbreviation to team ID.

        Args:
            team_abbr: Team abbreviation (e.g., 'NYY')

        Returns:
            Team ID or None if not found
        """
        # Common team abbreviation to ID mapping
        team_map = {
            'NYY': 147, 'BOS': 111, 'TB': 139, 'TOR': 141, 'BAL': 110,
            'CLE': 114, 'MIN': 142, 'CWS': 145, 'DET': 116, 'KC': 118,
            'HOU': 117, 'TEX': 140, 'SEA': 136, 'LAA': 108, 'OAK': 133,
            'ATL': 144, 'PHI': 143, 'NYM': 121, 'MIA': 146, 'WSH': 120,
            'MIL': 158, 'STL': 138, 'CHC': 112, 'CIN': 113, 'PIT': 134,
            'LAD': 119, 'SD': 135, 'SF': 137, 'COL': 115, 'ARI': 109
        }

        # Normalize abbreviation
        team_abbr = team_abbr.upper()

        # Handle common variations
        if team_abbr == 'CHW':
            team_abbr = 'CWS'
        elif team_abbr == 'KCR':
            team_abbr = 'KC'
        elif team_abbr == 'WSN':
            team_abbr = 'WSH'

        return team_map.get(team_abbr)

    def close(self):
        """Close the HTTP fetcher."""
        if self.fetcher:
            self.fetcher.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
