"""
MLB Stats API client for official baseball data.

Provides a clean interface to the MLB Stats API with automatic
retry logic and caching support.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import statsapi
import requests

from config.settings import APIConfig, get_settings
from config.logging_config import get_logger
from utils.api_cache import get_api_cache

logger = get_logger(__name__)

# Get shared cache instance
_cache = get_api_cache()


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
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'baseball-stats/1.0'})

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

            # Check cache first
            cache_params = {'team_id': team_id, 'start_date': start_date, 'end_date': end_date}
            cached = _cache.get('mlb', 'schedule', cache_params)
            if cached is not None:
                logger.debug(f"Cache hit for schedule: team={team}")
                return cached

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

            # Cache the result
            _cache.set('mlb', 'schedule', cache_params, schedule)

            return schedule

        except Exception as e:
            logger.error(f"Failed to get schedule: {e}", exc_info=True)
            return []

    def get_schedule_context(
        self,
        team_abbr: str,
        game_date: str,
        weeks_before: int = 2,
        weeks_after: int = 2
    ) -> Dict[str, List[Dict]]:
        """
        Get schedule context for calendar display.

        Fetches the full season schedule (cached) and extracts a window
        around the game date for the calendar display.

        Args:
            team_abbr: Team abbreviation
            game_date: Reference date (YYYY-MM-DD)
            weeks_before: Number of weeks before game date to show
            weeks_after: Number of weeks after game date to show

        Returns:
            Dict with 'calendar' list for display
        """
        try:
            ref_date = datetime.strptime(game_date, '%Y-%m-%d')
            season = ref_date.year

            # Fetch full season schedule (will be cached)
            season_start = f"{season}-03-01"
            season_end = f"{season}-11-15"
            all_games = self.get_schedule(team=team_abbr, start_date=season_start, end_date=season_end)

            # Split into previous and upcoming relative to game_date
            previous = [g for g in all_games if g['game_date'] < game_date]
            upcoming = [g for g in all_games if g['game_date'] >= game_date]

            # Team abbreviation mapping for opponent display
            team_name_to_abbr = {
                'Yankees': 'NYY', 'Red Sox': 'BOS', 'Rays': 'TB', 'Blue Jays': 'TOR', 'Orioles': 'BAL',
                'Guardians': 'CLE', 'Twins': 'MIN', 'White Sox': 'CWS', 'Tigers': 'DET', 'Royals': 'KC',
                'Astros': 'HOU', 'Rangers': 'TEX', 'Mariners': 'SEA', 'Angels': 'LAA', 'Athletics': 'OAK',
                'Braves': 'ATL', 'Phillies': 'PHI', 'Mets': 'NYM', 'Marlins': 'MIA', 'Nationals': 'WSH',
                'Brewers': 'MIL', 'Cardinals': 'STL', 'Cubs': 'CHC', 'Reds': 'CIN', 'Pirates': 'PIT',
                'Dodgers': 'LAD', 'Padres': 'SD', 'Giants': 'SF', 'Rockies': 'COL', 'Diamondbacks': 'ARI'
            }

            def get_opponent_abbr(team_full_name: str) -> str:
                """Convert full team name to abbreviation."""
                for name, abbr in team_name_to_abbr.items():
                    if name in team_full_name:
                        return abbr
                # Fallback: last word, 3 chars
                return team_full_name.split()[-1][:3].upper() if team_full_name else 'TBD'

            # Transform data to add opponent_abbr, result, score for template
            def transform_game(game: Dict, team_abbr: str) -> Dict:
                """Add template-friendly fields to game data."""
                # Determine if team is home or away by checking the team abbreviation
                # in home_name. Note: home_name is the full name like "New York Yankees"
                home_abbr = get_opponent_abbr(game.get('home_name', ''))
                is_home = (home_abbr == team_abbr)

                # Get opponent
                if is_home:
                    opponent_full = game.get('away_name', '')
                    team_score = game.get('home_score', 0)
                    opp_score = game.get('away_score', 0)
                else:
                    opponent_full = game.get('home_name', '')
                    team_score = game.get('away_score', 0)
                    opp_score = game.get('home_score', 0)

                # Get proper opponent abbreviation
                opponent_abbr = get_opponent_abbr(opponent_full)

                # Use "vs" for home games, "@" for away games
                if is_home:
                    opponent_abbr = 'vs ' + opponent_abbr
                else:
                    opponent_abbr = '@ ' + opponent_abbr
                
                # Determine result (W/L) and score
                status = game.get('status', '')
                if 'Final' in status:
                    if team_score > opp_score:
                        result = 'W'
                    else:
                        result = 'L'
                    score = f"{team_score}-{opp_score}"
                else:
                    result = ''
                    score = ''
                
                # Extract game time for upcoming (convert to configured timezone)
                game_time = 'TBD'
                if game.get('game_datetime'):
                    try:
                        # Parse UTC datetime
                        dt = datetime.fromisoformat(game['game_datetime'].replace('Z', '+00:00'))
                        # Convert to configured timezone
                        tz = ZoneInfo(get_settings().app.timezone)
                        dt_local = dt.astimezone(tz)
                        game_time = dt_local.strftime('%-I:%M %p').lstrip('0')
                    except:
                        pass
                
                # Add to original data
                game['opponent_abbr'] = opponent_abbr
                game['result'] = result
                game['score'] = score
                game['game_time'] = game_time
                
                return game

            previous = [transform_game(g, team_abbr) for g in previous]
            upcoming = [transform_game(g, team_abbr) for g in upcoming]

            # Build a proper calendar with off days filled in
            def build_calendar_with_off_days(games: list, start_date: datetime, end_date: datetime) -> list:
                """Fill in off days to create a proper calendar grid."""
                # Create a dict of games by date
                games_by_date = {}
                for g in games:
                    games_by_date[g['game_date']] = g

                # Generate all dates in range
                calendar = []
                current = start_date
                while current <= end_date:
                    date_str = current.strftime('%Y-%m-%d')
                    if date_str in games_by_date:
                        calendar.append(games_by_date[date_str])
                    else:
                        # Add off day placeholder
                        calendar.append({
                            'game_date': date_str,
                            'status': 'OFF',
                            'opponent_abbr': '',
                            'result': '',
                            'score': '',
                            'game_time': ''
                        })
                    current += timedelta(days=1)
                return calendar

            # Build calendar centered on game date
            # Find the Sunday of the week containing game_date (weeks start on Sunday)
            # weekday() returns 0=Mon, 6=Sun, so Sunday is (weekday + 1) % 7 days ago
            days_since_sunday = (ref_date.weekday() + 1) % 7
            game_week_sunday = ref_date - timedelta(days=days_since_sunday)

            # Calculate calendar start (weeks_before weeks before game week)
            cal_start = game_week_sunday - timedelta(weeks=weeks_before)

            # Calculate calendar end (weeks_after weeks after game week, ending on Saturday)
            cal_end = game_week_sunday + timedelta(weeks=weeks_after + 1) - timedelta(days=1)

            # Combine all games for the calendar
            all_transformed = previous + upcoming

            # Build calendar with off days filled in
            full_calendar = build_calendar_with_off_days(all_transformed, cal_start, cal_end)

            # Mark which day is "today" (the game date)
            for g in full_calendar:
                g['is_today'] = (g['game_date'] == game_date)

            return {
                'previous': [],  # Not used with new calendar
                'upcoming': [],  # Not used with new calendar
                'calendar': full_calendar
            }
        except Exception as e:
            logger.error(f"Failed to get schedule context for {team_abbr}: {e}", exc_info=True)
            return {'previous': [], 'upcoming': []}

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

    def get_head_to_head_record(
        self,
        team_abbr: str,
        opponent_abbr: str,
        season: int,
        before_date: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Get head-to-head record between two teams for a season.

        Args:
            team_abbr: Main team abbreviation (e.g., 'NYY')
            opponent_abbr: Opponent team abbreviation (e.g., 'CWS')
            season: Season year
            before_date: Only count games before this date (YYYY-MM-DD)

        Returns:
            Dict with 'wins' and 'losses' from team_abbr's perspective
        """
        try:
            # Fetch full season schedule for the team (will be cached)
            season_start = f"{season}-03-01"
            season_end = f"{season}-11-15"
            all_games = self.get_schedule(team=team_abbr, start_date=season_start, end_date=season_end)

            # Team name to abbreviation mapping
            team_name_to_abbr = {
                'Yankees': 'NYY', 'Red Sox': 'BOS', 'Rays': 'TB', 'Blue Jays': 'TOR', 'Orioles': 'BAL',
                'Guardians': 'CLE', 'Twins': 'MIN', 'White Sox': 'CWS', 'Tigers': 'DET', 'Royals': 'KC',
                'Astros': 'HOU', 'Rangers': 'TEX', 'Mariners': 'SEA', 'Angels': 'LAA', 'Athletics': 'OAK',
                'Braves': 'ATL', 'Phillies': 'PHI', 'Mets': 'NYM', 'Marlins': 'MIA', 'Nationals': 'WSH',
                'Brewers': 'MIL', 'Cardinals': 'STL', 'Cubs': 'CHC', 'Reds': 'CIN', 'Pirates': 'PIT',
                'Dodgers': 'LAD', 'Padres': 'SD', 'Giants': 'SF', 'Rockies': 'COL', 'Diamondbacks': 'ARI'
            }

            def get_abbr(team_full_name: str) -> str:
                for name, abbr in team_name_to_abbr.items():
                    if name in team_full_name:
                        return abbr
                return ''

            wins = 0
            losses = 0

            for game in all_games:
                # Skip if after cutoff date
                if before_date and game.get('game_date', '') >= before_date:
                    continue

                # Check if this game is vs the opponent
                home_abbr = get_abbr(game.get('home_name', ''))
                away_abbr = get_abbr(game.get('away_name', ''))

                if opponent_abbr not in (home_abbr, away_abbr):
                    continue

                # Skip if not final
                if 'Final' not in game.get('status', ''):
                    continue

                # Determine if team won
                is_home = (home_abbr == team_abbr)
                if is_home:
                    team_score = game.get('home_score', 0)
                    opp_score = game.get('away_score', 0)
                else:
                    team_score = game.get('away_score', 0)
                    opp_score = game.get('home_score', 0)

                if team_score > opp_score:
                    wins += 1
                else:
                    losses += 1

            logger.info(f"Head-to-head {team_abbr} vs {opponent_abbr} ({season}): {wins}-{losses}")
            return {'wins': wins, 'losses': losses}

        except Exception as e:
            logger.error(f"Failed to get head-to-head record: {e}", exc_info=True)
            return {'wins': 0, 'losses': 0}

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

    def get_team_record(self, team_abbr: str, season: Optional[int] = None) -> Optional[Dict[str, int]]:
        """
        Get team's win-loss record from standings.

        Args:
            team_abbr: Team abbreviation (e.g., 'NYY')
            season: Season year (default: current)

        Returns:
            Dict with 'wins' and 'losses' or None if not found
        """
        if season is None:
            season = datetime.now().year

        # Check cache first
        cache_params = {'team_abbr': team_abbr, 'season': season}
        cached = _cache.get('mlb', 'team_record', cache_params)
        if cached is not None:
            return cached

        try:
            team_id = self._get_team_id(team_abbr)
            if not team_id:
                return None

            # Get standings data (both AL and NL)
            for league_id in ['103', '104']:  # AL=103, NL=104
                standings = statsapi.standings_data(leagueId=league_id, season=season)

                # Search through all divisions
                for division_data in standings.values():
                    for team in division_data['teams']:
                        if team.get('team_id') == team_id:
                            logger.info(f"Found record for {team_abbr}: {team['w']}-{team['l']}")
                            result = {'wins': team['w'], 'losses': team['l']}
                            _cache.set('mlb', 'team_record', cache_params, result)
                            return result

            logger.warning(f"Team {team_abbr} not found in standings")
            return None

        except Exception as e:
            logger.error(f"Failed to get team record for {team_abbr}: {e}", exc_info=True)
            return None

    def get_game_lineups(self, game_id: int) -> Optional[Dict[str, List[Dict]]]:
        """
        Get batting lineups for both teams from game data.

        Args:
            game_id: MLB game PK

        Returns:
            Dict with 'away' and 'home' lineup lists, or None on failure
            Each lineup item contains: name, position, batting_order, player_id
        """
        # Check cache first
        cache_params = {'game_id': game_id}
        cached = _cache.get('mlb', 'game_lineups', cache_params)
        if cached is not None:
            return cached

        try:
            game_data = statsapi.get('game', {'gamePk': game_id})

            if 'liveData' not in game_data or 'boxscore' not in game_data['liveData']:
                logger.warning(f"No boxscore data for game {game_id}")
                return None

            boxscore = game_data['liveData']['boxscore']
            lineups = {'away': [], 'home': []}

            for team_type in ['away', 'home']:
                if team_type not in boxscore['teams']:
                    continue

                team_data = boxscore['teams'][team_type]
                batting_order = team_data.get('battingOrder', [])
                players = team_data.get('players', {})

                for i, player_id in enumerate(batting_order, 1):
                    player_key = f'ID{player_id}'
                    if player_key in players:
                        player = players[player_key]
                        person = player.get('person', {})
                        position = player.get('position', {})

                        lineups[team_type].append({
                            'name': person.get('fullName', 'Unknown'),
                            'position': position.get('abbreviation', 'DH'),
                            'batting_order': i,
                            'player_id': player_id,
                            'number': person.get('primaryNumber', 0)
                        })

            logger.info(f"Retrieved lineups for game {game_id}: {len(lineups['away'])} away, {len(lineups['home'])} home")
            _cache.set('mlb', 'game_lineups', cache_params, lineups)
            return lineups

        except Exception as e:
            logger.error(f"Failed to get lineups for game {game_id}: {e}", exc_info=True)
            return None

    def get_game_bench_players(self, game_id: int) -> Optional[Dict[str, List[Dict]]]:
        """
        Get bench players (position players not in starting lineup) for both teams.

        Args:
            game_id: MLB game PK

        Returns:
            Dict with 'away' and 'home' bench player lists, or None on failure
            Each bench player contains: name, position, player_id, number
        """
        try:
            game_data = statsapi.get('game', {'gamePk': game_id})

            if 'liveData' not in game_data or 'boxscore' not in game_data['liveData']:
                logger.warning(f"No boxscore data for game {game_id}")
                return None

            boxscore = game_data['liveData']['boxscore']
            bench = {'away': [], 'home': []}

            for team_type in ['away', 'home']:
                if team_type not in boxscore['teams']:
                    continue

                team_data = boxscore['teams'][team_type]
                batting_order = set(team_data.get('battingOrder', []))
                players = team_data.get('players', {})

                # Get all position players not in batting order
                for player_key, player in players.items():
                    player_id = player.get('person', {}).get('id')

                    # Skip pitchers and players in batting order
                    position = player.get('position', {}).get('abbreviation', '')
                    if position == 'P' or player_id in batting_order:
                        continue

                    person = player.get('person', {})
                    bench[team_type].append({
                        'name': person.get('fullName', 'Unknown'),
                        'position': position,
                        'player_id': player_id,
                        'number': person.get('primaryNumber', 0)
                    })

            logger.info(f"Retrieved bench for game {game_id}: {len(bench['away'])} away, {len(bench['home'])} home")
            return bench

        except Exception as e:
            logger.error(f"Failed to get bench for game {game_id}: {e}", exc_info=True)
            return None

    def get_bullpen_with_usage(
        self,
        game_id: int,
        starter_id: Optional[int] = None,
        days_back: int = 3
    ) -> Optional[Dict[str, List[Dict]]]:
        """
        Get bullpen pitchers with recent pitch counts.

        Args:
            game_id: MLB game PK
            starter_id: Starter's player ID to exclude (optional)
            days_back: Number of days to look back for usage (default: 3)

        Returns:
            Dict with 'away' and 'home' bullpen lists, or None on failure
            Each pitcher contains: name, number, recent_games (list of pitch counts)
        """
        try:
            from datetime import datetime, timedelta

            game_data = statsapi.get('game', {'gamePk': game_id})

            if 'liveData' not in game_data or 'boxscore' not in game_data['liveData']:
                logger.warning(f"No boxscore data for game {game_id}")
                return None

            # Get game date
            game_date_str = game_data.get('gameData', {}).get('datetime', {}).get('officialDate')
            if not game_date_str:
                logger.warning(f"No game date for {game_id}")
                return None

            game_date = datetime.strptime(game_date_str, '%Y-%m-%d')

            boxscore = game_data['liveData']['boxscore']
            bullpen = {'away': [], 'home': []}

            for team_type in ['away', 'home']:
                if team_type not in boxscore['teams']:
                    continue

                team_data = boxscore['teams'][team_type]
                team_id = team_data.get('team', {}).get('id')
                players = team_data.get('players', {})

                # Get all pitchers (excluding starter if provided)
                for player_key, player in players.items():
                    position = player.get('position', {}).get('abbreviation', '')
                    if position != 'P':
                        continue

                    person = player.get('person', {})
                    player_id = person.get('id')

                    # Skip the starting pitcher
                    if starter_id and player_id == starter_id:
                        continue

                    # Get recent pitch counts for this pitcher
                    recent_games = self._get_recent_pitch_counts(
                        player_id,
                        team_id,
                        game_date,
                        days_back
                    )

                    bullpen[team_type].append({
                        'name': person.get('fullName', 'Unknown'),
                        'number': person.get('primaryNumber', 0),
                        'player_id': player_id,
                        'recent_games': recent_games
                    })

            # Sort by total pitches in last 3 days (most used first)
            for team_type in ['away', 'home']:
                bullpen[team_type].sort(
                    key=lambda p: sum(g['pitches'] for g in p['recent_games']),
                    reverse=True
                )

            logger.info(f"Retrieved bullpen for game {game_id}: {len(bullpen['away'])} away, {len(bullpen['home'])} home")
            return bullpen

        except Exception as e:
            logger.error(f"Failed to get bullpen for game {game_id}: {e}", exc_info=True)
            return None

    def _get_recent_pitch_counts(
        self,
        player_id: int,
        team_id: int,
        game_date: datetime,
        days_back: int
    ) -> List[Dict]:
        """
        Get pitch counts for a pitcher's recent games.

        Returns:
            List of dicts with 'date', 'pitches' for last N days
        """
        try:
            # Calculate date range
            start_date = (game_date - timedelta(days=days_back)).strftime('%Y-%m-%d')
            end_date = (game_date - timedelta(days=1)).strftime('%Y-%m-%d')

            # Get team's schedule for this period
            # We need to check all games the team played to see if this pitcher appeared
            schedule = statsapi.schedule(
                start_date=start_date,
                end_date=end_date,
                team=team_id
            )

            recent_usage = []

            for game in schedule:
                game_id = game['game_id']
                game_dt = game['game_date']
                
                # Get boxscore for each game
                # Note: This is expensive (N+1 queries), but necessary for accurate pitch counts
                # Optimization: Could cache boxscores if we run this often
                try:
                    game_data = statsapi.get('game', {'gamePk': game_id})
                    if 'liveData' not in game_data or 'boxscore' not in game_data['liveData']:
                        continue
                        
                    boxscore = game_data['liveData']['boxscore']
                    
                    # Find player in boxscore
                    for team_type in ['away', 'home']:
                        team_data = boxscore['teams'][team_type]
                        players = team_data.get('players', {})
                        
                        player_key = f'ID{player_id}'
                        if player_key in players:
                            player_stats = players[player_key].get('stats', {}).get('pitching', {})
                            pitches = int(player_stats.get('numberOfPitches', 0))
                            
                            if pitches > 0:
                                recent_usage.append({
                                    'date': game_dt,
                                    'pitches': pitches,
                                    'game_id': game_id
                                })
                except Exception:
                    continue

            return recent_usage

        except Exception as e:
            logger.error(f"Error getting recent pitch counts for {player_id}: {e}")
            return []

    def get_team_injuries(self, team_abbr: str) -> List[Dict]:
        """
        Get current injuries for a team.

        Args:
            team_abbr: Team abbreviation

        Returns:
            List of injury dicts
        """
        # Check cache first
        cache_params = {'team_abbr': team_abbr}
        cached = _cache.get('mlb', 'team_injuries', cache_params)
        if cached is not None:
            return cached

        try:
            team_id = self._get_team_id(team_abbr)
            if not team_id:
                return []
                
            # Use statsapi to get roster with specific type
            # Note: statsapi.roster doesn't support rosterType='40Man' directly via args in all versions,
            # but we can use the endpoint directly if needed. 
            # However, let's try to fetch from the league-wide injury endpoint or roster endpoint.
            
            # Using the specific endpoint for transactions/injuries is better but complex.
            # Simpler approach: Get current roster and check status codes, 
            # OR use a known endpoint for injuries if available.
            
            # statsapi doesn't have a direct 'injuries' method.
            # We will use the generic get request to /sports/1/players with status filter
            
            # Status codes for IL:
            # D10: 10-day IL
            # D15: 15-day IL
            # D60: 60-day IL
            
            # Alternative: Fetch full 40-man roster and filter by status
            roster = statsapi.get('team_roster', {'teamId': team_id, 'rosterType': '40Man'})
            
            injuries = []
            if 'roster' in roster:
                for player in roster['roster']:
                    status = player.get('status', {})
                    code = status.get('code')
                    
                    if code in ['D10', 'D15', 'D60', 'D7']:
                        person = player.get('person', {})
                        injuries.append({
                            'player': person.get('fullName'),
                            'player_id': person.get('id'),
                            'status': status.get('description'),
                            'code': code,
                            'date': status.get('startDate', '')
                        })

            _cache.set('mlb', 'team_injuries', cache_params, injuries)
            return injuries

        except Exception as e:
            logger.error(f"Failed to get injuries for {team_abbr}: {e}", exc_info=True)
            return []

    def get_recent_transactions(self, date: str, days_back: int = 3) -> List[Dict]:
        """
        Get recent transactions across MLB.

        Args:
            date: Reference date
            days_back: Days to look back

        Returns:
            List of transaction dicts
        """
        # Check cache first
        cache_params = {'date': date, 'days_back': days_back}
        cached = _cache.get('mlb', 'transactions', cache_params)
        if cached is not None:
            return cached

        try:
            end_date_dt = datetime.strptime(date, '%Y-%m-%d')
            start_date_dt = end_date_dt - timedelta(days=days_back)

            # Use direct HTTP request since statsapi library has parameter validation issues
            url = f"{self.base_url}/transactions"
            params = {
                'startDate': start_date_dt.strftime('%Y-%m-%d'),
                'endDate': end_date_dt.strftime('%Y-%m-%d')
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            transactions = response.json()
            
            # Filter and format
            formatted = []
            if 'transactions' in transactions:
                for t in transactions['transactions']:
                    # We want significant moves: Roster moves, trades, injuries
                    # Filter out minor league moves if possible, but for now take all
                    formatted.append({
                        'date': t.get('date'),
                        'description': t.get('description'),
                        'team': t.get('fromTeam', {}).get('abbreviation') or t.get('toTeam', {}).get('abbreviation') or 'MLB',
                        'player': t.get('person', {}).get('fullName')
                    })
            
            # Sort by date desc
            formatted.sort(key=lambda x: x['date'], reverse=True)
            result = formatted[:10]  # Return top 10
            _cache.set('mlb', 'transactions', cache_params, result)
            return result

        except Exception as e:
            logger.error(f"Failed to get transactions: {e}", exc_info=True)
            return []

    def get_league_leaders(self, season: int, limit: int = 5) -> Dict[str, List[Dict]]:
        """
        Get league leaders for batting and pitching.

        Args:
            season: Season year
            limit: Number of leaders per category

        Returns:
            Dict with 'batting' and 'pitching' leader lists
        """
        # Check cache first
        cache_params = {'season': season, 'limit': limit}
        cached = _cache.get('mlb', 'league_leaders', cache_params)
        if cached is not None:
            return cached

        try:
            leaders = {'batting': [], 'pitching': []}
            
            # Team ID mapping
            team_id_to_abbr = {
                147: 'NYY', 111: 'BOS', 139: 'TB', 141: 'TOR', 110: 'BAL',
                114: 'CLE', 142: 'MIN', 145: 'CWS', 116: 'DET', 118: 'KC',
                117: 'HOU', 140: 'TEX', 136: 'SEA', 108: 'LAA', 133: 'OAK',
                144: 'ATL', 143: 'PHI', 121: 'NYM', 146: 'MIA', 120: 'WSH',
                158: 'MIL', 138: 'STL', 112: 'CHC', 113: 'CIN', 134: 'PIT',
                119: 'LAD', 135: 'SD', 137: 'SF', 115: 'COL', 109: 'ARI'
            }

            url = f"{self.base_url}/stats/leaders"
            
            # Batting (OPS, HR, AVG)
            params = {
                'leaderCategories': 'onBasePlusSlugging,homeRuns,battingAverage',
                'season': season,
                'limit': limit,
                'sportId': 1,
                'statGroup': 'hitting'
            }
            
            resp = self.session.get(url, params=params).json()
            
            # Collect all player IDs
            batter_ids = set()
            pitcher_ids = set()
            
            # Helper to extract IDs from response
            def extract_ids(resp_json, category_list):
                ids = set()
                if 'leagueLeaders' in resp_json:
                    for cat in resp_json['leagueLeaders']:
                        if cat['leaderCategory'] in category_list:
                            for player in cat['leaders']:
                                ids.add(player.get('person', {}).get('id'))
                return ids

            # Batting IDs
            batter_ids = extract_ids(resp, ['onBasePlusSlugging', 'homeRuns', 'battingAverage'])
            
            # Pitching IDs (need to fetch pitching leaders first to get IDs)
            # Pitching (ERA, K, WHIP)
            params['leaderCategories'] = 'earnedRunAverage,strikeouts,whip'
            params['statGroup'] = 'pitching'
            pitching_resp = self.session.get(url, params=params).json()
            pitcher_ids = extract_ids(pitching_resp, ['earnedRunAverage', 'strikeouts', 'whip'])
            
            # Batch fetch stats for batters
            if batter_ids:
                b_ids_str = ",".join(str(i) for i in batter_ids)
                b_stats = self.session.get(f"{self.base_url}/people", params={
                    'personIds': b_ids_str,
                    'hydrate': 'stats(group=[hitting],type=[season],season=2025)' # Hardcoded 2025 for now based on context, but should use season arg
                }).json()
                
                # Create a lookup dict
                b_lookup = {}
                if 'people' in b_stats:
                    for p in b_stats['people']:
                        pid = p['id']
                        stats = p.get('stats', [{}])[0].get('splits', [{}])[0].get('stat', {})
                        b_lookup[pid] = {
                            'OPS': stats.get('ops', '-'),
                            'HR': stats.get('homeRuns', '-'),
                            'AVG': stats.get('avg', '-')
                        }
            
            # Batch fetch stats for pitchers
            if pitcher_ids:
                p_ids_str = ",".join(str(i) for i in pitcher_ids)
                p_stats = self.session.get(f"{self.base_url}/people", params={
                    'personIds': p_ids_str,
                    'hydrate': 'stats(group=[pitching],type=[season],season=2025)'
                }).json()
                
                p_lookup = {}
                if 'people' in p_stats:
                    for p in p_stats['people']:
                        pid = p['id']
                        stats = p.get('stats', [{}])[0].get('splits', [{}])[0].get('stat', {})
                        p_lookup[pid] = {
                            'ERA': stats.get('era', '-'),
                            'K': stats.get('strikeOuts', '-'),
                            'WHIP': stats.get('whip', '-')
                        }

            # Build final list using OPS leaders as base
            if 'leagueLeaders' in resp:
                ops_leaders = next((c for c in resp['leagueLeaders'] if c['leaderCategory'] == 'onBasePlusSlugging'), None)
                if ops_leaders:
                    for player in ops_leaders['leaders']:
                        pid = player.get('person', {}).get('id')
                        tid = player.get('team', {}).get('id')
                        # Use hydrated stats if available, else fall back to leader value (which is just OPS)
                        stats = b_lookup.get(pid, {})
                        leaders['batting'].append({
                            'rank': player.get('rank'),
                            'name': player.get('person', {}).get('fullName'),
                            'team': team_id_to_abbr.get(tid, 'MLB'),
                            'OPS': stats.get('OPS', player.get('value')),
                            'HR': stats.get('HR', '-'),
                            'AVG': stats.get('AVG', '-')
                        })

            # Build final list using ERA leaders as base
            if 'leagueLeaders' in pitching_resp:
                era_leaders = next((c for c in pitching_resp['leagueLeaders'] if c['leaderCategory'] == 'earnedRunAverage'), None)
                if era_leaders:
                    for player in era_leaders['leaders']:
                        pid = player.get('person', {}).get('id')
                        tid = player.get('team', {}).get('id')
                        stats = p_lookup.get(pid, {})
                        leaders['pitching'].append({
                            'rank': player.get('rank'),
                            'name': player.get('person', {}).get('fullName'),
                            'team': team_id_to_abbr.get(tid, 'MLB'),
                            'ERA': stats.get('ERA', player.get('value')),
                            'K': stats.get('K', '-'),
                            'WHIP': stats.get('WHIP', '-')
                        })

            _cache.set('mlb', 'league_leaders', cache_params, leaders)
            return leaders

        except Exception as e:
            logger.error(f"Failed to get league leaders: {e}", exc_info=True)
            return {'batting': [], 'pitching': []}

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

            response = self.session.get(url, params=params, timeout=10).json()

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

    def get_player_info(self, player_id: int) -> Optional[Dict[str, Any]]:
        """
        Get player information including their jersey number.

        Args:
            player_id: MLB player ID

        Returns:
            Dict with player info or None if not found
        """
        try:
            response = statsapi.get('person', {'personId': player_id})
            if 'people' in response and len(response['people']) > 0:
                player = response['people'][0]
                return {
                    'name': player.get('fullName', ''),
                    'number': player.get('primaryNumber', '0'),
                    'position': player.get('primaryPosition', {}).get('abbreviation', '')
                }
            return None
        except Exception as e:
            logger.error(f"Failed to get player info for {player_id}: {e}", exc_info=True)
            return None

    def get_pitcher_season_stats(self, player_id: int, season: int) -> Optional[Dict[str, Any]]:
        """
        Get pitcher season stats from MLB API.

        Args:
            player_id: MLB player ID
            season: Season year

        Returns:
            Dict with formatted pitcher stats or None
        """
        # Check cache first
        cache_params = {'player_id': player_id, 'season': season}
        cached = _cache.get('mlb', 'pitcher_stats', cache_params)
        if cached is not None:
            return cached

        try:
            # Use statsapi.player_stats() which properly handles stats fetching
            stats_text = statsapi.player_stats(player_id, 'pitching', 'season')

            # Parse the text output (statsapi returns formatted text)
            # Extract stats from the text
            stats_dict = {}
            for line in stats_text.split('\n'):
                if ': ' in line:
                    key, value = line.split(': ', 1)
                    stats_dict[key.strip()] = value.strip()

            # Get player info for name, number, etc.
            player_info = self.get_player_info(player_id)
            person_data = statsapi.get('person', {'personId': player_id})
            person = person_data['people'][0] if person_data.get('people') else {}

            # Convert stats to our format
            wins = int(stats_dict.get('wins', 0))
            losses = int(stats_dict.get('losses', 0))
            era = float(stats_dict.get('era', 0))
            ip = float(stats_dict.get('inningsPitched', 0))
            whip = float(stats_dict.get('whip', 0))

            # Calculate rate stats
            k9 = float(stats_dict.get('strikeoutsPer9Inn', 0))
            bb9 = float(stats_dict.get('walksPer9Inn', 0))
            hr9 = float(stats_dict.get('homeRunsPer9', 0))

            result = {
                'name': person.get('fullName', player_info.get('name', '') if player_info else ''),
                'hand': person.get('pitchHand', {}).get('code', 'R'),
                'number': int(player_info.get('number', 0)) if player_info else 0,
                'headshot_url': f"https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/{player_id}/headshot/67/current",
                'WAR': 0.0,  # Not in basic stats, would need advanced endpoint
                'wins': wins,
                'losses': losses,
                'ERA': era,
                'IP': ip,
                'K/9': k9,
                'BB/9': bb9,
                'HR/9': hr9,
                'WHIP': whip,
                'GB%': 45.0  # Not in basic stats
            }

            # Cache the result
            _cache.set('mlb', 'pitcher_stats', cache_params, result)
            return result

        except Exception as e:
            logger.error(f"Error getting pitcher stats for {player_id}: {e}", exc_info=True)
            return None

    def get_batter_season_stats(self, player_id: int, season: int) -> Optional[Dict[str, Any]]:
        """
        Get batter season stats from MLB API.

        Args:
            player_id: MLB player ID
            season: Season year

        Returns:
            Dict with formatted batter stats or None
        """
        # Check cache first
        cache_params = {'player_id': player_id, 'season': season}
        cached = _cache.get('mlb', 'batter_stats', cache_params)
        if cached is not None:
            return cached

        try:
            # Use statsapi.player_stats() which properly handles stats fetching
            stats_text = statsapi.player_stats(player_id, 'hitting', 'season')

            # Parse the text output
            stats_dict = {}
            for line in stats_text.split('\n'):
                if ': ' in line:
                    key, value = line.split(': ', 1)
                    stats_dict[key.strip()] = value.strip()

            # Get player info
            person_data = statsapi.get('person', {'personId': player_id})
            person = person_data['people'][0] if person_data.get('people') else {}

            # Extract stats
            avg = float(stats_dict.get('avg', 0))
            obp = float(stats_dict.get('obp', 0))
            slg = float(stats_dict.get('slg', 0))
            slash = f".{int(avg*1000):03d}/.{int(obp*1000):03d}/.{int(slg*1000):03d}"

            # Calculate OPS
            ops = obp + slg

            # OPS+ (if available, otherwise estimate from OPS)
            ops_plus = 100  # Default, would need league average to calculate

            result = {
                'name': person.get('fullName', ''),
                'player_id': player_id,
                'age': person.get('currentAge', 0),
                'war': 0.0,  # Not in basic stats
                'slash': slash,
                'ops': ops,
                'ops_plus': ops_plus,
                'hr': int(stats_dict.get('homeRuns', 0)),
                'rbi': int(stats_dict.get('rbi', 0)),
                'tb': int(stats_dict.get('totalBases', 0)),
                'sb': int(stats_dict.get('stolenBases', 0)),
                'owar': 0.0,
                'dwar': 0.0
            }

            # Cache the result
            _cache.set('mlb', 'batter_stats', cache_params, result)
            return result

        except Exception as e:
            logger.error(f"Error getting batter stats for {player_id}: {e}", exc_info=True)
            return None

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
        """Close the HTTP session."""
        if self.session:
            self.session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
