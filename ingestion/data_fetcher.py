"""
Data fetcher orchestrator for game preview data.

Coordinates fetching from multiple sources (MLB API, FanGraphs, Statcast)
and integrates with cache layer for efficient data management.
"""

from typing import Dict, Any, Optional, List
from tqdm import tqdm
from ingestion.mlb_api_client import MLBStatsAPIClient
from ingestion.pybaseball_client import get_pitcher_stats, get_pitch_mix, get_batter_stats, get_batter_fangraphs_stats
from ingestion.weather_client import WeatherClient
from utils.team_data import get_team_full_name, get_team_logo_url, get_player_headshot_url
from utils.real_season_data import fetch_division_teams_data
from utils.re24_calculator import get_batter_season_re24
from config.logging_config import get_logger

logger = get_logger(__name__)


def get_division_from_team(team_abbr: str) -> str:
    """Get division name for a team."""
    divisions = {
        # AL East
        'NYY': 'AL East', 'BOS': 'AL East', 'TB': 'AL East',
        'TOR': 'AL East', 'BAL': 'AL East',

        # AL Central
        'CLE': 'AL Central', 'MIN': 'AL Central', 'CWS': 'AL Central',
        'DET': 'AL Central', 'KC': 'AL Central',

        # AL West
        'HOU': 'AL West', 'TEX': 'AL West', 'SEA': 'AL West',
        'LAA': 'AL West', 'OAK': 'AL West',

        # NL East
        'ATL': 'NL East', 'PHI': 'NL East', 'NYM': 'NL East',
        'MIA': 'NL East', 'WSH': 'NL East',

        # NL Central
        'MIL': 'NL Central', 'STL': 'NL Central', 'CHC': 'NL Central',
        'CIN': 'NL Central', 'PIT': 'NL Central',

        # NL West
        'LAD': 'NL West', 'SD': 'NL West', 'SF': 'NL West',
        'COL': 'NL West', 'ARI': 'NL West',
    }
    return divisions.get(team_abbr, 'Unknown')


def get_division_teams(division: str) -> list[str]:
    """Get all teams in a division."""
    division_teams = {
        'AL East': ['NYY', 'BOS', 'TB', 'TOR', 'BAL'],
        'AL Central': ['CLE', 'MIN', 'CWS', 'DET', 'KC'],
        'AL West': ['HOU', 'TEX', 'SEA', 'LAA', 'OAK'],
        'NL East': ['ATL', 'PHI', 'NYM', 'MIA', 'WSH'],
        'NL Central': ['MIL', 'STL', 'CHC', 'CIN', 'PIT'],
        'NL West': ['LAD', 'SD', 'SF', 'COL', 'ARI'],
    }
    return division_teams.get(division, [])


def get_team_short_name(team_abbr: str) -> str:
    """Get short team name (e.g., 'Yankees' instead of 'New York Yankees')."""
    short_names = {
        'NYY': 'Yankees', 'BOS': 'Red Sox', 'TB': 'Rays', 'TOR': 'Blue Jays', 'BAL': 'Orioles',
        'CLE': 'Guardians', 'MIN': 'Twins', 'CWS': 'White Sox', 'DET': 'Tigers', 'KC': 'Royals',
        'HOU': 'Astros', 'TEX': 'Rangers', 'SEA': 'Mariners', 'LAA': 'Angels', 'OAK': 'Athletics',
        'ATL': 'Braves', 'PHI': 'Phillies', 'NYM': 'Mets', 'MIA': 'Marlins', 'WSH': 'Nationals',
        'MIL': 'Brewers', 'STL': 'Cardinals', 'CHC': 'Cubs', 'CIN': 'Reds', 'PIT': 'Pirates',
        'LAD': 'Dodgers', 'SD': 'Padres', 'SF': 'Giants', 'COL': 'Rockies', 'ARI': 'Diamondbacks',
    }
    return short_names.get(team_abbr, team_abbr)


def format_game_date(date_str: str) -> str:
    """Format date as 'Thursday, September 25th, 2025'."""
    from datetime import datetime

    date = datetime.strptime(date_str, '%Y-%m-%d')
    day_name = date.strftime('%A')
    month_name = date.strftime('%B')
    day = date.day
    year = date.year

    # Add ordinal suffix
    if 11 <= day <= 13:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')

    return f"{day_name}, {month_name} {day}{suffix}, {year}"


def fetch_game_data(
    away_team: str,
    home_team: str,
    game_date: str,
    season: int = 2025,
    pitcher_names: Optional[Dict[str, str]] = None,
    pitcher_ids: Optional[Dict[str, int]] = None,
    lineups: Optional[Dict[str, list]] = None
) -> Dict[str, Any]:
    """
    Fetch complete game preview data from all sources.

    Data is cached at the API level (individual endpoints), not at the game level.
    This allows format changes without re-fetching from external APIs.

    Args:
        away_team: Away team abbreviation (e.g., 'NYY')
        home_team: Home team abbreviation (e.g., 'BOS')
        game_date: Game date in YYYY-MM-DD format
        season: Season year (default: 2025)
        pitcher_names: Optional dict with 'away' and 'home' pitcher names
        pitcher_ids: Optional dict with 'away' and 'home' MLB player IDs
        lineups: Optional dict with 'away' and 'home' lineup lists

    Returns:
        Complete game data dict ready for template rendering
    """
    logger.info(f"Fetching data for {away_team} @ {home_team} on {game_date}")

    # 1. Basic game info
    data = {
        'away_team': away_team,
        'home_team': home_team,
        'away_team_full': get_team_full_name(away_team),
        'home_team_full': get_team_full_name(home_team),
        'away_team_short': get_team_short_name(away_team),
        'home_team_short': get_team_short_name(home_team),
        'away_team_logo': get_team_logo_url(away_team),
        'home_team_logo': get_team_logo_url(home_team),
        'game_date': game_date,
        'game_date_formatted': format_game_date(game_date),
        'away_division': get_division_from_team(away_team),
        'home_division': get_division_from_team(home_team),
    }

    # 2. Fetch game details from MLB API
    logger.info("Fetching game details from MLB API...")
    game_id = None
    try:
        client = MLBStatsAPIClient()
        schedule = client.get_schedule(team=home_team, start_date=game_date, end_date=game_date)

        if schedule:
            game = schedule[0]
            game_id = game.get('game_id')
            data['game_time'] = game.get('game_time', 'TBD')
            data['venue'] = game.get('venue_name', 'Unknown Venue')
            data['game_id'] = game_id

            # Fetch weather for the venue
            try:
                weather_client = WeatherClient()
                temperature = weather_client.get_forecast_temperature(data['venue'])
                if temperature:
                    data['temperature'] = temperature
                    logger.info(f"Weather: {temperature}°F at {data['venue']}")
                else:
                    data['temperature'] = None
                    logger.warning(f"Could not fetch weather for {data['venue']}")
            except Exception as weather_error:
                logger.warning(f"Weather fetch error: {weather_error}")
                data['temperature'] = None

            # Get team records from standings (not from schedule)
            away_record = client.get_team_record(away_team, season)
            home_record = client.get_team_record(home_team, season)

            if away_record:
                data['away_record'] = f"{away_record['wins']}-{away_record['losses']}"
            else:
                data['away_record'] = "0-0"

            if home_record:
                data['home_record'] = f"{home_record['wins']}-{home_record['losses']}"
            else:
                data['home_record'] = "0-0"

            # Fetch schedule context (previous/upcoming games)
            logger.info("Fetching schedule context...")
            schedule_context = client.get_schedule_context(home_team, game_date)
            data['schedule_context'] = schedule_context

    except Exception as e:
        logger.warning(f"Could not fetch game details: {e}")
        data['game_time'] = 'TBD'
        data['venue'] = 'TBD'
        data['temperature'] = None
        data['away_record'] = '0-0'
        data['home_record'] = '0-0'

    # 3. Fetch pitcher stats from MLB API and pitch mix from Statcast
    if pitcher_ids:
        logger.info("Fetching pitcher stats from MLB API and pitch mix from Statcast...")

        if pitcher_ids.get('away'):
            # Get stats from MLB API
            away_pitcher_stats = client.get_pitcher_season_stats(pitcher_ids['away'], season)
            if away_pitcher_stats:
                data['away_pitcher'] = away_pitcher_stats
            else:
                logger.warning(f"Failed to fetch away pitcher stats from MLB API")

            # Get pitch mix from Statcast (using pitcher name if available)
            if pitcher_names and pitcher_names.get('away'):
                away_pitcher_pitches = get_pitch_mix(pitcher_names['away'], season)
                if away_pitcher_pitches:
                    data['away_pitcher_pitches'] = away_pitcher_pitches

        if pitcher_ids.get('home'):
            # Get stats from MLB API
            home_pitcher_stats = client.get_pitcher_season_stats(pitcher_ids['home'], season)
            if home_pitcher_stats:
                data['home_pitcher'] = home_pitcher_stats
            else:
                logger.warning(f"Failed to fetch home pitcher stats from MLB API")

            # Get pitch mix from Statcast (using pitcher name if available)
            if pitcher_names and pitcher_names.get('home'):
                home_pitcher_pitches = get_pitch_mix(pitcher_names['home'], season)
                if home_pitcher_pitches:
                    data['home_pitcher_pitches'] = home_pitcher_pitches

    # 4. Fetch lineups (automatically from game API or manually provided)
    if not lineups and game_id:
        # Try to fetch lineups automatically from game data
        logger.info("Fetching lineups from MLB game data...")
        try:
            api_lineups = client.get_game_lineups(game_id)
            if api_lineups:
                lineups = api_lineups
                logger.info(f"Retrieved {len(api_lineups['away'])} away, {len(api_lineups['home'])} home batters")
        except Exception as e:
            logger.warning(f"Could not fetch lineups automatically: {e}")

    # 5. Fetch lineup stats from MLB API (for each batter)
    if lineups:
        logger.info("Fetching lineup stats from MLB API...")

        if lineups.get('away'):
            away_lineup = []
            away_batters = lineups['away']

            # Progress bar for away lineup
            pbar = tqdm(away_batters, desc=f"  {away_team} lineup", unit="batter", leave=False)
            for batter in pbar:
                # Handle both manual format (tuple) and API format (dict)
                if isinstance(batter, dict):
                    name = batter['name']
                    position = batter.get('position', 'DH')
                    number = batter.get('number', 0)
                    player_id = batter.get('player_id')
                else:
                    # Manual format: (name, position, number)
                    name, position, number = batter
                    player_id = None

                # Update progress bar with current player
                pbar.set_postfix_str(name[:20])

                # Get stats from MLB API if we have player_id
                if player_id:
                    batter_stats = client.get_batter_season_stats(player_id, season)
                    if batter_stats:
                        batter_stats['position'] = position
                        batter_stats['number'] = number
                        batter_stats['headshot_url'] = get_player_headshot_url(player_id)
                        away_lineup.append(batter_stats)
                    else:
                        logger.warning(f"Failed to fetch stats for {name} (ID: {player_id})")

            if away_lineup:
                data['away_lineup'] = away_lineup

        if lineups.get('home'):
            home_lineup = []
            home_batters = lineups['home']

            # Progress bar for home lineup
            pbar = tqdm(home_batters, desc=f"  {home_team} lineup", unit="batter", leave=False)
            for batter in pbar:
                # Handle both manual format (tuple) and API format (dict)
                if isinstance(batter, dict):
                    name = batter['name']
                    position = batter.get('position', 'DH')
                    number = batter.get('number', 0)
                    player_id = batter.get('player_id')
                else:
                    # Manual format: (name, position, number)
                    name, position, number = batter
                    player_id = None

                # Update progress bar with current player
                pbar.set_postfix_str(name[:20])

                # Get stats from MLB API if we have player_id
                if player_id:
                    batter_stats = client.get_batter_season_stats(player_id, season)
                    if batter_stats:
                        batter_stats['position'] = position
                        batter_stats['number'] = number
                        batter_stats['headshot_url'] = get_player_headshot_url(player_id)
                        home_lineup.append(batter_stats)
                    else:
                        logger.warning(f"Failed to fetch stats for {name} (ID: {player_id})")

            if home_lineup:
                data['home_lineup'] = home_lineup

    # 5a. Enrich lineup stats with FanGraphs data (oWAR, dWAR, wRC+)
    logger.info("Enriching lineup stats with FanGraphs data...")
    try:
        for lineup_key in ['away_lineup', 'home_lineup']:
            if lineup_key in data:
                for batter in data[lineup_key]:
                    player_name = batter.get('name')
                    if player_name:
                        fg_stats = get_batter_fangraphs_stats(player_name, season)
                        if fg_stats:
                            # Use FanGraphs oWAR and dWAR
                            batter['owar'] = fg_stats.get('owar', 0.0)
                            batter['dwar'] = fg_stats.get('dwar', 0.0)
                            # Use wRC+ as OPS+ (similar metrics)
                            batter['ops_plus'] = fg_stats.get('wrc_plus', 100)
                            logger.debug(f"FanGraphs stats for {player_name}: oWAR={batter['owar']}, dWAR={batter['dwar']}, OPS+={batter['ops_plus']}")
                        else:
                            logger.debug(f"No FanGraphs stats found for {player_name}")
    except Exception as e:
        logger.warning(f"Failed to enrich lineup with FanGraphs stats: {e}")

    # 5b. Fetch bench players from MLB API (if game_id available)
    if game_id:
        logger.info("Fetching bench players from MLB game data...")
        try:
            bench_players = client.get_game_bench_players(game_id)
            if bench_players:
                # Fetch stats for bench players (no progress bar for bench - usually only 3-5 players)
                if bench_players.get('away'):
                    away_bench = []
                    for player in bench_players['away']:
                        batter_stats = client.get_batter_season_stats(player['player_id'], season)
                        if batter_stats:
                            batter_stats['position'] = player['position']
                            batter_stats['number'] = player['number']
                            batter_stats['headshot_url'] = get_player_headshot_url(player['player_id'])
                            away_bench.append(batter_stats)
                    if away_bench:
                        data['away_bench'] = away_bench
                        logger.info(f"Retrieved {len(away_bench)} bench players for {away_team}")

                if bench_players.get('home'):
                    home_bench = []
                    for player in bench_players['home']:
                        batter_stats = client.get_batter_season_stats(player['player_id'], season)
                        if batter_stats:
                            batter_stats['position'] = player['position']
                            batter_stats['number'] = player['number']
                            batter_stats['headshot_url'] = get_player_headshot_url(player['player_id'])
                            home_bench.append(batter_stats)
                    if home_bench:
                        data['home_bench'] = home_bench
                        logger.info(f"Retrieved {len(home_bench)} bench players for {home_team}")
        except Exception as e:
            logger.warning(f"Could not fetch bench players: {e}")

    # 5c. Fetch bullpen roster (if game_id and pitcher IDs available)
    if game_id and pitcher_ids:
        logger.info("Fetching bullpen roster...")
        try:
            # Note: We'll filter starters manually since the method gets all pitchers
            bullpen_data = client.get_bullpen_with_usage(
                game_id,
                starter_id=None,  # We'll filter both starters in post-processing
                days_back=3
            )
            if bullpen_data:
                # Filter out starters from bullpen lists
                away_starter_id = pitcher_ids.get('away')
                home_starter_id = pitcher_ids.get('home')

                if bullpen_data.get('away'):
                    away_bullpen = [p for p in bullpen_data['away'] if p['player_id'] != away_starter_id]
                    if away_bullpen:
                        data['away_bullpen'] = away_bullpen
                        logger.info(f"Retrieved {len(away_bullpen)} bullpen pitchers for {away_team}")

                if bullpen_data.get('home'):
                    home_bullpen = [p for p in bullpen_data['home'] if p['player_id'] != home_starter_id]
                    if home_bullpen:
                        data['home_bullpen'] = home_bullpen
                        logger.info(f"Retrieved {len(home_bullpen)} bullpen pitchers for {home_team}")
        except Exception as e:
            logger.warning(f"Could not fetch bullpen data: {e}")

    # 6. Fetch division race data
    logger.info("Fetching division race data from MLB API...")
    away_division = data['away_division']
    home_division = data['home_division']

    # Fetch away team's division
    away_division_teams = get_division_teams(away_division)
    away_division_data = fetch_division_teams_data(away_division_teams, season)

    if away_division_data:
        # Convert DataFrames to dict for JSON serialization
        serializable_data = {}
        for team, df in away_division_data.items():
            serializable_data[team] = df.to_dict(orient='records')
        data['division_race_data'] = {away_division: serializable_data}

    # Fetch home team's division if different
    if home_division != away_division:
        home_division_teams = get_division_teams(home_division)
        home_division_data = fetch_division_teams_data(home_division_teams, season)

        if home_division_data:
            # Convert DataFrames to dict for JSON serialization
            serializable_data = {}
            for team, df in home_division_data.items():
                serializable_data[team] = df.to_dict(orient='records')

            if 'division_race_data' not in data:
                data['division_race_data'] = {}
            data['division_race_data'][home_division] = serializable_data

    # 7. Fetch RE24 data for lineup players (home team only for now)
    logger.info("Fetching RE24 data for lineup players...")
    try:
        # Collect player IDs and names from home lineup
        if 'home_lineup' in data:
            home_player_ids = []
            home_player_names = {}

            for batter in data['home_lineup']:
                player_id = batter.get('player_id')
                if player_id:
                    home_player_ids.append(player_id)
                    # Use last name for chart labels (handle suffixes like Jr., Sr., II, III)
                    full_name = batter.get('name', '')
                    name_parts = full_name.split() if full_name else []
                    suffixes = {'Jr.', 'Sr.', 'II', 'III', 'IV', 'V'}
                    if len(name_parts) >= 2:
                        last_name = name_parts[-1]
                        if last_name in suffixes and len(name_parts) >= 3:
                            last_name = name_parts[-2]
                    else:
                        last_name = full_name or str(player_id)
                    home_player_names[player_id] = last_name

            if home_player_ids:
                home_re24_data = {}
                pbar = tqdm(home_player_ids, desc=f"  {home_team} RE24", unit="player", leave=False)
                for player_id in pbar:
                    name = home_player_names.get(player_id, str(player_id))
                    pbar.set_postfix_str(name[:15])

                    re24_result = get_batter_season_re24(player_id, season, end_date=game_date)
                    if re24_result['games']:
                        home_re24_data[name] = re24_result['games']

                if home_re24_data:
                    data['home_re24_data'] = home_re24_data
                    logger.info(f"Retrieved RE24 data for {len(home_re24_data)} {home_team} players")

        # Also fetch for away team
        if 'away_lineup' in data:
            away_player_ids = []
            away_player_names = {}

            for batter in data['away_lineup']:
                player_id = batter.get('player_id')
                if player_id:
                    away_player_ids.append(player_id)
                    # Use last name for chart labels (handle suffixes like Jr., Sr., II, III)
                    full_name = batter.get('name', '')
                    name_parts = full_name.split() if full_name else []
                    suffixes = {'Jr.', 'Sr.', 'II', 'III', 'IV', 'V'}
                    if len(name_parts) >= 2:
                        last_name = name_parts[-1]
                        if last_name in suffixes and len(name_parts) >= 3:
                            last_name = name_parts[-2]
                    else:
                        last_name = full_name or str(player_id)
                    away_player_names[player_id] = last_name

            if away_player_ids:
                away_re24_data = {}
                pbar = tqdm(away_player_ids, desc=f"  {away_team} RE24", unit="player", leave=False)
                for player_id in pbar:
                    name = away_player_names.get(player_id, str(player_id))
                    pbar.set_postfix_str(name[:15])

                    re24_result = get_batter_season_re24(player_id, season, end_date=game_date)
                    if re24_result['games']:
                        away_re24_data[name] = re24_result['games']

                if away_re24_data:
                    data['away_re24_data'] = away_re24_data
                    logger.info(f"Retrieved RE24 data for {len(away_re24_data)} {away_team} players")

    except Exception as e:
        logger.warning(f"Failed to fetch RE24 data: {e}")

    # 8. Fetch head-to-head record for the season
    logger.info("Fetching head-to-head season record...")
    try:
        client = MLBStatsAPIClient()
        h2h_record = client.get_head_to_head_record(
            team_abbr=home_team,
            opponent_abbr=away_team,
            season=season,
            before_date=game_date
        )
        data['h2h_season_wins'] = h2h_record['wins']
        data['h2h_season_losses'] = h2h_record['losses']
        logger.info(f"Season record {home_team} vs {away_team}: {h2h_record['wins']}-{h2h_record['losses']}")
    except Exception as e:
        logger.warning(f"Failed to fetch head-to-head record: {e}")

    # 9. Fetch additional context (Injuries, Transactions, Leaders)
    logger.info("Fetching additional context (Injuries, Transactions, Leaders)...")
    try:
        # Injuries
        data['injuries'] = {
            'away': client.get_team_injuries(away_team),
            'home': client.get_team_injuries(home_team)
        }

        # Transactions
        data['transactions'] = client.get_recent_transactions(game_date)

        # League Leaders
        data['league_leaders'] = client.get_league_leaders(season)

    except Exception as e:
        logger.warning(f"Failed to fetch additional context: {e}")

    # Add fetch timestamp
    from datetime import datetime
    data['fetched_at'] = datetime.now().isoformat()

    return data
