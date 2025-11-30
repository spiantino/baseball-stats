"""
Team data utilities - logos, full names, colors, etc.
"""

# MLB team full names
TEAM_FULL_NAMES = {
    # AL East
    'NYY': 'New York Yankees',
    'BOS': 'Boston Red Sox',
    'TB': 'Tampa Bay Rays',
    'TOR': 'Toronto Blue Jays',
    'BAL': 'Baltimore Orioles',

    # AL Central
    'CLE': 'Cleveland Guardians',
    'MIN': 'Minnesota Twins',
    'CWS': 'Chicago White Sox',
    'DET': 'Detroit Tigers',
    'KC': 'Kansas City Royals',

    # AL West
    'HOU': 'Houston Astros',
    'TEX': 'Texas Rangers',
    'SEA': 'Seattle Mariners',
    'LAA': 'Los Angeles Angels',
    'OAK': 'Oakland Athletics',

    # NL East
    'ATL': 'Atlanta Braves',
    'PHI': 'Philadelphia Phillies',
    'NYM': 'New York Mets',
    'MIA': 'Miami Marlins',
    'WSH': 'Washington Nationals',

    # NL Central
    'MIL': 'Milwaukee Brewers',
    'STL': 'St. Louis Cardinals',
    'CHC': 'Chicago Cubs',
    'CIN': 'Cincinnati Reds',
    'PIT': 'Pittsburgh Pirates',

    # NL West
    'LAD': 'Los Angeles Dodgers',
    'SD': 'San Diego Padres',
    'SF': 'San Francisco Giants',
    'COL': 'Colorado Rockies',
    'ARI': 'Arizona Diamondbacks',
}

# MLB team IDs (for API calls and logo URLs)
TEAM_IDS = {
    'NYY': 147, 'BOS': 111, 'TB': 139, 'TOR': 141, 'BAL': 110,
    'CLE': 114, 'MIN': 142, 'CWS': 145, 'DET': 116, 'KC': 118,
    'HOU': 117, 'TEX': 140, 'SEA': 136, 'LAA': 108, 'OAK': 133,
    'ATL': 144, 'PHI': 143, 'NYM': 121, 'MIA': 146, 'WSH': 120,
    'MIL': 158, 'STL': 138, 'CHC': 112, 'CIN': 113, 'PIT': 134,
    'LAD': 119, 'SD': 135, 'SF': 137, 'COL': 115, 'ARI': 109,
}


def get_team_full_name(abbr: str) -> str:
    """
    Get full team name from abbreviation.

    Args:
        abbr: Team abbreviation (e.g., 'NYY')

    Returns:
        Full team name (e.g., 'New York Yankees')
    """
    return TEAM_FULL_NAMES.get(abbr, abbr)


def get_team_logo_url(abbr: str) -> str:
    """
    Get team logo URL from MLB static assets.

    Args:
        abbr: Team abbreviation (e.g., 'NYY')

    Returns:
        URL to team logo SVG
    """
    team_id = TEAM_IDS.get(abbr)
    if team_id:
        return f"https://www.mlbstatic.com/team-logos/{team_id}.svg"
    return ""


def get_player_headshot_url(player_id: int) -> str:
    """
    Get player headshot URL from MLB static assets.

    Args:
        player_id: MLB player ID

    Returns:
        URL to player headshot image
    """
    # MLB uses a CDN with fallback to generic headshot
    return f"https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/{player_id}/headshot/67/current"


def get_team_id(abbr: str) -> int:
    """
    Get MLB team ID from abbreviation.

    Args:
        abbr: Team abbreviation (e.g., 'NYY')

    Returns:
        MLB team ID
    """
    return TEAM_IDS.get(abbr, 0)
