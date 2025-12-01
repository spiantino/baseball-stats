"""
Games Behind (GB) trend chart for division standings.

Visualizes how teams' positions in the division race change over time.
Uses Matplotlib for publication-quality print output.
"""

from typing import List, Dict, Optional
from pathlib import Path
import pandas as pd

# Set non-interactive backend to prevent macOS dock icon
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.figure as mpl_fig
from matplotlib.patches import Rectangle
from matplotlib import font_manager
import plotly.graph_objects as go  # For legacy functions (to be converted)

from config.logging_config import get_logger

logger = get_logger(__name__)

# Register custom fonts from local fonts directory
_fonts_dir = Path(__file__).parent.parent / 'fonts'
if _fonts_dir.exists():
    for font_file in _fonts_dir.glob('*.ttf'):
        try:
            font_manager.fontManager.addfont(str(font_file))
            logger.debug(f"Registered font: {font_file.name}")
        except Exception as e:
            logger.warning(f"Failed to register font {font_file.name}: {e}")


# MLB team colors (primary colors)
TEAM_COLORS = {
    'NYY': '#003087',  # Yankees blue
    'BOS': '#BD3039',  # Red Sox red
    'TB': '#092C5C',   # Rays blue
    'TOR': '#134A8E',  # Blue Jays blue
    'BAL': '#DF4601',  # Orioles orange
    'CLE': '#E31937',  # Guardians red
    'MIN': '#002B5C',  # Twins navy
    'CWS': '#27251F',  # White Sox black
    'DET': '#0C2340',  # Tigers navy
    'KC': '#004687',   # Royals blue
    'HOU': '#EB6E1F',  # Astros orange
    'TEX': '#003278',  # Rangers blue
    'SEA': '#0C2C56',  # Mariners navy
    'LAA': '#BA0021',  # Angels red
    'OAK': '#003831',  # Athletics green
    'ATL': '#CE1141',  # Braves red
    'PHI': '#E81828',  # Phillies red
    'NYM': '#002D72',  # Mets blue
    'MIA': '#00A3E0',  # Marlins blue
    'WSH': '#AB0003',  # Nationals red
    'MIL': '#FFC52F',  # Brewers gold
    'STL': '#C41E3A',  # Cardinals red
    'CHC': '#0E3386',  # Cubs blue
    'CIN': '#C6011F',  # Reds red
    'PIT': '#FDB827',  # Pirates gold
    'LAD': '#005A9C',  # Dodgers blue
    'SD': '#2F241D',   # Padres brown
    'SF': '#FD5A1E',   # Giants orange
    'COL': '#33006F',  # Rockies purple
    'ARI': '#A71930',  # Diamondbacks red
}


def create_games_behind_chart(
    team_data: Dict[str, pd.DataFrame],
    title: Optional[str] = None
) -> go.Figure:
    """
    Create games behind trend chart for multiple teams.

    Args:
        team_data: Dict mapping team abbr -> DataFrame with columns ['date', 'games_behind']
        title: Chart title (optional)

    Returns:
        Plotly Figure

    Example:
        >>> team_data = {
        ...     'NYY': pd.DataFrame({
        ...         'date': ['2024-04-01', '2024-04-02'],
        ...         'games_behind': [0.0, 0.5]
        ...     }),
        ...     'BOS': pd.DataFrame({
        ...         'date': ['2024-04-01', '2024-04-02'],
        ...         'games_behind': [2.0, 1.5]
        ...     })
        ... }
        >>> fig = create_games_behind_chart(team_data)
    """
    fig = go.Figure()

    # Add a trace for each team
    for team, df in team_data.items():
        if df.empty:
            logger.warning(f"No data for team {team}")
            continue

        # Get team color
        color = TEAM_COLORS.get(team, '#333333')

        # Add line with markers
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['games_behind'],
            mode='lines+markers',
            name=team,
            line=dict(color=color, width=3),
            marker=dict(size=6),
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         'Date: %{x|%b %d}<br>' +
                         'GB: %{y:.1f}<br>' +
                         '<extra></extra>'
        ))

    # Update layout
    if title is None:
        title = 'Division Race: Games Behind Leader'

    fig.update_layout(
        title=dict(
            text=title,
            x=0.5,
            xanchor='center',
            font=dict(size=18, color='#333')
        ),
        xaxis=dict(
            title='Date',
            showgrid=True,
            gridcolor='#E5E5E5'
        ),
        yaxis=dict(
            title='Games Behind',
            showgrid=True,
            gridcolor='#E5E5E5',
            # Reverse y-axis so 0 is at top (leader)
            autorange='reversed'
        ),
        hovermode='x unified',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5
        ),
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(family='Arial, sans-serif')
    )

    logger.info(f"Created games behind chart with {len(team_data)} teams")
    return fig


def create_division_standings_chart(
    standings_df: pd.DataFrame,
    division_name: str
) -> go.Figure:
    """
    Create horizontal bar chart of current division standings.

    Args:
        standings_df: DataFrame with columns ['team', 'wins', 'losses', 'pct', 'gb']
        division_name: Division name (e.g., 'AL East')

    Returns:
        Plotly Figure

    Example:
        >>> standings = pd.DataFrame({
        ...     'team': ['NYY', 'BAL', 'TB', 'TOR', 'BOS'],
        ...     'wins': [95, 92, 85, 80, 75],
        ...     'losses': [67, 70, 77, 82, 87],
        ...     'gb': [0, 3, 10, 15, 20]
        ... })
        >>> fig = create_division_standings_chart(standings, 'AL East')
    """
    # Sort by games behind (ascending)
    standings_df = standings_df.sort_values('gb')

    # Get colors for each team
    colors = [TEAM_COLORS.get(team, '#333333') for team in standings_df['team']]

    # Create horizontal bar chart
    fig = go.Figure(data=[
        go.Bar(
            x=standings_df['wins'],
            y=standings_df['team'],
            orientation='h',
            marker=dict(color=colors),
            text=standings_df.apply(
                lambda row: f"{row['wins']}-{row['losses']} ({row['gb']} GB)",
                axis=1
            ),
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>' +
                         'Wins: %{x}<br>' +
                         '<extra></extra>'
        )
    ])

    fig.update_layout(
        title=dict(
            text=f'{division_name} Standings',
            x=0.5,
            xanchor='center'
        ),
        xaxis=dict(title='Wins'),
        yaxis=dict(
            title='',
            autorange='reversed'  # Top team at top
        ),
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=False
    )

    logger.info(f"Created standings chart for {division_name}")
    return fig


def create_division_race_chart(
    team_data: Dict[str, pd.DataFrame],
    division_name: str,
    title: Optional[str] = None,
    figsize: tuple = (6, 3.5),
    dpi: int = 150,
    playing_teams: Optional[List[str]] = None,
    show_y_labels: bool = True
) -> mpl_fig.Figure:
    """
    Create division race chart showing games above .500 over the season.

    Uses Matplotlib for publication-quality print output with fine-grained control.

    Args:
        team_data: Dict mapping team abbr -> DataFrame with columns:
                   - 'game_number': Game number in season (1-162)
                   - 'wins': Cumulative wins after this game
                   - 'losses': Cumulative losses after this game
                   - 'games_above_500': Games above .500 (wins - losses)
        division_name: Division name (e.g., 'AL East')
        title: Optional custom title
        figsize: Figure size in inches (width, height)
        dpi: Resolution for rasterized output
        playing_teams: Optional list of team abbreviations that are playing in this game

    Returns:
        Matplotlib Figure

    Example:
        >>> team_data = {
        ...     'NYY': pd.DataFrame({
        ...         'game_number': [1, 2, 3],
        ...         'wins': [1, 2, 2],
        ...         'losses': [0, 0, 1],
        ...         'games_above_500': [1, 2, 1]
        ...     }),
        ...     'BOS': pd.DataFrame({
        ...         'game_number': [1, 2, 3],
        ...         'wins': [0, 1, 2],
        ...         'losses': [1, 1, 1],
        ...         'games_above_500': [-1, 0, 1]
        ...     })
        ... }
        >>> fig = create_division_race_chart(team_data, 'AL East')
    """
    # Create figure and axis
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)

    # Track final positions for annotations
    team_final_positions = {}

    # Find max games played across all teams
    max_games = max(df['game_number'].max() for df in team_data.values() if not df.empty)

    # B&W-friendly colors for non-playing teams
    bw_colors = ['#333333', '#666666', '#999999']
    bw_color_idx = 0

    # Determine if we have playing teams
    playing_teams = playing_teams or []

    # Plot each team's line
    for team, df in team_data.items():
        if df.empty:
            logger.warning(f"No data for team {team}")
            continue

        # Determine line style based on whether team is playing
        is_playing = team in playing_teams

        if is_playing:
            # Playing teams: use team color, thick line
            color = TEAM_COLORS.get(team, '#333333')
            linewidth = 3.5
            alpha = 1.0
            zorder = 5  # Higher z-order (on top)
        else:
            # Non-playing teams: grayscale, thin line
            color = bw_colors[bw_color_idx % len(bw_colors)]
            bw_color_idx += 1
            linewidth = 1.5
            alpha = 0.6
            zorder = 3

        # Plot line
        ax.plot(
            df['game_number'],
            df['games_above_500'],
            color=color,
            linewidth=linewidth,
            alpha=alpha,
            solid_capstyle='round',
            solid_joinstyle='round',
            zorder=zorder
        )

        # Store final position for annotation
        final_game = df.iloc[-1]
        team_final_positions[team] = {
            'games_above_500': final_game['games_above_500'],
            'color': color,
            'is_playing': is_playing
        }

    # Find the leader (team with most games above .500)
    leader_games_above = max(pos['games_above_500'] for pos in team_final_positions.values())

    # Count how many teams are tied for first
    teams_tied_for_first = sum(1 for pos in team_final_positions.values() if pos['games_above_500'] == leader_games_above)

    # Annotate end of each line with team code and games behind
    for team, pos in team_final_positions.items():
        games_above = pos['games_above_500']
        games_behind = leader_games_above - games_above
        is_playing = pos['is_playing']

        # Format annotation
        if games_behind == 0:
            # Tied for first or leader
            if teams_tied_for_first > 1:
                annotation = f"{team} (tied)"
            else:
                annotation = team
        else:
            # Show team code and games behind
            annotation = f"{team} (-{games_behind:.0f})"

        # Add annotation at end of line (make playing teams more prominent)
        ax.annotate(
            annotation,
            xy=(max_games, games_above),
            xytext=(5, 0),
            textcoords='offset points',
            fontsize=10 if is_playing else 8,
            fontweight='bold' if is_playing else 'normal',
            color=pos['color'],
            verticalalignment='center',
            family='Crimson Text'
        )

    # Add horizontal line at .500
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.5, zorder=1)

    # Add horizontal gridlines at intervals of 10
    y_min, y_max = ax.get_ylim()
    y_grid_start = int(y_min // 10) * 10
    y_grid_end = int(y_max // 10) * 10 + 10
    for y in range(y_grid_start, y_grid_end + 1, 10):
        if y != 0:  # Skip 0 since we have the .500 line
            ax.axhline(y=y, color='#CCCCCC', linestyle=':', linewidth=0.8, alpha=0.5, zorder=1)

    # Add vertical lines for month boundaries (approximate game numbers)
    # Typical MLB season: April (1-25), May (26-56), June (57-86),
    # July (87-113), August (114-144), September (145-162)
    month_boundaries = {
        'May': 26,
        'June': 57,
        'July': 87,
        'Aug': 114,
        'Sep': 145
    }

    for month, game_num in month_boundaries.items():
        if game_num <= max_games:
            ax.axvline(x=game_num, color='#CCCCCC', linestyle=':', linewidth=0.8, alpha=0.5, zorder=1)
            # Add month label at top
            ax.text(game_num, ax.get_ylim()[1], month,
                    fontsize=8, color='gray', alpha=0.7,
                    horizontalalignment='left', verticalalignment='bottom',
                    rotation=0, family='Crimson Text')

    # No axis labels (title is in template)

    # Style tick labels with JetBrains Mono (monospace for numbers)
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_fontfamily('JetBrains Mono')
        label.set_fontsize(8)

    # Hide y-axis labels and ticks if requested (to save space)
    if not show_y_labels:
        ax.set_yticklabels([])
        ax.tick_params(axis='y', length=0)

    # Set axis limits (extend x to make room for annotations)
    ax.set_xlim(0, max_games + 15)

    # Disable default grid (we added custom gridlines)
    ax.grid(False)
    ax.set_axisbelow(True)  # Grid behind data

    # Spine styling (remove all spines for clean appearance)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_visible(False)

    # No legend (annotations replace it)

    # Tight layout
    fig.tight_layout()

    logger.info(f"Created Matplotlib division race chart for {division_name} with {len(team_data)} teams")
    return fig


def create_wild_card_race_chart(
    teams_df: pd.DataFrame,
    league: str = 'AL'
) -> go.Figure:
    """
    Create wild card race visualization.

    Args:
        teams_df: DataFrame with columns ['team', 'wins', 'losses', 'wc_gb']
        league: 'AL' or 'NL'

    Returns:
        Plotly Figure

    Shows teams in wild card contention with games back from WC position.
    """
    # Sort by wild card games behind
    teams_df = teams_df.sort_values('wc_gb')

    # Take top 10 teams in contention
    teams_df = teams_df.head(10)

    colors = [TEAM_COLORS.get(team, '#333333') for team in teams_df['team']]

    fig = go.Figure(data=[
        go.Bar(
            y=teams_df['team'],
            x=teams_df['wc_gb'],
            orientation='h',
            marker=dict(color=colors),
            text=teams_df['wc_gb'].apply(lambda x: f'{x:.1f} GB' if x > 0 else 'In'),
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>' +
                         'WC GB: %{x:.1f}<br>' +
                         '<extra></extra>'
        )
    ])

    fig.update_layout(
        title=f'{league} Wild Card Race',
        xaxis=dict(
            title='Games Behind Wild Card',
            autorange='reversed'  # Closer teams on left
        ),
        yaxis=dict(
            title='',
            autorange='reversed'
        ),
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=False
    )

    logger.info(f"Created wild card race chart for {league}")
    return fig


def create_re24_chart(
    player_data: Dict[str, pd.DataFrame],
    team_name: str,
    title: Optional[str] = None,
    figsize: tuple = (7.5, 4),
    dpi: int = 100,
    highlight_players: Optional[List[str]] = None,
    show_y_labels: bool = True
) -> mpl_fig.Figure:
    """
    Create cumulative RE24 chart showing player value over the season.

    Args:
        player_data: Dict mapping player name -> DataFrame with columns:
                     - 'game_date': Date of the game (YYYY-MM-DD)
                     - 'cumulative_re24': Cumulative RE24 after this game
        team_name: Team name for title
        title: Optional custom title
        figsize: Figure size in inches (width, height)
        dpi: Resolution for rasterized output
        highlight_players: Optional list of player names to highlight

    Returns:
        Matplotlib Figure
    """
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)

    # Track final positions for annotations
    player_final_positions = {}

    # Collect all unique dates across all players to create a common x-axis
    all_dates = set()
    for df in player_data.values():
        if not df.empty and 'game_date' in df.columns:
            all_dates.update(df['game_date'].tolist())

    # Sort dates and create a mapping to team game number
    sorted_dates = sorted(all_dates)
    date_to_game_num = {date: i + 1 for i, date in enumerate(sorted_dates)}
    max_games = len(sorted_dates)

    # Color palette for players (distinct colors)
    player_colors = [
        '#1f77b4',  # blue
        '#ff7f0e',  # orange
        '#2ca02c',  # green
        '#d62728',  # red
        '#9467bd',  # purple
        '#8c564b',  # brown
        '#e377c2',  # pink
        '#7f7f7f',  # gray
        '#bcbd22',  # olive
        '#17becf',  # cyan
    ]

    highlight_players = highlight_players or []

    # Sort players by final RE24 to assign colors consistently
    sorted_players = sorted(
        player_data.items(),
        key=lambda x: x[1]['cumulative_re24'].iloc[-1] if not x[1].empty else 0,
        reverse=True
    )

    for idx, (player, df) in enumerate(sorted_players):
        if df.empty:
            continue

        is_highlighted = player in highlight_players
        color = player_colors[idx % len(player_colors)]

        # Highlighted players get thicker lines
        if is_highlighted:
            linewidth = 2.5
            alpha = 1.0
            zorder = 5
        else:
            linewidth = 1.5
            alpha = 0.7
            zorder = 3

        # Map game_date to team game number for proper x-axis alignment
        x_values = df['game_date'].map(date_to_game_num)

        ax.plot(
            x_values,
            df['cumulative_re24'],
            color=color,
            linewidth=linewidth,
            alpha=alpha,
            solid_capstyle='round',
            solid_joinstyle='round',
            zorder=zorder
        )

        # Store final position (use the last game's date mapped to game number)
        final_game = df.iloc[-1]
        final_x = date_to_game_num.get(final_game['game_date'], max_games)
        player_final_positions[player] = {
            'cumulative_re24': final_game['cumulative_re24'],
            'final_x': final_x,
            'color': color,
            'is_highlighted': is_highlighted
        }

    # Annotate end of each line with player name and RE24
    # Sort by RE24 to place labels from top to bottom
    sorted_positions = sorted(
        player_final_positions.items(),
        key=lambda x: x[1]['cumulative_re24'],
        reverse=True
    )

    # Calculate y positions to avoid overlap
    y_range = ax.get_ylim()[1] - ax.get_ylim()[0]
    min_spacing = y_range * 0.035  # Minimum vertical space between labels

    placed_positions = []
    for player, pos in sorted_positions:
        re24 = pos['cumulative_re24']
        is_highlighted = pos['is_highlighted']

        sign = '+' if re24 >= 0 else ''
        annotation = f"{player} {sign}{re24:.0f}"

        # Find y position that doesn't overlap
        label_y = re24
        for placed_y in placed_positions:
            if abs(label_y - placed_y) < min_spacing:
                # Nudge down if too close to a higher label
                label_y = placed_y - min_spacing

        placed_positions.append(label_y)

        ax.annotate(
            annotation,
            xy=(max_games, re24),  # Point at end of line
            xytext=(8, 0),  # Offset to the right in points
            textcoords='offset points',
            fontsize=9 if is_highlighted else 8,
            fontweight='bold' if is_highlighted else 'normal',
            color=pos['color'],
            verticalalignment='center',
            ha='left',
            family='Crimson Text',
            annotation_clip=False
        )

    # Add horizontal line at 0
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.5, zorder=1)

    # Add horizontal gridlines at intervals of 20
    y_min, y_max = ax.get_ylim()
    y_grid_start = int(y_min // 20) * 20
    y_grid_end = int(y_max // 20) * 20 + 20
    for y in range(y_grid_start, y_grid_end + 1, 20):
        if y != 0:
            ax.axhline(y=y, color='#CCCCCC', linestyle=':', linewidth=0.8, alpha=0.5, zorder=1)

    # Add vertical lines for month boundaries
    month_boundaries = {
        'May': 26,
        'June': 57,
        'July': 87,
        'Aug': 114,
        'Sep': 145
    }

    for month, game_num in month_boundaries.items():
        if game_num <= max_games:
            ax.axvline(x=game_num, color='#CCCCCC', linestyle=':', linewidth=0.8, alpha=0.5, zorder=1)
            ax.text(game_num, ax.get_ylim()[1], month,
                    fontsize=8, color='gray', alpha=0.7,
                    horizontalalignment='left', verticalalignment='bottom',
                    family='Crimson Text')

    # Style tick labels
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_fontfamily('JetBrains Mono')
        label.set_fontsize(8)

    if not show_y_labels:
        ax.set_yticklabels([])
        ax.tick_params(axis='y', length=0)

    # Set axis limits
    ax.set_xlim(0, max_games + 25)  # Extra room for player names

    ax.grid(False)
    ax.set_axisbelow(True)

    # Remove spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_visible(False)

    fig.tight_layout()

    logger.info(f"Created RE24 chart for {team_name} with {len(player_data)} players")
    return fig
