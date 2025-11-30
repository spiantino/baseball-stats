"""
Games Behind (GB) trend chart for division standings.

Visualizes how teams' positions in the division race change over time.
Uses Matplotlib for publication-quality print output.
"""

from typing import List, Dict, Optional
import pandas as pd

# Set non-interactive backend to prevent macOS dock icon
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.figure as mpl_fig
from matplotlib.patches import Rectangle
import plotly.graph_objects as go  # For legacy functions (to be converted)

from config.logging_config import get_logger

logger = get_logger(__name__)


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
    dpi: int = 150
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

    # Plot each team's line
    for team, df in team_data.items():
        if df.empty:
            logger.warning(f"No data for team {team}")
            continue

        # Get team color
        color = TEAM_COLORS.get(team, '#333333')

        # Plot line
        ax.plot(
            df['game_number'],
            df['games_above_500'],
            color=color,
            linewidth=2.5,
            solid_capstyle='round',
            solid_joinstyle='round',
            zorder=3
        )

        # Store final position for annotation
        final_game = df.iloc[-1]
        team_final_positions[team] = {
            'games_above_500': final_game['games_above_500'],
            'color': color
        }

    # Find the leader (team with most games above .500)
    leader_games_above = max(pos['games_above_500'] for pos in team_final_positions.values())

    # Annotate end of each line with team code and games behind
    for team, pos in team_final_positions.items():
        games_above = pos['games_above_500']
        games_behind = leader_games_above - games_above

        # Format annotation
        if games_behind == 0:
            # Leader - just show team code
            annotation = team
        else:
            # Show team code and games behind
            annotation = f"{team} (-{games_behind:.0f})"

        # Add annotation at end of line
        ax.annotate(
            annotation,
            xy=(162, games_above),
            xytext=(5, 0),
            textcoords='offset points',
            fontsize=9,
            fontweight='bold',
            color=pos['color'],
            verticalalignment='center'
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
        'August': 114,
        'September': 145
    }

    for month, game_num in month_boundaries.items():
        ax.axvline(x=game_num, color='#CCCCCC', linestyle=':', linewidth=0.8, alpha=0.5, zorder=1)
        # Add month label at top
        ax.text(game_num, ax.get_ylim()[1], month,
                fontsize=8, color='gray', alpha=0.7,
                horizontalalignment='left', verticalalignment='bottom',
                rotation=0)

    # No title or axis labels (will be added as section title in template)

    # Set axis limits (extend x to make room for annotations)
    ax.set_xlim(0, 175)

    # Disable default grid (we added custom gridlines)
    ax.grid(False)
    ax.set_axisbelow(True)  # Grid behind data

    # Spine styling (remove top and right)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(1)
    ax.spines['bottom'].set_linewidth(1)

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
