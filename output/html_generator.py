"""
HTML dashboard generator using Jinja2 templates.

Creates interactive HTML dashboards with Plotly charts.
"""

from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime
import logging

from jinja2 import Environment, FileSystemLoader
import plotly.graph_objects as go

from config.logging_config import get_logger

logger = get_logger(__name__)


class HTMLGenerator:
    """
    Generate interactive HTML dashboards with Plotly charts.

    Uses same Jinja2 templates as PDF generator but with interactive charts.
    """

    def __init__(
        self,
        template_dir: Optional[str] = None,
        output_dir: Optional[str] = None
    ):
        """
        Initialize HTML generator.

        Args:
            template_dir: Directory containing Jinja2 templates
            output_dir: Directory for HTML output
        """
        # Set up template directory
        if template_dir is None:
            template_dir = Path(__file__).parent.parent / 'templates'
        self.template_dir = Path(template_dir)

        # Set up output directory
        if output_dir is None:
            output_dir = Path(__file__).parent.parent / 'output' / 'html'
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=True,
            trim_blocks=True,
            lstrip_blocks=True
        )

        # Initialize chart renderer
        # Charts are now passed as pre-rendered SVG strings

        logger.info(f"HTMLGenerator initialized with templates from {self.template_dir}")

    def generate_game_preview_html(
        self,
        data: Dict[str, Any],
        charts: Optional[Dict[str, go.Figure]] = None,
        output_filename: Optional[str] = None
    ) -> Path:
        """
        Generate interactive game preview HTML.

        Args:
            data: Template context data (teams, pitchers, lineups, etc.)
            charts: Dict mapping chart_id -> Plotly Figure
            output_filename: Output filename (auto-generated if None)

        Returns:
            Path to generated HTML

        Example:
            >>> data = {
            ...     'away_team': 'NYY',
            ...     'home_team': 'BOS',
            ...     'game_date': '2024-06-15',
            ...     'away_pitcher': {...},
            ...     'home_pitcher': {...}
            ... }
            >>> charts = {
            ...     'pitcher_comparison': fig1,
            ...     'war_spider': fig2
            ... }
            >>> html_path = generator.generate_game_preview_html(data, charts)
        """
        # Add generation metadata
        data['output_format'] = 'html'
        data['generation_date'] = datetime.now().strftime('%Y-%m-%d %H:%M')

        # Add charts (already rendered as SVG strings)
        if charts:
            data.update(charts)

        # Render template to HTML
        html_content = self._render_template('game_preview.html', data)

        # Generate output filename if not provided
        if output_filename is None:
            away = data.get('away_team', 'AWAY')
            home = data.get('home_team', 'HOME')
            date = data.get('game_date', datetime.now().strftime('%Y%m%d'))
            output_filename = f'game_preview_{away}_at_{home}_{date}.html'

        # Save HTML
        output_path = self.output_dir / output_filename
        self._save_html(html_content, output_path)

        logger.info(f"Generated game preview HTML: {output_path}")
        return output_path

    def generate_dashboard(
        self,
        data: Dict[str, Any],
        charts: Optional[Dict[str, go.Figure]] = None,
        output_filename: str = 'dashboard.html'
    ) -> Path:
        """
        Generate interactive dashboard HTML.

        Args:
            data: Dashboard data
            charts: Plotly charts
            output_filename: Output filename

        Returns:
            Path to generated HTML
        """
        # Add generation metadata
        data['output_format'] = 'html'
        data['generation_date'] = datetime.now().strftime('%Y-%m-%d %H:%M')

        # Add charts (already rendered as SVG strings)
        if charts:
            data.update(charts)

        # Render template
        template_name = data.get('template', 'game_preview.html')
        html_content = self._render_template(template_name, data)

        # Save HTML
        output_path = self.output_dir / output_filename
        self._save_html(html_content, output_path)

        logger.info(f"Generated dashboard HTML: {output_path}")
        return output_path

    def generate_index_page(
        self,
        games: List[Dict[str, Any]],
        output_filename: str = 'index.html'
    ) -> Path:
        """
        Generate index page listing all game previews.

        Args:
            games: List of game data dicts
            output_filename: Output filename

        Returns:
            Path to index HTML

        Example:
            >>> games = [
            ...     {'away_team': 'NYY', 'home_team': 'BOS', 'game_date': '2024-06-15'},
            ...     {'away_team': 'LAD', 'home_team': 'SF', 'game_date': '2024-06-15'}
            ... ]
            >>> index_path = generator.generate_index_page(games)
        """
        data = {
            'output_format': 'html',
            'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'games': games,
            'title': 'Game Previews'
        }

        # Create simple index template inline
        index_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .header {
            background-color: #003087;
            color: white;
            padding: 30px;
            text-align: center;
            border-radius: 8px;
            margin-bottom: 30px;
        }
        .game-list {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 20px;
        }
        .game-card {
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            transition: transform 0.2s, box-shadow 0.2s;
        }
        .game-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }
        .game-card a {
            text-decoration: none;
            color: inherit;
        }
        .matchup {
            font-size: 18pt;
            font-weight: bold;
            color: #003087;
            margin-bottom: 10px;
        }
        .date {
            color: #666;
            font-size: 11pt;
        }
        .teams {
            margin: 15px 0;
        }
        .team {
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #e5e5e5;
        }
        .footer {
            text-align: center;
            padding: 20px;
            margin-top: 40px;
            color: #666;
            font-size: 10pt;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>{{ title }}</h1>
        <p>Generated {{ generation_date }}</p>
    </div>

    <div class="game-list">
        {% for game in games %}
        <div class="game-card">
            <a href="game_preview_{{ game.away_team }}_at_{{ game.home_team }}_{{ game.game_date }}.html">
                <div class="matchup">{{ game.away_team }} @ {{ game.home_team }}</div>
                <div class="date">{{ game.game_date }}</div>
                <div class="teams">
                    <div class="team">
                        <span>{{ game.away_team }}</span>
                        <span>{{ game.get('away_record', 'N/A') }}</span>
                    </div>
                    <div class="team">
                        <span>{{ game.home_team }}</span>
                        <span>{{ game.get('home_record', 'N/A') }}</span>
                    </div>
                </div>
            </a>
        </div>
        {% endfor %}
    </div>

    <div class="footer">
        <p>Data sources: MLB Stats API, FanGraphs, Baseball Reference</p>
    </div>
</body>
</html>
        """

        # Render template
        template = self.jinja_env.from_string(index_template)
        html_content = template.render(**data)

        # Save HTML
        output_path = self.output_dir / output_filename
        self._save_html(html_content, output_path)

        logger.info(f"Generated index page with {len(games)} games: {output_path}")
        return output_path

    def _render_template(self, template_name: str, data: Dict[str, Any]) -> str:
        """
        Render Jinja2 template to HTML string.

        Args:
            template_name: Name of template file
            data: Template context data

        Returns:
            Rendered HTML string
        """
        try:
            template = self.jinja_env.get_template(template_name)
            html = template.render(**data)
            logger.debug(f"Rendered template: {template_name}")
            return html

        except Exception as e:
            logger.error(f"Failed to render template {template_name}: {e}", exc_info=True)
            raise

    def _save_html(self, html_content: str, output_path: Path) -> None:
        """
        Save HTML content to file.

        Args:
            html_content: HTML string
            output_path: Path for HTML output
        """
        try:
            output_path.write_text(html_content, encoding='utf-8')
            logger.debug(f"Saved HTML to: {output_path}")

        except Exception as e:
            logger.error(f"Failed to save HTML: {e}", exc_info=True)
            raise

    def get_available_templates(self) -> List[str]:
        """
        Get list of available template files.

        Returns:
            List of template filenames
        """
        templates = []
        for path in self.template_dir.rglob('*.html'):
            rel_path = path.relative_to(self.template_dir)
            templates.append(str(rel_path))

        return sorted(templates)


def create_html_from_game_data(
    game_data: Dict[str, Any],
    charts: Optional[Dict[str, go.Figure]] = None,
    output_filename: Optional[str] = None
) -> Path:
    """
    Convenience function to generate game preview HTML.

    Args:
        game_data: Game preview data
        charts: Optional Plotly charts
        output_filename: Output filename

    Returns:
        Path to generated HTML

    Example:
        >>> from output.html_generator import create_html_from_game_data
        >>> html_path = create_html_from_game_data(game_data, charts)
        >>> print(f"HTML saved to: {html_path}")
    """
    generator = HTMLGenerator()
    return generator.generate_game_preview_html(game_data, charts, output_filename)
