import argparse
import datetime
import pandas as pd
from collections import defaultdict, Counter

import scrape
from latex import Latex
from tablebuilder import TableBuilder
from dbclasses import Player, Game, Team


def get_pitch_counts(player):
    """
    Return counts of all pitch types for input player
    """
    p = Player(name=player)
    team = p.get_stat('Team')

    g = Game()
    dates = g.find_pitch_dates(player, team)

    counts =  Counter()

    last_date = None
    for date in dates:

        # Check if game is a double header
        idx = 1 if date == last_date else 0

        g = Game()
        g.query_game_preview_by_date(team, date, idx)
        g.parse_pitch_types()
        game_counts = g._pit_counts[player]
        counts += game_counts

        last_date = date

    return counts


def scrape_update(home, away, year):
    print("Gathering game previews...")
    scrape.game_previews()

    print("Scraping past boxscores...")
    scrape.boxscores(date='all')

    print("Scraping batting leaderboard...")
    scrape.fangraphs(state='bat', year=year)

    print("Scraping pitching leaderboard...")
    scrape.fangraphs(state='pit', year=year)

    print("Scraping league standings...")
    scrape.standings()

    print("Scraping schedule, roster, pitch logs, injuries, transactions...")
    for team in [home, away]:
        scrape.schedule(team)
        scrape.pitching_logs(team, year)
        scrape.current_injuries(team)
        scrape.transactions(team, year)
        scrape.forty_man(team, year)

    scrape.league_elo()


if __name__ == '__main__':
    today = datetime.date.today().strftime('%Y-%m-%d')
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--team', default='NYY')
    parser.add_argument('-d', '--date', default=today)
    args = parser.parse_args()

    g = Game()
    g.query_game_preview_by_date(team=args.team, date='2018-06-19')
    g.parse_all()

    team = g._team
    opp  = g._opp

    year = g._date.split('-')[0]
    scrape_update(g._team, g._opp, year)

    tb=TableBuilder(g)

    summary = tb.summary_info()
    pitchers = tb.starting_pitchers()
    starters, bench = tb.rosters()
    bullpen = tb.bullpen()
    standings = tb.standings()
    history = tb.game_history()
    bat_df = tb.bat_leaders(stat='WAR', n=30)
    hr_df  = tb.bat_leaders(stat='HR',  n=10)
    rbi_df = tb.bat_leaders(stat='RBI', n=10)
    pit_df = tb.pit_leaders(stat='WAR', n=30, role='starter')
    era_df = tb.pit_leaders(stat='ERA', n=10, role='starter')
    rel_df = tb.pit_leaders(stat='WAR', n=10, role='reliever')
    elo_df = tb.elo()
    pit_hist = tb.pitcher_history()
    last_week_bp = tb.previous_week_bullpen()
    series_table = tb.series_results()
    gb = tb.games_behind(team, opp)

    print(summary)
    print(pitchers)
    print(starters)
    print(bench)
    print(bullpen)
    print(standings)
    print(history)
    print(bat_df)
    print(hr_df)
    print(rbi_df)
    print(pit_df)
    print(era_df)
    print(rel_df)
    print(elo_df)
    print(pit_hist)
    print(last_week_bp)
    print(series_table)
    print(gb)
