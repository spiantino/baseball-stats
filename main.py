import argparse
import datetime
import pandas as pd
from collections import defaultdict, Counter

import scrape
import latex
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

def scrape_games():
    print("Gathering game previews...")
    scrape.game_previews()

    print("Scraping past boxscores...")
    scrape.boxscores(date='all')


def scrape_update(home, away, year):
    print("Scraping batting leaderboard...")
    scrape.fangraphs(state='bat', year=year)

    print("Scraping pitching leaderboard...")
    scrape.fangraphs(state='pit', year=year)
    scrape.fangraph_splits(year=year)

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

def run(team, date=None, scrape=False):
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    date = today if not date else date

    if scrape:
        scrape_games()

    g = Game()
    g.query_game_preview_by_date(team=team, date=date)
    g.parse_all()

    if g._game:
        opp  = g._opp

        home = g._game['home']
        away = g._game['away']
        year = g._date.split('-')[0]

        scrape_update(home, away, year)

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

        # print(summary)
        # print(pitchers)
        # print(starters)
        # print(bench)
        # print(bullpen)
        # print(standings)
        # print(history)
        # print(bat_df)
        # print(hr_df)
        # print(rbi_df)
        # print(pit_df)
        # print(era_df)
        # print(rel_df)
        # print(elo_df)
        # print(pit_hist)
        # print(last_week_bp)
        # print(series_table)
        # print(gb)

        latex.make_pdf(team, date, home, away,
                       summary, pitchers, starters,
                       bench, bullpen, standings,
                       history, bat_df, hr_df, rbi_df,
                       pit_df, era_df, rel_df, elo_df,
                       pit_hist, last_week_bp, series_table, gb)


if __name__ == '__main__':
    today = datetime.date.today().strftime('%Y-%m-%d')
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--team', default='NYY')
    parser.add_argument('-d', '--date', default=today)
    parser.add_argument('-s', '--scrape', default=True)
    args = parser.parse_args()
    run(team=args.team, date=args.date, scrape=args.scrape)
