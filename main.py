import argparse
import datetime
import requests
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as dates
import scrape

from collections import defaultdict, Counter
from unidecode import unidecode
from dbcontroller import DBController
from utils import combine_dicts_in_list, subtract_dates, get_stadium_location
from latex import Latex

import math
from fractions import Fraction

from tablebuilder import TableBuilder
from dbclasses import Player, Game, Team

def games_behind_data(home, away):
    """
    Get Games Behind data form a team's
    Schedule array in the Teams collection
    """
    gbh = dbc.get_games_behind_history(home)
    gba = dbc.get_games_behind_history(away)

    gbh_data = combine_dicts_in_list(gbh)
    gba_data = combine_dicts_in_list(gba)

    gbh_df = pd.DataFrame(gbh_data)
    gba_df = pd.DataFrame(gba_data)

    def find_date(x):
        months = {'Jan' : 'Janurary',
                  'Feb' : 'February',
                  'Mar' : 'March',
                  'Apr' : 'April',
                  'May' : 'May',
                  'Jun' : 'June',
                  'Jul' : 'July',
                  'Aug' : 'August',
                  'Sep' : 'September',
                  'Oct' : 'October',
                  'Nov' : 'November',
                  'Dec' : 'December'}
        m = months[x.split()[1]].strip()
        d = x.split()[2].strip()
        y = datetime.date.today().strftime('%Y')
        datestr = '{} {} {}'.format(m,d,y)
        return datetime.datetime.strptime(datestr, '%B %d %Y')

    gbh_df['Date'] = gbh_df.Date.apply(lambda x: find_date(x))
    gba_df['Date'] = gba_df.Date.apply(lambda x: find_date(x))

    # Drop duplicate rows (double headers)
    gbh_df = gbh_df.drop_duplicates()
    gba_df = gba_df.drop_duplicates()

    def fill_missing_dates(df):
        df = df.set_index('Date')
        start, end = df.index[0], df.index[-1]
        idx = pd.date_range(start, end)
        df = df.reindex(idx, fill_value=None)
        df = df.fillna(method='ffill')
        df = df.reset_index().rename(columns={'index' : 'Date'})
        return df

    dfh = fill_missing_dates(gbh_df)
    dfa = fill_missing_dates(gba_df)

    # Normalize games behind column
    def norm_gb(x):
        return 0 if x.split()[0] in ['up', 'Tied'] else x

    dfh['GB'] = dfh.GB.apply(lambda x: norm_gb(x))
    dfa['GB'] = dfa.GB.apply(lambda x: norm_gb(x))

    # Make plots
    h_plot = plt.plot(dfh['Date'], dfh['GB'])
    a_plot = plt.plot(dfa['Date'], dfa['GB'])
    return [h_plot, a_plot]

### Pitch count functions - move to a new file?
def parse_pitch_types(game_data):
    """
    Gather counts of pitch types for
    all pitchers in a single game
    """
    live_data = game_data['preview'][0]['liveData']
    player_data = live_data['players']['allPlayers']
    all_plays = live_data['plays']['allPlays']
    pit_data = defaultdict(list)
    for play in all_plays:
        pitcher_id = 'ID' + play['matchup']['pitcher']
        pitcher_raw = ' '.join((player_data[pitcher_id]['name']['first'],
                                player_data[pitcher_id]['name']['last']))
        pitcher = unidecode(pitcher_raw)
        for event in play['playEvents']:
            try:
                pitch = event['details']['type']
                pit_data[pitcher].append(pitch)
            except:
                continue
    pit_counts = {k : Counter(v) for k,v in pit_data.items()}
    return pit_counts


def get_pitch_counts(player):
    """
    Return counts of all pitch types for input player
    """
    dates = dbc.get_all_pitch_dates(player)
    team = dbc.get_player_team(player)
    counts = Counter()
    for date in dates:
        game = list(dbc.get_team_game_preview(team, date))[0]
        game_counts = parse_pitch_types(game)[player]
        counts += game_counts
    return counts



def scrape_update(home, away, year):
    print("Gathering game previews...")
    scrape.game_previews()

    print("Scraping past boxscores...")
    scrape.boxscores(date='all')

    print("Scraping batting leaderboard...")
    scrape.fangraphs(state='bat', year=current_year)

    print("Scraping pitching leaderboard...")
    scrape.fangraphs(state='pit', year=current_year)

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
    g = Game()
    g.query_game_preview_by_date(team='NYY', date='2018-06-19')
    g.parse_all()
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
