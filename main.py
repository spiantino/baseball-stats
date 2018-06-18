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



def get_past_game_dates(team, n=10):
    """
    Find dates for last 10 games
    !!! move to dbcontroller
    """
    data = dbc.get_team(team)['Schedule']
    df = pd.DataFrame(data)
    df = df.loc[df['']=='boxscore']

    year = datetime.date.today().strftime('%Y')
    def format_date(x, y):
        _, m, d = x.split()[:3]
        df_date = '{} {} {}'.format(m, d, y)
        return datetime.datetime.strptime(df_date, '%b %d %Y')

    df['Dates'] = df.Date.apply(lambda x: str(format_date(x, y=year).date()))
    dates = sorted(df.Dates.values, reverse=True)[:n]
    return dates


def pitcher_history(team):
    """
    A Teams pitcher history
    for previous 10 games
    """
    dates = get_past_game_dates(team)
    games = dbc.get_team_game_previews(team, dates)

    cols = ['Date', 'Opponent', 'Pitcher', 'IP', 'Hits', 'Runs',
            'ER', 'Walks', 'Strikeouts', 'Home Runs', 'GSc']
    df_data = []
    for game in games:
        away, home = game['away'], game['home']
        side = 'home' if team==home else 'away'
        opp_side = set({'home', 'away'} - {side}).pop()

        date = game['date']
        date_data = datetime.datetime.strptime(date, "%Y-%m-%d")
        short_date = "{d.month}/{d.day} {dow}".format(d=date_data, dow=date_data.strftime("%a"))
        opp = game[opp_side]

        data = game['preview'][0]['liveData']['boxscore']['teams'][side]

        pit_id = 'ID' + data['pitchers'][0]
        pit_obj = data['players'][pit_id]

        name = ' '.join((pit_obj['name']['first'],
                         pit_obj['name']['last']))

        pit_data = pit_obj['gameStats']['pitching']

        ip    = pit_data['inningsPitched']
        er    = pit_data['earnedRuns']
        hr    = pit_data['homeRuns']
        hits  = pit_data['hits']
        runs  = pit_data['runs']
        walks = pit_data['baseOnBalls']
        strko = pit_data['strikeOuts']

        # Find GSc from BR data
        decoded = unidecode(name)
        pitchers = list(dbc.get_pitchers_by_game(team, date))
        all_pits = pitchers[0][team]['pitching']
        try:
            pit_stats = [x for x in all_pits if decoded in x.values()][0]
            gsc = pit_stats['GSc']
        except:
            gsc = '-'

        df_data.append([short_date, opp, name, ip, hits, runs,
                        er, walks, strko, hr, gsc])
    df = pd.DataFrame(df_data, columns=cols)
    df = df.sort_values(by='Date', ascending=False)
    return df


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


def elo():
    # League ELO
    elo_cols = ['Rating', 'Team', 'Division%',
                'Playoff%', 'WorldSeries%']
    elo_stats = dbc.get_elo_stats()
    elo_data = combine_dicts_in_list(elo_stats)
    df = pd.DataFrame(elo_data)
    df = df[elo_cols].sort_values(by='Rating', ascending=False)

    # Round rating column
    df['Rating'] = df.Rating.round().astype(int)

    # Add Rank column and make it first
    df['Rank'] = df.Rating.rank(ascending=False)
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]

    # Format columns with percantages
    perc_cols = ['Division%', 'Playoff%', 'WorldSeries%']
    for col in perc_cols:
        df[col] = df[col].apply(lambda x: "{:.1%}".format(round(x, 3)))

    return df

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


def previous_week_bullpen(team):
    """
    For each day in the week:
    Inning entered the game, number of pitches thrown,
    WPA, warmed up (if this exists anywhere)
    """
    today = datetime.date.today()
    weekday = today.weekday()

    start = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start
    end_of_week = start_of_week + datetime.timedelta(6)
    date_range = pd.date_range(start=start_of_week, end=end_of_week)

    df_data = {}
    for dt_date in date_range:
        date = dt_date.strftime('%Y-%m-%d')
        try:
            game = dbc.get_team_game_preview(team, date)
            game_data = list(game)[0]
            side = 'home' if game_data['home'] == team else 'away'
            team_data = game_data['preview'][0]['liveData']\
                                 ['boxscore']['teams'][side]
            players = team_data['players']
            bullpen = team_data['bullpen']
            pitch_data = game_data[team]['pitching'][1:]

            pitchers_data = []
            inning_idx = 0
            for player_stats in pitch_data:
                name = player_stats['Pitching']
                wpa  = float(player_stats['WPA'])
                pits = int(player_stats['Pit'])
                ip = float(player_stats['IP'])

                # Infer inning that pitcher entered the game
                inning_idx += math.floor(ip)
                remainder = round((ip % 1) * 10)
                if remainder > 0:
                    inning_idx += Fraction(remainder, 3)
                entered = math.floor((inning_idx - ip) + 1)

                if entered > 1:
                    player_data = [name, entered, pits, wpa]
                    data_str = '{} - Entered: {} Pitch count: {} WPA: {}'.format(name, entered, pits, wpa)
                    pitchers_data.append(data_str)

            #Find the rest of the bullpen that did not play
            for player_id in bullpen:
                pid = 'ID' + player_id
                name = ' '.join((players[pid]['name']['first'],
                                 players[pid]['name']['last']))
                player_data = name
                pitchers_data.append(player_data)

            df_data[date] = pitchers_data

        except: # When there was no game on that day
            continue

    # Add padding so array lengths match
    max_ = max([len(df_data[k]) for k in df_data.keys()])
    for k in df_data.keys():
        pad_len = max_ - len(df_data[k])
        padding = [None for _ in range(pad_len)]
        df_data[k].extend(padding)

    df = pd.DataFrame(df_data)

    return df


def series_results(team):
    """
    Table with date, time, score, starter,
    ip, game score, for the current series
    """
    today = datetime.date.today().strftime('%Y-%m-%d')
    opponent = None
    df_data = []
    cols = ['date', 'time', 'home', 'away', 'score',
            'home starter', 'home ip', 'home gs',
            'away starter', 'away ip', 'away gs']
    all_game_dates = dbc.get_past_game_dates_by_team(team)

    double_header = False
    for date in all_game_dates:
        game = dbc.get_team_game_preview(team, date)
        games = list(game)
        if len(games) > 1:
            if double_header:
                game_data = games[0]
            else:
                game_data = games[1]
                double_header = True
        else:
            game_data = games[0]
        preview = game_data['preview'][0]
        state = preview['gameData']['status']['detailedState']
        if date == today and state != 'Final':
            continue
        home = game_data['home']
        away = game_data['away']

        # Stop collecting data when opponent changes
        this_opponent = away if team == home else home
        if not opponent:
            opponent = this_opponent
        elif opponent != this_opponent:
            break

        game_time = preview['gameData']['datetime']['time']
        am_or_pm = preview['gameData']['datetime']['ampm']
        time = '{}{}'.format(game_time, am_or_pm)

        h_score = preview['liveData']['linescore']['home']['runs']
        a_score = preview['liveData']['linescore']['away']['runs']

        score = '{}-{}'.format(h_score, a_score)

        try:
            home_pit_data = game_data[home]['pitching'][1]
            away_pit_data = game_data[away]['pitching'][1]
        except:
            print("Need boxscores for {}".format(date))
            continue

        home_starter = home_pit_data['Pitching']
        away_starter = away_pit_data['Pitching']

        home_ip = home_pit_data['IP']
        away_ip = away_pit_data['IP']

        home_gsc = home_pit_data['GSc']
        away_gsc = away_pit_data['GSc']

        df_data.append([date, time, home, away, score, home_starter, home_ip,
                        home_gsc, away_starter, away_ip, away_gsc])

    df = pd.DataFrame(df_data, columns=cols)
    return df

def extract_game_data(cursorobj):
    """
    Returns game data from a cursor object
    Determines which game to return when a double header occurs
    """
    games = list(cursorobj)
    if len(games) > 1:
        status = games[0]['preview'][0]['gameData']['status']['detailedState']
        if status == 'Final':
            return games[1]
        else:
            return games[0]

    elif len(games) == 1:
        return games[0]

    else:
        return None

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
    g.query_game_preview_by_date(team='NYY', date='2018-06-15')
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
