import argparse
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as dates
import scrape

from collections import defaultdict, Counter
from unidecode import unidecode
from dbcontroller import DBController
from utils import combine_dicts_in_list, subtract_dates
from latex import Latex

import math
from fractions import Fraction

def summary_table(data, year, team):
    current_year = datetime.date.today().strftime('%Y')
    game_data = data['preview'][0]['gameData']
    game_state = game_data['status']['detailedState']

    home_abbr = data['home']
    away_abbr = data['away']

    if game_state == 'Scheduled':
        home_name = game_data['teams']['home']['name']
        away_name = game_data['teams']['away']['name']
        home_abbr = game_data['teams']['home']['abbreviation']
        away_abbr = game_data['teams']['away']['abbreviation']
        home_rec  = game_data['teams']['home']['record']['leagueRecord']
        away_rec  = game_data['teams']['away']['record']['leagueRecord']

        raw_game_time = game_data['datetime']['time']
        hour = int(raw_game_time.split(':')[0]) + 1
        mins = raw_game_time.split(':')[1]
        game_time = "{}:{}".format(hour, mins)

    else:
        home_name = game_data['teams']['home']['name']['full']
        away_name = game_data['teams']['away']['name']['full']
        home_abbr = game_data['teams']['home']['name']['abbrev']
        away_abbr = game_data['teams']['away']['name']['abbrev']
        home_rec  = game_data['teams']['home']['record']
        away_rec  = game_data['teams']['away']['record']
        game_time = game_data['datetime']['time']

    home_wins = home_rec['wins']
    away_wins = away_rec['wins']

    home_losses = home_rec['losses']
    away_losses = away_rec['losses']

    if team == home_abbr:
        game_number = int(home_losses) + int(home_wins) + 1
    elif team == away_abbr:
        game_number = int(away_losses) + int(away_wins) + 1
    else:
        print("Error determining game number")

    am_or_pm = game_data['datetime']['ampm']
    stadium  = game_data['venue']['name']

    title = "{} ({}-{}) @ {} ({}-{})".format(away_name,
                                              away_wins,
                                              away_losses,
                                              home_name,
                                              home_wins,
                                              home_losses)

    details = '{}{} {}'.format(game_time, am_or_pm, stadium)

    # Forecast
    try:
        forecast = game_data['weather']
        condition = forecast['condition']
        temp = forecast['temp']
        wind = forecast['wind']
    except:
        condition, temp, wind = 'n/a', 'n/a', 'n/a'

    # Starting pitchers
    try:    # Game state: scheduled
        pitchers = game_data['probablePitchers']
        players  = game_data['players']

        away_pit_id = 'ID' + str(pitchers['away']['id'])
        home_pit_id = 'ID' + str(pitchers['home']['id'])

        away_pit_data = players[away_pit_id]
        home_pit_data = players[home_pit_id]

        away_pit_name = away_pit_data['fullName']
        home_pit_name = home_pit_data['fullName']

        away_pit_num = away_pit_data['primaryNumber']
        home_pit_num = home_pit_data['primaryNumber']

        away_pit_hand = away_pit_data['pitchHand']['code']
        home_pit_hand = home_pit_data['pitchHand']['code']

    except:
        pitchers = data['preview'][0]['liveData']['boxscore']['teams']
        players  = data['preview'][0]['liveData']['players']['allPlayers']

        # Are these the starting pitchers?
        away_pit_id = 'ID' + str(pitchers['away']['pitchers'][0])
        home_pit_id = 'ID' + str(pitchers['home']['pitchers'][0])

        away_pit_data = players[away_pit_id]
        home_pit_data = players[home_pit_id]

        away_pit_name = ' '.join((away_pit_data['name']['first'],
                                  away_pit_data['name']['last']))

        home_pit_name = ' '.join((home_pit_data['name']['first'],
                                  home_pit_data['name']['last']))

        away_pit_num = away_pit_data['shirtNum']
        home_pit_num = home_pit_data['shirtNum']

        away_pit_hand = away_pit_data['rightLeft']
        home_pit_hand = home_pit_data['rightLeft']

    away_decoded = unidecode(away_pit_name)
    home_decoded = unidecode(home_pit_name)

    away_pit_stats = dbc.get_player(away_decoded)['fg']['pit'][year]
    home_pit_stats = dbc.get_player(home_decoded)['fg']['pit'][year]

    pit_cols = ['Team', 'R/L', '#', 'Name', 'pit_WAR', 'W', 'L', 'ERA',
                'IP', 'K/9', 'BB/9', 'HR/9', 'GB%']

    away_pit_data = {k:v for k,v in away_pit_stats.items() if k in pit_cols}

    home_pit_data = {k:v for k,v in home_pit_stats.items() if k in pit_cols}

    away_pit_data['Team'] = away_abbr
    away_pit_data['Name'] = away_pit_name
    away_pit_data['R/L'] = away_pit_hand
    away_pit_data['#'] = away_pit_num

    home_pit_data['Team'] = home_abbr
    home_pit_data['Name'] = home_pit_name
    home_pit_data['R/L'] = home_pit_hand
    home_pit_data['#'] = home_pit_num

    pit_df = pd.DataFrame([away_pit_data, home_pit_data])
    pit_df = pit_df[pit_cols].rename({'pit_WAR' : 'WAR'}, axis='columns')

    return {'game' : game_number,
            'title' : title,
            'details' : details,
            'condition' : condition,
            'temp' : temp,
            'wind' : wind,
            'pit_df': pit_df}


def rosters(who, data, year):
    game_state = data['preview'][0]['gameData']['status']['detailedState']
    live_data = data['preview'][0]['liveData']['boxscore']

    if game_state == 'Scheduled':
        away_batter_ids = live_data['teams']['away']['players'].keys()
        home_batter_ids = live_data['teams']['home']['players'].keys()
    else:
        if who == 'starters':
            away_batters = live_data['teams']['away']['battingOrder']
            home_batters = live_data['teams']['home']['battingOrder']

        elif who == 'bench':
            away_batters = live_data['teams']['away']['bench']
            home_batters = live_data['teams']['home']['bench']

        away_batter_ids = ['ID{}'.format(x) for x in away_batters]
        home_batter_ids = ['ID{}'.format(x) for x in home_batters]

    def generate_stats_table(batter_ids, team):
        try:
            team_name = data['preview'][0]['gameData']\
                            ['teams'][team]['name']['abbrev']
        except:
            team_name = data['preview'][0]['gameData']\
                            ['teams'][team]['abbreviation']

        df_cols = ['Order', 'Position', 'Number', 'Name',
                   'WAR', 'Slash line', 'HR', 'RBI',
                   'SB', 'Off WAR', 'Def WAR']
        df_data = []

        for idx, playerid in enumerate(batter_ids):
            order = idx + 1
            batter = live_data['teams'][team]['players'][playerid]
            try:
                name = ' '.join((batter['name']['first'],
                                 batter['name']['last']))
            except:
                name = batter['person']['fullName']
            decoded = unidecode(name)
            try:
                try:
                    pos = batter['position']['abbreviation']
                except:
                    pos = batter['position']
            except:
                pos = None
            try:
                try:
                    num = batter['shirtNum']
                except:
                    num = batter['jerseyNumber']
            except:
                num = None

            # Find player data from fg/br data
            pobj = dbc.get_player(decoded)
            try:
                try:
                    pdata = pobj['fg']['bat'][year]
                    avg = pdata['AVG']
                except:
                    pdata = pobj['br']['Standard Batting'][year]
                    avg = pdata['BA']
                obp = pdata['OBP']
                slg = pdata['SLG']
                hrs = pdata['HR']
                rbi = pdata['RBI']
                sb  = pdata['SB']
                slashline = '{}/{}/{}'.format(
                    "{:.3f}".format(avg).lstrip('0'),
                    "{:.3f}".format(obp).lstrip('0'),
                    "{:.3f}".format(slg).lstrip('0'))
            except:
                avg = None
                obp = None
                slg = None
                hrs = None
                rbi = None
                sb  = None
                slashline = None

            # Query WAR stats from Players collection
            # Replace try/except with has_data() method
            try:
                try:
                    war_stats = dbc.get_player_war_fg(player=decoded,
                                                      kind='batter',
                                                      year=year)
                    war  = war_stats['war']
                    off  = war_stats['off']
                    def_ = war_stats['def']
                except:
                    war_stats = dbc.get_player_war_br(player=decoded,
                                                      kind='batter',
                                                      year=year)
                    war  = war_stats['war']
                    off  = war_stats['off']
                    def_ = war_stats['def']
            except:
                print("No {} data for {}".format(year, name))
                war  = None
                off  = None
                def_ = None

            df_data.append([order, pos, num, name, war,
                            slashline, hrs, rbi, sb, off, def_])

        df = pd.DataFrame(df_data, columns = df_cols)
        return df

    away_df = generate_stats_table(away_batter_ids, 'away')
    home_df = generate_stats_table(home_batter_ids, 'home')

    if state == 'Scheduled':
        away_df =  away_df.loc[away_df['Position'] != 'P']
        home_df =  home_df.loc[home_df['Position'] != 'P']

        away_df = away_df.sort_values(by='WAR', ascending=False)
        home_df = home_df.sort_values(by='WAR', ascending=False)

        away_df = away_df.drop(columns='Order')
        home_df = home_df.drop(columns='Order')

    away_df = away_df.fillna(value='-')
    home_df = home_df.fillna(value='-')

    return (away_df, home_df)


def bullpen(data, year):
    """
    """
    live_data = data['preview'][0]['liveData']['boxscore']

    away_bullpen = live_data['teams']['away']['bullpen']
    home_bullpen = live_data['teams']['home']['bullpen']

    away_bullpen_ids = ['ID{}'.format(x) for x in away_bullpen]
    home_bullpen_ids = ['ID{}'.format(x) for x in home_bullpen]

    def generate_stats_table(bullpen_ids, team):
        try:
            team_name = data['preview'][0]['gameData']\
                            ['teams'][team]['name']['abbrev']
        except:
            team_name = data['preview'][0]['gameData']\
                            ['teams'][team]['abbreviation']

        df_cols = ['Name', 'Number', 'WAR', 'SV', 'ERA',
                   'IP', 'k/9', 'bb/9', 'hr/9', 'gb%', 'days']
        df_data = []

        for playerid in bullpen_ids:
            pitch = live_data['teams'][team]['players'][playerid]
            name = ' '.join((pitch['name']['first'],
                             pitch['name']['last']))
            try:
                num = pitch['shirtNum']
            except:
                num = '--'
            hits = pitch['seasonStats']['pitching']['hits']
            runs = pitch['seasonStats']['pitching']['runs']
            eruns = pitch['seasonStats']['pitching']['earnedRuns']
            strikeouts = pitch['seasonStats']['pitching']['strikeOuts']

            # Query stats from Players collection
            decoded = unidecode(name)

            #Replace try/except clauses with has_data() method
            try:
                try:
                    pitstats = dbc.get_player(decoded)['fg']['pit'][year]
                    war = pitstats['pit_WAR']
                    sv  = pitstats['SV']
                    era = pitstats['ERA']
                    ip  = pitstats['IP']
                    k9  = pitstats['K/9']
                    bb9 = pitstats['BB/9']
                    hr9 = pitstats['HR/9']
                    gb  = pitstats['GB%']
                except:
                    sp  = dbc.get_player(decoded)['br']\
                                                 ['Standard Pitching'][year]
                    pv  = dbc.get_player(decoded)['br']\
                                                 ['Pitching Value'][year]
                    war = pv['WAR']
                    sv  = sp['SV']
                    era = sp['ERA']
                    ip  = sp['IP']
                    k9  = None
                    bb9 = sp['BB9']
                    hr9 = sp['HR9']
                    gb  = None
            except:
                print("No {} data for {}".format(year, name))
                war = None
                sv  = None
                era = None
                ip  = None
                k9  = None
                bb9 = None
                hr9 = None
                gb  = None


            # Find number of days since last active
            today = datetime.date.today().strftime('%Y-%m-%d')
            last_date = dbc.get_last_pitch_date(decoded, team_name)
            if last_date:
                days = subtract_dates(today, last_date)
            else:
                days = None

            df_data.append([decoded, num, war, sv, era,
                            ip, k9, bb9, hr9, gb, days])

        df = pd.DataFrame(df_data, columns=df_cols)
        return df

    away_df = generate_stats_table(away_bullpen_ids, 'away')
    home_df = generate_stats_table(home_bullpen_ids, 'home')

    away_df = away_df.sort_values(by='IP', ascending=False)
    home_df = home_df.sort_values(by='IP', ascending=False)

    away_df = away_df.fillna(value='-')
    home_df = home_df.fillna(value='-')

    return (away_df, home_df)


def game_history(team):
    sched = dbc.get_team(team)['Schedule']

    cols = ['Date', 'Time', 'Opp', 'Result', 'Score', 'GB']
    df = pd.DataFrame(sched)
    df = df.loc[df['']=='boxscore']

    df['Opp'] = df[['Field', 'Opp']].apply(lambda x:' '.join((x[0], x[1]))
                                                       .strip(), axis=1)

    df['Score'] = df[['R', 'RA']].apply(lambda x: '{}-{}'.format(int(x[0]),
                                                                 int(x[1])),
                                                                 axis=1)
    def format_date(x):
        _, m, d = x.split()[:3]
        df_date = '{} {}'.format(m, d)
        dt_date = datetime.datetime.strptime(df_date, '%b %d')
        return dt_date.strftime("%m-%d")

    df['Date'] = df.Date.apply(lambda x: format_date(x))
    df = df.rename({'W/L' : 'Result'}, axis=1)
    df = df[cols]

    return df


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


def standings(home, away):
    home_div = dbc.get_team(home)['div']
    away_div = dbc.get_team(away)['div']

    divh = list(dbc.get_teams_by_division(home_div))
    diva = list(dbc.get_teams_by_division(away_div))

    divh_data = combine_dicts_in_list(divh)
    diva_data = combine_dicts_in_list(diva)

    # If divisions are the same, just return one dataframe
    if divh == diva:
        divs = [divh_data]
    else:
        divs = [divh_data, diva_data]

    df_results = []
    for teams in divs:
        standings_cols = ['Tm', 'W', 'L', 'last10', 'gb',
                          'div', 'Strk', 'Home', 'Road', 'W-L%']
        df = pd.DataFrame(teams)[standings_cols]
        df = df.sort_values(by='W-L%', ascending=False)
        # Find home/away win %
        def win_percent(x):
            w, l = x.split('-')
            total = int(w) + int(l)
            percent = float(w) / total
            return round(percent, 3)

        df['home_rec'] = df.Home.apply(lambda x: win_percent(x))
        df['away_rec'] = df.Road.apply(lambda x: win_percent(x))

        # Rename team column to division value
        div = df.iloc[0]['div']
        df = df.rename(columns={'Tm' : div})

        # Drop unused cols
        df.drop('div',  axis=1, inplace=True)
        df.drop('W-L%', axis=1, inplace=True)
        df.drop('Home', axis=1, inplace=True)
        df.drop('Road', axis=1, inplace=True)
        df_results.append(df)

    return df_results


def leaderboards(kind, stat, n, role='starter'):
    current_year = datetime.date.today().strftime('%Y')
    if kind == 'bat':
        leaders = dbc.get_top_n_leaders(kind=kind,
                                        stat=stat,
                                        year=current_year,
                                        n=n)
    elif kind in ['pit', 'reliever']:
        if stat == 'WAR':
            ascending = False
        elif stat == 'ERA':
            ascending = True
        leaders = dbc.get_top_n_pitchers(role=role,
                                         stat=stat,
                                         year = current_year,
                                         ascending=ascending,
                                         n=n)
    lb_data = combine_dicts_in_list(leaders)

    # Rename WAR col
    renames = {'bat' : ('bat_rank', 'bat_WAR'),
               'pit' : ('pit_rank', 'pit_WAR')}

    df = pd.DataFrame(lb_data).rename({renames[kind][0] : 'Rank',
                                       renames[kind][1] : 'WAR'}, axis=1)
    if kind == 'bat':
        def make_slash_line(*x):
            avg, obp, slg = x
            return '{}/{}/{}'.format(
                "{:.3f}".format(avg).lstrip('0'),
                "{:.3f}".format(obp).lstrip('0'),
                "{:.3f}".format(slg).lstrip('0'))

        df['AVG/OBP/SLG'] = df[['AVG', 'OBP', 'SLG']]\
                             .apply(lambda x: make_slash_line(*x), axis=1)

    elif kind == 'pit':
        def format_wl(*x):
            return '{}-{}'.format(x[0], x[1])

        df['W/L'] = df[['W', 'L']].apply(lambda x: format_wl(*x), axis=1)

    # Custom columns for each table type
    # Maybe best to pass this into the function?
    if stat == 'HR':
        cols = ['Name', 'Team', 'HR']

    elif stat == 'RBI':
        cols = ['Name', 'Team', 'RBI']

    elif kind == 'bat':
        cols = ['Rank', 'Name', 'Team', 'WAR',
                'AVG/OBP/SLG', 'HR', 'RBI', 'SB',
                'BB%', 'K%', 'BABIP', 'Off', 'Def']

    elif kind == 'pit':
        cols = ['Rank', 'Name', 'Team', 'WAR', 'W', 'L',
                'ERA', 'IP', 'K/9', 'BB/9', 'HR/9', 'GB%']
    df = df[cols]

    # Fix rank column
    df['Rank'] = df.index + 1

    return df

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


if __name__ == '__main__':
    today = datetime.date.today().strftime('%Y-%m-%d')
    current_year = today.split('-')[0]

    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--team', default='NYY')
    parser.add_argument('-d', '--date', default=today)
    args = parser.parse_args()

    year = args.date.split('-')[0]

    # Create database controller object
    dbc = DBController()

    # Gather game previews
    print("Gathering game previews...")
    scrape.game_previews()

    print("Scraping past boxscores...")
    scrape.boxscores(date='all')

    # Query upcomming game and populate data
    game = dbc.get_team_game_preview(team=args.team, date=args.date)

    game_data = extract_game_data(game)
    if game_data:
        home = game_data['home']
        away = game_data['away']
        state = game_data['preview'][0]['gameData']['status']['detailedState']
    else:
        raise ValueError("NO GAME FOUND")

    # Gather global leaderboard data
    print("Scraping batting leaderboard...")
    scrape.fangraphs(state='bat', year=current_year)

    print("Scraping pitching leaderboard...")
    scrape.fangraphs(state='pit', year=current_year)

    print("Scraping schedule, roster, pitch logs, injuries, transactions...")
    for team in [home, away]:
        scrape.schedule(team)
        scrape.pitching_logs(team, year)
        scrape.current_injuries(team)
        scrape.transactions(team, year)
        scrape.forty_man(team, year)

    scrape.league_elo()

    summary   = summary_table(data=game_data, year=year, team=args.team)
    if state == 'Scheduled':
        starters = rosters(who='all', data=game_data, year=year)
        bench = None
    else:
        starters  = rosters(who='starters', data=game_data, year=year)
        bench     = rosters(who='bench', data=game_data, year=year)
    bullpen   = bullpen(data=game_data, year=year)
    standings = standings(home, away)
    ahistory = game_history(away)
    hhistory = game_history(home)
    bat_df = leaderboards(kind='bat', stat='WAR', n=30, role='starter')
    pit_df = leaderboards(kind='pit', stat='WAR', n=30, role='starter')
    era_df = leaderboards(kind='pit', stat='ERA', n=10, role='starter')
    rel_df = leaderboards(kind='pit', stat='WAR', n=10, role='reliever')
    hr_df  = leaderboards(kind='bat', stat='HR',  n=10, role='starter')
    rbi_df = leaderboards(kind='bat', stat='RBI', n=10, role='starter')
    elo = elo()
    pitcher_history = pitcher_history(team=args.team)
    last_week_bullpen = previous_week_bullpen(team=args.team)
    series_table = series_results(team=args.team)

    # print(summary)
    # print(starters)
    # print(bench)
    # print(bullpen)
    # for table in standings:
    #     print(table)
    # print(ahistory)
    # print(hhistory)
    # print(bat_df)
    # print(pit_df)
    # print(era_df)
    # print(rel_df)
    # print(hr_df)
    # print(elo)
    # print(pitcher_history)
    # print(last_week_bullpen)
    # print(series_table)

    l = Latex("{}-{}.tex".format(args.team, args.date))
    l.header()
    l.title(summary)

    l.start_table('lcclrrrrrrrrr')
    l.add_headers(['Team', 'R/L', '#', 'Name', 'war', 'w', 'l', 'era', 'ip', 'k/9', 'bb/9', 'hr/9', 'gb%'])
    l.add_rows(summary['pit_df'], ['', '', '{:.0f}', '', '{:.1f}', '{:.0f}', '{:.0f}', '{:.2f}', '{:.1f}', '{:.2f}', '{:.2f}', '{:.2f}', '{:.2f}'])
    l.end_table()

    l.add_section("{} Lineup".format(away))
    l.start_table('lcclrcrrrrr')
    l.add_headers(['', 'Pos', '#', 'Name', 'war', 'slash', 'hr', 'rbi', 'sb', 'owar', 'dwar'])
    l.add_rows(starters[0], ['{:.0f}', '', '{:.0f}', '', '{:.1f}', '', '{:.0f}', '{:.0f}', '{:.0f}', '{:.1f}', '{:.1f}'])
    if bench:
        l.add_divider()
        l.add_rows(bench[0], ['{:.0f}', '', '{:.0f}', '', '{:.1f}', '', '{:.0f}', '{:.0f}', '{:.0f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.add_section("{} Lineup".format(home))
    l.start_table('lcclrcrrrrr')
    l.add_headers(['', 'Pos', '#', 'Name', 'war', 'slash', 'hr', 'rbi', 'sb', 'owar', 'dwar'])
    l.add_rows(starters[1], ['{:.0f}', '', '{:.0f}', '', '{:.1f}', '', '{:.0f}', '{:.0f}', '{:.0f}', '{:.1f}', '{:.1f}'])
    if bench:
        l.add_divider()
        l.add_rows(bench[1], ['{:.0f}', '', '{:.0f}', '', '{:.1f}', '', '{:.0f}', '{:.0f}', '{:.0f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.add_section("Standings")
    l.start_multicol(2)
    for table in standings:
        l.start_table('lrrcccrr')
        l.add_headers([table.columns[0], 'w', 'l', 'l10', 'gb', 'strk', 'home', 'away'])
        l.add_rows(table, ['', '{:.0f}', '{:.0f}', '', '{:.0f}', '', '{:.3f}', '{:.3f}'])
        l.end_table()
    l.end_multicol()

    l.page_break()
    l.add_subsection("{} Bullpen".format(away))
    l.start_table('lcrrrrrrrr')
    l.add_headers(['Name', '#', 'war', 'sv', 'era', 'ip', 'k/9', 'bb/9', 'hr/9', 'gb%'])
    l.add_rows(bullpen[0], ['', '{:.0f}', '{:.1f}', '{:.0f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.add_subsection("{} Bullpen".format(home))
    l.start_table('lcrrrrrrrr')
    l.add_headers(['Name', '#', 'war', 'sv', 'era', 'ip', 'k/9', 'bb/9', 'hr/9', 'gb%'])
    l.add_rows(bullpen[1], ['', '{:.0f}', '{:.1f}', '{:.0f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.add_section("{} Recent Starts".format(args.team))
    l.start_table('lclrrrrrrrr')
    l.add_headers(['Date', 'Opp', 'Starter', 'ip', 'h', 'r', 'er', 'bb', 'k', 'hr', 'gs'])
    l.add_rows(pitcher_history, ['', '', '', '{:.1f}', '{:.0f}', '{:.0f}', '{:.0f}', '{:.0f}', '{:.0f}', '{:.0f}', '{:.0f}'])
    l.end_table()

    l.page_break()
    l.start_multicol(2)
    l.add_subsection("{} Game Log".format(away))
    l.start_table('lrlccc')
    l.add_headers(['Date', 'Time', 'Opp', ' ', 'Score', 'gb'])
    l.add_rows(ahistory, ['', '', '', '', '', ''])
    l.end_table()

    l.add_subsection("{} Game Log".format(home))
    l.start_table('lrlccc')
    l.add_headers(['Date', 'Time', 'Opp', ' ', 'Score', 'gb'])
    l.add_rows(hhistory, ['', '', '', '', '', ''])
    l.end_table()
    l.end_multicol()

    l.add_section("Batting Leaderboards")
    l.start_table('rllrcrrrrrrrr')
    l.add_headers(['', 'Name', 'Team', 'war', 'slash', 'hr', 'rbi', 'sb', 'bb%', 'k%', 'babip', 'owar', 'dwar'])
    l.add_rows(bat_df, ['{:.0f}', '', '', '{:.1f}', '', '{:.0f}', '{:.0f}', '{:.0f}', '{:.1f}', '{:.1f}', '{:.3f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.start_multicol(2)
    l.add_subsection("HR")
    l.start_table('llrr')
    l.add_headers(['Name','Team', 'hr', '#'])
    l.add_rows(hr_df, ['', '', '{:.0f}', '{:.0f}'])
    l.end_table()

    l.add_subsection("RBI")
    l.start_table('llrr')
    l.add_headers(['Name','Team', 'rbi', '#'])
    l.add_rows(rbi_df, ['', '', '{:.0f}', '{:.0f}'])
    l.end_table()
    l.end_multicol()

    l.add_section("Pitching Leaderboards")
    l.add_subsection("WAR")
    l.start_table('rllrrrrrrrrr')
    l.add_headers(['','Name','Team','war','w','l','era','ip','k/9','bb/9','hr/9','gb%'])
    l.add_rows(pit_df, ['{:.0f}', '', '', '{:.1f}', '{:.0f}', '{:.0f}', '{:.2f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.2f}'])
    l.end_table()

    l.add_subsection("WAR - Relivers")
    l.start_table('rllrrrrrrrrr')
    l.add_headers(['','Name','Team','war','w','l','era','ip','k/9','bb/9','hr/9','gb%'])
    l.add_rows(rel_df, ['{:.0f}', '', '', '{:.1f}', '{:.0f}', '{:.0f}', '{:.2f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.2f}'])
    l.end_table()

    l.add_subsection("ERA")
    l.start_table('rllrrrrrrrrr')
    l.add_headers(['', 'Name', 'Team', 'war', 'w', 'l', 'era', 'ip', 'k/9', 'bb/9', 'hr/9', 'gb%'])
    l.add_rows(era_df, ['{:.0f}', '', '', '{:.1f}', '{:.0f}', '{:.0f}', '{:.2f}', '{:.1f}', '{:.2f}', '{:.2f}', '{:.2f}', '{:.2f}'])
    l.end_table()

    l.add_section("ELO Ratings")
    l.start_table('rrlrrr')
    l.add_headers(['', 'Rating', 'Team', 'div%', 'post%', 'ws%'])
    l.add_rows(elo, ['{:.0f}', '{:.0f}', '', '{:.2f}', '{:.2f}', '{:.2f}'])
    l.end_table()

    l.footer()
    l.make_pdf()
