import scrape
import argparse
import datetime
import pandas as pd
import unidecode
from collections import defaultdict

from dbcontroller import DBController
from scrape import convert_name


def combine_dicts_in_list(list_of_dicts):
    """
    !!! Move to utils file
    Convert db format into df format
    """
    data = defaultdict(list)
    for d in list_of_dicts:
        for k, v in d.items():
            data[k].append(v)
    return data

def summary_table(data, year):
    current_year = datetime.date.today().strftime('%Y')
    game_number = data['preview'][0]['gameData']['game']['gameNumber']

    home_abbr = data['home']
    away_abbr = data['away']

    try:
        home_name_full = data['preview'][0]['gameData']\
                             ['teams']['home']['name']

        away_name_full = data['preview'][0]['gameData']\
                             ['teams']['away']['name']
    except:
        home_name_full = data['preview'][0]['gameData']\
                             ['teams']['home']['name']['full']

        away_name_full = data['preview'][0]['gameData']\
                             ['teams']['away']['name']['full']

    try:
        home_rec = data['preview'][0]['gameData']['teams']\
                       ['home']['record']['leagueRecord']

        away_rec = data['preview'][0]['gameData']['teams']\
                       ['away']['record']['leagueRecord']
    except:
        home_rec = data['preview'][0]['gameData']\
                       ['teams']['away']['record']

        away_rec = data['preview'][0]['gameData']\
                       ['teams']['away']['record']

    home_wins = home_rec['wins']
    away_wins = away_rec['wins']

    home_losses = home_rec['losses']
    away_losses = away_rec['losses']

    title = "{} ({}-{}) @ {} ({}-{})".format(away_name_full,
                                              away_wins,
                                              away_losses,
                                              home_name_full,
                                              home_wins,
                                              home_losses)

    game_time = data['preview'][0]['gameData']['datetime']['time']
    am_or_pm = data['preview'][0]['gameData']['datetime']['ampm']
    stadium  = data['preview'][0]['gameData']['venue']['name']

    # Forecast
    # Summary Text - where is this located?

    # Starting pitchers
    try:    # Game state: scheduled
        pitchers = data['preview'][0]['gameData']['probablePitchers']
        players  = data['preview'][0]['gameData']['players']

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

    away_pit_stats = dbc.get_player(away_pit_name)[year]
    home_pit_stats = dbc.get_player(home_pit_name)[year]

    pit_cols = ['Name', 'pit_WAR', 'W', 'L', 'ERA',
                'IP', 'K/9', 'BB/9', 'HR/9', 'GB%']

    away_pit_data = {k:v for k,v in away_pit_stats.items() if k in pit_cols}

    home_pit_data = {k:v for k,v in home_pit_stats.items() if k in pit_cols}

    away_pit_data['Name'] = '{} {} {} {}'.format(away_abbr,
                                                 away_pit_hand,
                                                 away_pit_num,
                                                 away_pit_name)

    home_pit_data['Name'] = '{} {} {} {}'.format(home_abbr,
                                                 home_pit_hand,
                                                 home_pit_num,
                                                 home_pit_name)

    pit_df = pd.DataFrame([away_pit_data, home_pit_data])
    pit_df = pit_df[pit_cols].rename({'pit_WAR' : 'WAR'}, axis='columns')

    return pit_df

def rosters(who, data, year):
    live_data = data['preview'][0]['liveData']['boxscore']

    if who == 'starters':
        away_batters = live_data['teams']['away']['battingOrder']
        home_batters = live_data['teams']['home']['battingOrder']

    elif who == 'bench':
        away_batters = live_data['teams']['away']['bench']
        home_batters = live_data['teams']['home']['bench']

    away_batter_ids = ['ID{}'.format(x) for x in away_batters]
    home_batter_ids = ['ID{}'.format(x) for x in home_batters]

    def generate_stats_table(batter_ids, team):
        df_cols = ['Order', 'Position', 'Number', 'Name',
                   'WAR', 'Slash line', 'HR', 'RBI',
                   'SB', 'Off WAR', 'Def WAR']
        df_data = []

        for idx, playerid in enumerate(batter_ids):
            order = idx + 1
            batter = live_data['teams'][team]['players'][playerid]
            name = ' '.join((batter['name']['first'],
                             batter['name']['last']))
            try:
                pos = batter['position']
            except:
                pos = ''
            num = batter['shirtNum']
            batstats = batter['seasonStats']['batting']
            avg = batstats['avg']
            obp = batstats['obp']
            slg = batstats['slg']
            hrs = batstats['homeRuns']
            rbi = batstats['rbi']
            sb  = batstats['stolenBases']
            slashline = '{}/{}/{}'.format(avg, obp, slg)

            # Query WAR stats from Players collection
            try:
                decoded = unidecode.unidecode(name)
                war_stats = dbc.get_player_war(player=decoded,
                                               type='batter',
                                               year=year)
                war  = war_stats['war']
                off  = war_stats['off']
                def_ = war_stats['def']
            except:
                war  = 'N/A'
                off  = 'N/A'
                def_ = 'N/A'
                print("{} not in Players collection".format(name))

            df_data.append([order, pos, num, name, war,
                            slashline, hrs, rbi, sb, off, def_])

        df = pd.DataFrame(df_data, columns = df_cols)
        return df

    away_df = generate_stats_table(away_batter_ids, 'away')
    home_df = generate_stats_table(home_batter_ids, 'home')
    return (away_df, home_df)


def bullpen(data, year):
    """
    Change try/except clause to check if player pos is pitcher
    once position data is in Players collections
    """
    live_data = data['preview'][0]['liveData']['boxscore']

    away_bullpen = live_data['teams']['away']['bullpen']
    home_bullpen = live_data['teams']['home']['bullpen']

    away_bullpen_ids = ['ID{}'.format(x) for x in away_bullpen]
    home_bullpen_ids = ['ID{}'.format(x) for x in home_bullpen]

    def generate_stats_table(bullpen_ids, team):
        df_cols = ['Name', 'Number', 'WAR', 'SV', 'ERA',
                   'IP', 'k/9', 'bb/9', 'hr/9', 'gb%']
        df_data = []

        for playerid in bullpen_ids:
            pitch = live_data['teams'][team]['players'][playerid]
            name = ' '.join((pitch['name']['first'],
                             pitch['name']['last']))
            num = pitch['shirtNum']
            hits = pitch['seasonStats']['pitching']['hits']
            runs = pitch['seasonStats']['pitching']['runs']
            eruns = pitch['seasonStats']['pitching']['earnedRuns']
            strikeouts = pitch['seasonStats']['pitching']['strikeOuts']

            # Query stats from Players collection
            decoded = unidecode.unidecode(name)
            pitstats = dbc.get_player(decoded)[year]
            war = pitstats['pit_WAR']
            sv  = pitstats['SV']
            era = pitstats['ERA']
            ip  = pitstats['IP']
            k9  = pitstats['K/9']
            bb9 = pitstats['BB/9']
            hr9 = pitstats['HR/9']
            gb  = pitstats['GB%']
            df_data.append([decoded, num, war, sv, era,
                        ip, k9, bb9, hr9, gb])
        df = pd.DataFrame(df_data, columns=df_cols)
        return df

    away_df = generate_stats_table(away_bullpen_ids, 'away')
    home_df = generate_stats_table(home_bullpen_ids, 'home')

    return (away_df, home_df)

# def game_history(*teams):
#     # Trigger scrape for each game where detailed state not "final" ?
#     hist = list(dbc.get_matchup_history(*teams))

#     df_data = []
#     for game in hist:
#         date = game['date']
#     time = game['preview'][0]['gameData']['datetime']['time']
#     tzone = game['preview'][0]['gameData']['datetime']['timeZone']
#     game_time = ' '.join((time, tzone))

def standings(home, away):
    teams = list(dbc.get_teams(home, away))
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

    return df


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
            return '{}/{}/{}'.format(avg, obp, slg)

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

    elif kind == 'bat':
        cols = ['Rank', 'Name', 'Team', 'WAR',
                'AVG/OBP/SLG', 'HR', 'RBI', 'SB',
                'BB%', 'K%', 'BABIP', 'Off', 'Def']

    elif role == 'reliever':
        cols = ['Rank', 'Name', 'Team', 'WAR']

    elif kind == 'pit':
        cols = ['Rank', 'Name', 'Team', 'WAR', 'W', 'L',
                'ERA', 'IP', 'K/9', 'BB/9', 'HR/9', 'GB%']
    df = df[cols]

    # Fix rank column
    df['Rank'] = df.index + 1

    return df

def elo():
    # League ELO
    elo_cols = ['Team', 'Rating', 'Division%',
                'Playoff%', 'WorldSeries%']
    elo_stats = dbc.get_elo_stats()
    elo_data = combine_dicts_in_list(elo_stats)
    df = pd.DataFrame(elo_data)
    df = df[elo_cols].sort_values(by='Rating', ascending=False)
    return df.reset_index()


if __name__ == '__main__':
    today = datetime.date.today().strftime('%Y-%m-%d')
    current_year = today.split('-')[0]

    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--team', default='NYY')
    parser.add_argument('-d', '--date', default=today)
    args = parser.parse_args()

    year = args.date.split('-')[0]


    # # Gather global leaderboard data
    # print("Scraping batting leaderboard...")
    # scrape.fangraphs(state='bat', year=current_year)

    # print("Scraping pitching leaderboard...")
    # scrape.fangraphs(state='pit', year=current_year)

    # # Gather game previews
    # print("Gathering game preivews...")
    # scrape.game_preview()

    # Create database controller object
    dbc = DBController()

    # Query upcomming game and populate data
    game = dbc.get_team_game_preview(team=args.team, date=args.date)

    try:
        game_data = list(game)[0]
        home = game_data['home']
        away = game_data['away']
    except:
        # throw error (no game found for that team/date)
        raise ValueError("NO GAME FOUND")

    # game_state = data['preview'][0]['gameData']['status']['detailedState']
    summary   = summary_table(data=game_data, year=year)
    starters  = rosters(who='starters', data=game_data, year=year)
    bench     = rosters(who='bench', data=game_data, year=year)
    bullpen   = bullpen(data=game_data, year=year)
    standings = standings(home, away)
    bat_df = leaderboards(kind='bat', stat='WAR', n=10, role='starter')
    pit_df = leaderboards(kind='pit', stat='WAR', n=10, role='starter')
    era_df = leaderboards(kind='pit', stat='ERA', n=10, role='starter')
    rel_df = leaderboards(kind='pit', stat='WAR', n=10, role='reliever')
    hr_df  = leaderboards(kind='bat', stat='HR',  n=10, role='starter')
    elo = elo()

    print(summary)
    print(starters)
    print(bench)
    print(bullpen)
    print(standings)
    print(bat_df)
    print(pit_df)
    print(era_df)
    print(rel_df)
    print(hr_df)
    print(elo)


