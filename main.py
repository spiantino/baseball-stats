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

def game_history(home, away):
    home = dbc.get_team(home)['Schedule']
    away = dbc.get_team(away)['Schedule']

    res = []
    cols = ['Date', 'Time', 'Opp', 'Result', 'Score']
    for team in [home, away]:
        df = pd.DataFrame(team)
        df = df.loc[df['']=='boxscore']

        df['Opp'] = df[['Field', 'Opp']].apply(lambda x:' '.join((x[0], x[1]))
                                                           .strip(), axis=1)

        df['Score'] = df[['R', 'RA']].apply(lambda x:''.join((x[0],'-',x[1])),
                                                                     axis=1)

        df = df.rename({'W/L' : 'Result'}, axis=1)
        df = df[cols]
        res.append(df)

    return res


def get_past_game_dates(team):
    """
    Find dates for last 10 games
    """
    data = dbc.get_team(team)['Schedule']
    df = pd.DataFrame(data)
    df = df.loc[df['']=='boxscore']

    year = datetime.date.today().strftime('%Y')
    def format_date(x, y):
        _, m, d = x.split()
        df_date = '{} {} {}'.format(m, d, y)
        return datetime.datetime.strptime(df_date, '%b %d %Y')

    df['Dates'] = df.Date.apply(lambda x: str(format_date(x, y=year).date()))
    return df.Dates.values


def pitcher_history(team):
    """
    A Teams pitcher history
    for previous 10 games
    """
    dates = get_past_game_dates(team)
    games = dbc.get_team_game_preview(team, date)

    cols = ['Date', 'Opponent', 'Pitcher', 'IP', 'Hits', 'Runs',
            'ER', 'Walks', 'Strikeouts', 'Home Runs', 'Score']
    df_data = []
    for game in games:
        away, home = game['away'], game['home']
        side = 'home' if team==home else 'away'
        opp_side = set({'home', 'away'} - {side}).pop()

        date = game['date']
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

        tscore = game['preview'][0]['liveData']['linescore'][side]['runs']
        oscore = game['preview'][0]['liveData']['linescore'][opp_side]['runs']

        score = '{}-{}'.format(tscore, oscore)

        df_data.append([date, opp, name, ip, hits, runs,
                        er, walks, strko, hr, score])


def chart_gb():
    # chart for games behind
    pass



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
        raise ValueError("NO GAME FOUND")

    # game_state = data['preview'][0]['gameData']['status']['detailedState']
    # summary   = summary_table(data=game_data, year=year)
    # starters  = rosters(who='starters', data=game_data, year=year)
    # bench     = rosters(who='bench', data=game_data, year=year)
    # bullpen   = bullpen(data=game_data, year=year)
    # standings = standings(home, away)
    history = game_history(home, away)
    # bat_df = leaderboards(kind='bat', stat='WAR', n=10, role='starter')
    # pit_df = leaderboards(kind='pit', stat='WAR', n=10, role='starter')
    # era_df = leaderboards(kind='pit', stat='ERA', n=10, role='starter')
    # rel_df = leaderboards(kind='pit', stat='WAR', n=10, role='reliever')
    # hr_df  = leaderboards(kind='bat', stat='HR',  n=10, role='starter')
    elo = elo()

    # print(summary)
    # print(starters)
    # print(bench)
    # print(bullpen)
    # print(standings)
    print(history[0])
    print(history[1])
    # print(bat_df)
    # print(pit_df)
    # print(era_df)
    # print(rel_df)
    # print(hr_df)
    # print(elo)

    import jinja2
    import os
    import subprocess
    import shutil
    from jinja2 import Template
    latex_jinja_env = jinja2.Environment(
                        block_start_string = '\BLOCK{',
                        block_end_string = '}',
                        variable_start_string = '\VAR{',
                        variable_end_string = '}',
                        comment_start_string = '\#{',
                        comment_end_string = '}',
                        line_statement_prefix = '%%',
                        line_comment_prefix = '%#',
                        trim_blocks = True,
                        autoescape = False,
                        loader = jinja2.FileSystemLoader(os.path.abspath('.'))
    )
    template = latex_jinja_env.get_template('template.tex')
    # render = template.render(elo=elo.to_latex())

    def compile_pdf_from_template(template, insert_variables, out_path):
        """Render a template file and compile it to pdf"""

        rendered_template = template.render(**insert_variables)
        build_d = os.path.join(os.path.dirname(os.path.realpath(out_path)), '.build')
        print(build_d)
        if not os.path.exists(build_d):  # create the build directory if not exisiting
            os.makedirs(build_d)

        temp_out = os.path.join(build_d, "tmp")
        with open(temp_out+'.tex', "w") as f:  # saves tex_code to output file
            f.write(rendered_template)

        # subprocess.check_call(['pdflatex', '{}/{}.tex'.format(build_d,
                                                              # temp_out)])


        os.system('pdflatex -output-directory {} {}'.format(build_d, temp_out+'.tex'))
        shutil.copy2(temp_out+".pdf", os.path.relpath(out_path))

    compile_pdf_from_template(template, {'elo': elo.to_latex(index=False)}, out_path='./test.pdf')




