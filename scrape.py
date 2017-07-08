from bs4 import BeautifulSoup, Comment
from df2gspread import df2gspread as d2g
# from urllib.request import urlopen
import pandas as pd
import numpy as np
import requests
import re
import json
import datetime
import pickle
import xmltodict as xd

# GOOGLE DOC ADDRESS
#'1aoVUZE3dAFEVQDWbOY9YqLNSw5cvJr4l4i16a3FM-aQ'

def open_url(url):
    # page = urlopen(url)
    page = requests.get(page)
    return BeautifulSoup(page.text, "html.parser")

def write_to_sheet(df, sheet_name, start_cell='A1', clean=True):
    print("Writing {} to sheet...".format(sheet_name))
    df = df.fillna('')
    d2g.upload( df=df,
                gfile='1aoVUZE3dAFEVQDWbOY9YqLNSw5cvJr4l4i16a3FM-aQ',
                wks_name=sheet_name,
                row_names=False,
                start_cell=start_cell,
                clean=clean
                )

def fanGraphs(type_, team, year):
    """
    Scrape data from fangraphs.com
    """
    # Turn args into acceptable parameters
    if type_ == 'batting':
        type_ = 'bat'

    elif type_ in ['pitch', 'pitching']:
        type_ = 'pit'

    team_id = {
               'all'          : 0,
               'angels'       : 1,
               'astros'       : 21,
               'athletics'    : 10,
               'blue jays'    : 14,
               'braves'       : 16,
               'brewers'      : 23,
               'cardinals'    : 28,
               'cubs'         : 17,
               'diamondbacks' : 15,
               'dodgers'      : 22,
               'giants'       : 30,
               'indians'      : 5,
               'mariners'     : 11,
               'marlins'      : 20,
               'mets'         : 25,
               'nationals'    : 24,
               'orioles'      : 2,
               'padres'       : 29,
               'phillies'     : 26,
               'pirates'      : 27,
               'rangers'      : 13,
               'rays'         : 12,
               'red sox'      : 3,
               'reds'         : 18,
               'rockies'      : 19,
               'royals'       : 7,
               'tigers'       : 6,
               'twins'        : 8,
               'white sox'    : 4,
               'yankees'      : 9
               }

    team = team.lower()
    tid = team_id[team]

    url = """http://www.fangraphs.com/leaders.aspx?pos=all&stats={0}\
             &lg=all&qual=0&type=8&season={1}\
             &month=0&season1={1}\
             &ind=0&team={2}"""\
             .format(type_, year, tid)\
             .replace(' ', '')

    with open('data.pkl', 'rb') as f:
        params = pickle.load(f)

    res = requests.post(url, data=params).text
    res = res.replace('\ufeff', '#,')
    res = res.replace('"', '')
    res = res.replace('\r\n', ',1,')

    data = res.split(',')[:-2]

    row_len = 22 if type_=='bat' else 20
    row_len += 1 if team=='all' else 0

    data = np.array(data).reshape(-1, row_len)

    cols, dfdata = data[:1].reshape(-1,), data[1:]
    df = pd.DataFrame(dfdata, columns=cols)

    df['#'] = df.index + 1
    df = df.iloc[:, :-1]

    type_ = 'Batting' if type_=='bat' else 'Pitching'
    # team = 'league' if team=='all' else team
    # sheet_name = '{}-{}-leaderboard-{}'.format(team, type_, year)
    sheet_name = '{} Leaderboard'.format(type_)
    write_to_sheet(df=df, sheet_name=sheet_name)

def br_standings():
    """
    Scrape MLB standings or Yankees schedule
    from baseball-reference.com
    """
    url = "http://www.baseball-reference.com/leagues/MLB-standings.shtml"
    soup = open_url(url)

    divs = ['E', 'C', 'W']
    for i, div in enumerate(divs):
        tables = soup.find_all('table', {'id' : 'standings_{}'.format(div)})
        al_table, nl_table = tables[0], tables[1]

        for j, table in enumerate(tables):

            data = []
            for item in table.find_all(lambda tag: tag.has_attr('data-stat')):
                data.append(item.string)

            # Infer division name
            div_loc  = {
                         'E' : 'East',
                         'C' : 'Central',
                         'W' : 'West'
                        }

            div_type = {
                        0: 'AL',
                        1: 'NL'
                        }

            # Build output dataframe
            division = '{} {}'.format(div_type[j], div_loc[div])
            data = np.array(data).reshape([-1, 5])
            cols=[division, ' ', '  ', '   ', '    ']
            df=pd.DataFrame(data, columns=cols)

            # Write out with appropriate padding
            clean = True if (i==0 and j==0) else False
            pad_map = {
                        0: {0: 1,  1: 9},
                        1: {0: 17, 1: 25},
                        2: {0: 33, 1: 41}
                      }
            cell_idx = 'A{}'.format(pad_map[i][j])
            write_to_sheet(df=df,
                           sheet_name='Division Standings',
                           start_cell=cell_idx,
                           clean=clean)

    # Scrape full league standings
    comment = soup.find_all(string=lambda text: isinstance(text,Comment))
    comment_html = [x for x in comment if '<td' in x][-1].string

    table = BeautifulSoup(comment_html, "html.parser")

    # Locate table and extract data
    data = []
    for item in table.find_all(lambda tag: tag.has_attr('data-stat')):
        data.append(item.string)

    # Construct league standings dataframe
    data = np.array(data).reshape([-1, 27])
    cols, dfdata = data[:1].reshape(-1,), data[1:]
    standings = pd.DataFrame(dfdata, columns=cols)

    # Write output to sheet
    write_to_sheet(df=standings, sheet_name='League Standings')

def yankees_schedule():
    """
    Scrape yankees schedule with results
    from baseball-reference.com
    """
    url = "http://www.baseball-reference.com/teams/NYY/2017-schedule-scores.shtml"

    soup = open_url(url)

    table = soup.find('div', {'class': 'table_outer_container'})
    ths = table.find_all('th')

    data = []
    for item in table.find_all(lambda tag: tag.has_attr('data-stat')):
        data.append(item.text)

    # Extract games that have already been played
    current = next((x for x in data if x and
                    x.startswith("Game Preview")), None)

    idx = data.index(current) - 7
    already_played = np.array(data[:idx]).reshape([-1, 20])
    ap_cols = already_played[:1].reshape(-1,)
    ap_data = already_played[1:]

    df_ap = pd.DataFrame(ap_data, columns=ap_cols)
    df_ap = df_ap.loc[df_ap['Gm#'] != 'Gm#']
    df_ap['Streak'] = df_ap.Streak.apply(lambda x: "'{}".format(x))

    # Extract upcomming games schedule
    upcomming = np.array(data[idx:]).reshape([-1, 9])
    up_cols = ['Gm#', 'Date', '', 'Tm', ' ', 'Opp', '  ', '   ', 'D/N']

    df_up = pd.DataFrame(upcomming, columns=up_cols)
    df_up = df_up.loc[df_up['Gm#'] != 'Gm#']

    write_to_sheet(df=df_ap, sheet_name='Schedule Played')
    write_to_sheet(df=df_up, sheet_name='Schedule Upcoming')


def pitching_logs(team, year):
    """
    Scrape pitching logs from
    baseball-reference.com
    """
    team = team.upper()
    url = "http://www.baseball-reference.com/teams/tgl.cgi?team={}&t=p&year={}".format(team, year)

    soup = open_url(url)

    data = []
    for item in soup.find_all(lambda tag: tag.has_attr('data-stat')):
        data.append(item.text)

    # Slice to only capture relevant data
    data = data[data.index('Rk'): ]

    # Add None columns to match shape
    flags = ['May', 'Jun', 'Jul', 'Aug', 'September/October']
    for month in flags:
        try:
            idx = data.index(month)
            for i in range(idx+1, idx+3):
                data.insert(i, None)
        except:
            continue

    data =  np.array(data).reshape(-1, 34)
    cols = data[:1].reshape(-1,)
    dfdata = data[1:]

    df = pd.DataFrame(dfdata, columns=cols)
    df = df.loc[~df.Rk.isin(flags)]

    # Split pitchers into different columns
    pit_lists = df.iloc[:,-1].apply(lambda x: x.split(',')).tolist()
    max_len = max([len(x) for x in pit_lists])

    # Remove combined column
    df = df.iloc[:,:-1]

    # Add padding to make shapes match
    for l in pit_lists:
        pad = max_len - len(l)
        for _ in range(pad):
            l.append(None)

    # Add pitchers to new columns
    pitchers = np.array(pit_lists).T
    for i in range(0, max_len):
        colname = 'Pitcher_{}'.format(i+1)
        df.loc[:,colname] = pitchers[i]


    # sheet_name = '{}-pitching-logs-{}'.format(team.lower(), year)
    sheet_name = 'Pitching Logs'
    write_to_sheet(df=df, sheet_name=sheet_name)

def forty_man():
    """
    Extract 40-man roster from
    baseball-reference.com
    """
    url = "http://www.baseball-reference.com/teams/NYY/2017-roster.shtml"

    soup = open_url(url)
    table = soup.find('table', {'id' : 'the40man'})

    data = []
    for item in table.find_all(lambda tag: tag.has_attr('data-stat')):
        data.append(item.text)

    data = np.array(data).reshape(-1, 14)
    cols = data[:1].reshape(-1,).tolist()
    dfdata = data[1:]

    # Make blank cols unique (move this to write_to_sheet function?)
    i=1
    for col in cols:
        if col == '':
            cols[cols.index(col)] = ' ' * i
            i +=1

    df = pd.DataFrame(dfdata, columns=cols)
    df = df.iloc[:-1]

    df.rename(columns={'': ' '}, inplace=True)
    write_to_sheet(df=df, sheet_name='40-Man Roster')

def current_injuries():
    """
    Extract current injuries table
    from baseball-reference.com
    """
    url = "http://www.baseball-reference.com/teams/NYY/2017.shtml"

    soup = open_url(url)

    # Data is stored in html comment
    comment = soup.find_all(string=lambda text: isinstance(text,Comment))
    comment_html = [x for x in comment if 'Injuries Table' in x][-1].string

    table = BeautifulSoup(comment_html, "html.parser")

    data = []
    for item in table.find_all(lambda tag: tag.has_attr('data-stat')):
        data.append(item.text)

    data = np.array(data).reshape(-1, 4)
    cols = data[:1].reshape(-1,).tolist()
    dfdata = data[1:]

    df = pd.DataFrame(dfdata, columns=cols)
    write_to_sheet(df=df, sheet_name='Current Injuries')

def transactions(team, year):
    """
    Extract transations from
    http://mlb.mlb.com/mlb/transactions
    """
    team = team.lower()
    team_id = {
               'angels'       : 108,
               'astros'       : 117,
               'athletics'    : 133,
               'blue jays'    : 141,
               'braves'       : 144,
               'brewers'      : 158,
               'cardinals'    : 138,
               'cubs'         : 112,
               'diamondbacks' : 109,
               'dodgers'      : 119,
               'giants'       : 137,
               'indians'      : 114,
               'mariners'     : 136,
               'marlins'      : 146,
               'mets'         : 121,
               'nationals'    : 120,
               'orioles'      : 110,
               'padres'       : 135,
               'phillies'     : 143,
               'pirates'      : 134,
               'rangers'      : 140,
               'rays'         : 139,
               'red sox'      : 111,
               'reds'         : 113,
               'rockies'      : 115,
               'royals'       : 118,
               'tigers'       : 116,
               'twins'        : 142,
               'white sox'    : 145,
               'yankees'      : 147,
               }
    tid = team_id[team]

    current_year = datetime.date.today().strftime('%Y')

    if str(year) == current_year:
        today = datetime.date.today().strftime('%Y%m%d')
        url = "http://mlb.mlb.com/lookup/json/named.transaction_all.bam?start_date={}0101&end_date={}&team_id={}".format(year, today, tid)
    else:
        url = "http://mlb.mlb.com/lookup/json/named.transaction_all.bam?start_date={0}0101&end_date={0}1231&team_id={1}".format(year, tid)

    # Open and read json object
    # res = urlopen(url).read()
    # j = json.loads(res.decode('utf-8'))

    res = requests.get(url)
    j = json.loads(res.text)

    transactions = j['transaction_all'] \
                    ['queryResults']    \
                    ['row']
    data = []
    for tran in transactions:
        date = tran['trans_date']
        note = tran['note']
        data.append([date, note])

    df = pd.DataFrame(data, columns=['Date', 'Transaction'])
    df = df.sort_values(by='Date', ascending=False)
    df['Date'] = pd.to_datetime(df.Date)
    df['Date'] = df.Date.apply(lambda x: x.strftime("%m/%d/%y"))

    # sheet_name = '{}-transactions-{}'.format(team, year)
    sheet_name = 'Transactions'
    write_to_sheet(df=df, sheet_name=sheet_name)

def boxscore(team):
    """
    Extract box scores from
    """
    team = team.lower()
    today = datetime.date.today().strftime('%Y-%m-%d')
    i = 0

    def team_search(i):
        """
        Search through previous days
        until match is found
        """
        yesterday = ((datetime.date.today() -
                      datetime.timedelta(i))
                      .strftime('%Y-%m-%d'))
        y,m,d = yesterday.split('-')
        print("""Searching for last game... looking at {}"""\
                                            .format(yesterday))
        url = "http://www.baseball-reference.com/boxes/?year={}\
               &month={}\
               &day={}"\
               .format(y,m,d)\
               .replace(' ', '')
        soup = open_url(url)

        matches = soup.find_all('table', {'class' : 'teams'})

        for match in matches:
            teams = [a.string.lower() for a in match.find_all('a', href=True)]

            # Search for team and if game is finished
            if all(x in ' '.join(teams) for x in ['final', team]):
                box_url = match.find_all('a', href=True)
                return box_url[1]['href']

        # If not found, search through the previous day
        i+=1
        return team_search(i)

    url = "http://www.baseball-reference.com" + team_search(i)
    soup = open_url(url)

    teams = [x.string for x in soup.find_all('h2')[:2]]

    # Extract summary table
    summary = soup.find('table', {
                                  'class' :
                                  'linescore nohover stats_table no_freeze'
                                  }
                        )
    sumstats = [x.string for x in
                summary.find_all('td', {'class' : 'center'})]

    dfdata = np.array(sumstats).reshape(2, 13)
    sumcols = ['Team', '1', '2', '3', '4', '5',
               '6', '7', '8', '9', 'R', 'H', 'E']
    sum_df = pd.DataFrame(dfdata, columns=sumcols)

    # Insert team names
    sum_df.loc[:,'Team'] = teams
    write_to_sheet(df=sum_df,
                   sheet_name='Boxscore',
                   clean=True)

    # Extract box-scores
    comment = soup.find_all(string=lambda text: isinstance(text,Comment))
    bat_tables = [x for x in comment if '>Batting</th>' in x]
    pit_table = [x for x in comment if '>Pitching</th>' in x][0]

    # Collect home and away batting box score
    b_hdf, b_adf = pd.DataFrame(), pd.DataFrame()
    for table in bat_tables:
        box = BeautifulSoup(table, "html.parser")

        data = []
        for item in box.find_all(lambda tag: tag.has_attr('data-stat')):
            data.append(item.text)
        players = [x.string for x in box.find_all('a', href=True)]
        players.append('Team Totals')

        data = np.array(data).reshape(-1, 22)
        cols = data[:1].reshape(-1,)
        dfdata = data[1:]

        df = pd.DataFrame(dfdata, columns=cols)
        df = df.dropna(axis=0)
        df = df.loc[df.Batting!='']
        df.loc[:,'Batting'] = players

        # Assign data frame to home or away
        if b_hdf.empty:
            b_hdf = df
        else:
            b_adf = df

    # Write to next available cell
    idx = sum_df.shape[0] + 3
    cell_idx = 'A{}'.format(idx)
    write_to_sheet(df=b_hdf,
                   sheet_name='Boxscore',
                   start_cell=cell_idx,
                   clean=False)

    idx += b_hdf.shape[0] + 2
    cell_idx = 'A{}'.format(idx)
    write_to_sheet(df=b_adf,
                   sheet_name='Boxscore',
                   start_cell=cell_idx,
                   clean=False)

    # Collect home and away pitching box score
    box = BeautifulSoup(pit_table, "html.parser")

    data = []
    for item in box.find_all(lambda tag: tag.has_attr('data-stat')):
        data.append(item.text)

    data = np.array(data).reshape(-1, 25)
    cols = data[:1].reshape(-1,)
    dfdata = data[1:]
    df = pd.DataFrame(dfdata, columns=cols)

    # Split df in two then concatenate
    idx2 = df.Pitching.tolist().index('Pitching')
    p_hdf, p_adf = df.iloc[:idx2], df.iloc[idx2+1:]
    p_hdf = p_hdf.loc[p_hdf.Pitching!='']
    p_adf = p_adf.loc[p_adf.Pitching!='']

    idx += b_adf.shape[0] + 2
    cell_idx = 'A{}'.format(idx)
    write_to_sheet(df=p_hdf,
                   sheet_name='Boxscore',
                   start_cell=cell_idx,
                   clean=False)

    idx += p_hdf.shape[0] + 2
    cell_idx = 'A{}'.format(idx)
    write_to_sheet(df=p_adf,
                   sheet_name='Boxscore',
                   start_cell=cell_idx,
                   clean=False)

def game_preview(team):
    """
    Collect data on upcomming game
    from mlb.com/gameday
    """
    today = datetime.date.today().strftime('%m/%d/%Y')
    m,d,y = today.split('/')

    url = 'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date='\
                                                        .format(today)
    team = team.lower()

    # res = urlopen(url).read()
    # schedule_data = json.loads(res.decode('utf-8'))

    res = requests.get(url)
    schedule_data = json.loads(res.text)

    data = schedule_data['dates'][0]['games']

    # Get game link
    game_data = [
                 x for x in data
                 if team in
                 x['teams']['home']['team']['name'].lower().split()
                 or team in
                 x['teams']['away']['team']['name'].lower().split()
                ]

    # Exit if game is not found
    if not game_data:
        print('Game preview not found')
        return

    # Collect sumamry data
    away = game_data[0]['teams']['away']['team']['name']
    home = game_data[0]['teams']['home']['team']['name']
    a_rec = game_data[0]['teams']['away']['leagueRecord'] #wins, losses, pct
    h_rec = game_data[0]['teams']['home']['leagueRecord']

    a_win, a_loss, a_pct = a_rec['wins'], a_rec['losses'], a_rec['pct']
    h_win, h_loss, h_pct = h_rec['wins'], h_rec['losses'], h_rec['pct']

    sum_df = pd.DataFrame({
                           'Teams'  : [home, away],
                           'Wins'   : [h_win, a_win],
                           'Losses' : [h_loss, a_loss],
                           'Pct'    : [h_pct, a_pct]
                         })
    sum_df = sum_df[['Teams', 'Wins', 'Losses', 'Pct']]

    # Open game url
    game_url = 'https://statsapi.mlb.com' + game_data[0]['link']
    # res = urlopen(game_url).read()
    # game_data = json.loads(res.decode('utf-8'))
    res = requests.get(url)
    schedule_data = json.loads(res.text)


    # Find batting lineups
    h_batter_ids = game_data['liveData']['boxscore']\
                            ['teams']['home']['battingOrder']

    a_batter_ids = game_data['liveData']['boxscore']\
                            ['teams']['away']['battingOrder']

    # h_batters = game_data['liveData']['boxscore']['teams']['home']['batters']
    # a_batters = game_data['liveData']['boxscore']['teams']['away']['batters']

    all_players = game_data['liveData']['players']['allPlayers']

    h_batter_data = [all_players['ID'+x] for x in h_batter_ids]
    h_batters = [
                 ' '.join([x['name']['first'],
                           x['name']['last']])
                 for x in h_batter_data
                ]

    a_batter_data = [all_players['ID'+x] for x in a_batter_ids]
    a_batters = [
                 ' '.join([x['name']['first'],
                           x['name']['last']])
                 for x in a_batter_data
                ]

    lineup_data = {
                   '{} Batters'.format(home) : h_batters,
                   '{} Batters'.format(away) : a_batters
                  }
    lineup_df = pd.DataFrame(lineup_data)

    # Find xml link to gamecenter data
    game_id = game_data['gameData']['game']['id']

    xml = 'http://gd2.mlb.com/components/game/mlb/year_{}/\
           month_{}/day_{}/gid_{}/gamecenter.xml'\
           .format(y, m, d, game_id)\
           .replace(' ', '')

    # Open xml link and parse pitchers and text blurbs
    # res = urlopen(xml).read()
    res = requests.get(xml).text
    dxml = xd.parse(res)
    home = dxml['game']['probables']['home']
    away = dxml['game']['probables']['away']

    # Pitcher names
    h_pit = ' '.join([home['useName'],
                      home['lastName'],
                      home['throwinghand']
                     ])

    a_pit = ' '.join([away['useName'],
                      away['lastName'],
                      away['throwinghand']
                     ])

    # Pitcher stats
    hp_win  = home['wins']
    hp_loss = home['losses']
    hp_era  = home['era']
    hp_so   = home['so']

    ap_win  = away['wins']
    ap_loss = away['losses']
    ap_era  = away['era']
    ap_so   = away['so']

    # Pitcher blurbs
    h_blurb = home['report']
    a_blurb = away['report']

    # Game blurb
    blurb = dxml['game']['previews']['mlb']['blurb']

    pit_df   = pd.DataFrame({
                             'Pitcher' : [h_pit, a_pit],
                             'Wins'    : [hp_win, ap_win],
                             'Losses'  : [hp_loss, ap_loss],
                             'ERA'     : [hp_era, ap_era],
                             'SO'      : [hp_so, ap_so],
                             'Blurb'   : [h_blurb, a_blurb]
                            })
    pit_df = pit_df[['Pitcher', 'Wins', 'Losses', 'ERA', 'SO', 'Blurb']]

    # Weather
    weather = game_data['gameData']['weather']
    weather_df = pd.DataFrame(weather, index=[0])

    # Write data to sheet
    write_to_sheet(df=sum_df,
                   sheet_name='Game Preview',
                   start_cell='A1',
                   clean=True)

    idx = sum_df.shape[0] + 2
    cell_idx = 'A{}'.format(idx)
    blurb_df = pd.DataFrame([blurb], columns=[' '])
    write_to_sheet(df=blurb_df,
                   sheet_name='Game Preview',
                   start_cell=cell_idx,
                   clean=False)

    idx += 3
    cell_idx = 'A{}'.format(idx)
    write_to_sheet(df=weather_df,
                   sheet_name='Game Preview',
                   start_cell=cell_idx,
                   clean=False)

    idx += 3
    cell_idx = 'A{}'.format(idx)
    write_to_sheet(df=pit_df,
                   sheet_name='Game Preview',
                   start_cell=cell_idx,
                   clean=False)

    idx += pit_df.shape[0] + 2
    cell_idx = 'A{}'.format(idx)
    write_to_sheet(df=lineup_df,
                   sheet_name='Game Preview',
                   start_cell=cell_idx,
                   clean=False)

#### TEST AREA
if __name__ == '__main__':
    # fanGraphs('bat', 'all', 2017)
    # fanGraphs('pit', 'all', 2017)
    # br_standings()
    # yankees_schedule()
    # pitching_logs('NYY', 2016)
    # # pitching_logs('NYY', 2017)
    # game_preview('mets')
    # forty_man()
    # current_injuries()
    # transactions('astros', 2017)
    # boxscore('yankees')
    pass

