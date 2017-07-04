from bs4 import BeautifulSoup, Comment
from urllib.request import urlopen
import pandas as pd
import re
import numpy as np
import json
import datetime
from df2gspread import df2gspread as d2g
#'1aoVUZE3dAFEVQDWbOY9YqLNSw5cvJr4l4i16a3FM-aQ'

def openUrl(url):
    page = urlopen(url)
    return BeautifulSoup(page.read(), "html.parser")

def write_to_sheet(df, sheet_name):
    print("writing {} to sheet".format(sheet_name))
    df = df.fillna('')
    d2g.upload( df,
               '1aoVUZE3dAFEVQDWbOY9YqLNSw5cvJr4l4i16a3FM-aQ',
                sheet_name,
                row_names=False
                )

def fanGraphs(type_, team):
    """
    Scrape data from fangraphs.com
    """
    # Turn args into acceptable parameters
    if type_ == 'batting':
        type_ = 'bat'

    elif type_ in ['pitch', 'pitching']:
        type_ = 'pit'

    team_id = {
               'all': 0,
               'angels': 1,
               'astros': 21,
               'athletics': 10,
               'blue jays': 14,
               'braves': 16,
               'brewers': 23,
               'cardinals': 28,
               'cubs': 17,
               'diamondbacks': 15,
               'dodgers': 22,
               'giants': 30,
               'indians': 5,
               'mariners': 11,
               'marlins': 20,
               'mets': 25,
               'nationals': 24,
               'orioles': 2,
               'padres': 29,
               'phillies': 26,
               'pirates': 27,
               'rangers': 13,
               'rays': 12,
               'red sox': 3,
               'reds': 18,
               'rockies': 19,
               'royals': 7,
               'tigers': 6,
               'twins': 8,
               'white sox': 4,
               'yankees': 9,
               }

    team = team.lower()
    tid = team_id[team]

    url = """http://www.fangraphs.com/leaders.aspx?pos=all&stats={}&lg=all&qual=0&type=8&season=2017&month=0&season1=2017&ind=0&team={}&rost=&age=&filter=&players=""".format(type_, tid)

    # Store url HTML as a bs4 object
    soup = openUrl(url)

    # Extract HTML from stat table element
    table = soup.find('tbody')

    # Extract column names
    cols = []
    th = soup.find_all('th')
    for t in th:
        cols.append(t.string)

    # Extract rows from table
    trs = table.find_all('tr')#[:-1]
    stats = []
    for tr in trs:
        tds = tr.find_all('td')
        row = [td.string for td in tds]
        stats.append(row)

    df = pd.DataFrame(stats, columns=cols)

    # df.to_csv('nyy-pitching-stats.csv', index=False)
    print(df)

def br_standings():
    """
    Scrape MLB standings or Yankees schedule
    from baseball-reference.com
    """
    url = "http://www.baseball-reference.com/leagues/MLB-standings.shtml"
    soup = openUrl(url)

    h2 = soup.find_all('h2')
    headers = [x.string for x in h2[:6]]

    # Scrape division standings
    tables = soup.find_all('div', attrs={'class':'table_outer_container'})
    df = pd.DataFrame()
    for i, table in enumerate(tables):

        # Extract all tables and relevant stats
        trs = table.find_all('tr')
        data = [th.string for tr in trs for th in tr if th.string != '\n']

        # Construct data frame
        cols, stats = data[:5], data[5:]
        stats = np.array(stats).reshape([-1, 5])

        # Add blank row as divider
        if not df.empty:
            blanks = [None for _ in range(len(cols))]
            df = df.append(pd.Series(blanks, index=cols), ignore_index=True)

        # Made division header
        df = df.append(pd.Series([headers[i], None, None, None, None],
                                 index=cols), ignore_index=True)

        # Make division dataframe
        df2 = pd.DataFrame(stats, columns=cols)

        # Concat division df to master df
        df = pd.concat([df, df2])
        df = df[cols]

        # division = headers[i]
        # cols = pd.MultiIndex.from_tuples([(division, x) for x in cols])

        # df2 = pd.DataFrame(stats, columns=cols)
        # df = pd.concat([df.T, df2.T])
        # df = df.T

        if i+1 == len(headers):
            break

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

    # Save output for testing
    df.to_csv('division-standings.csv', index=False)
    standings.to_csv('league-standings.csv', index=False)

def yankees_schedule():
    """
    Scrape yankees schedule with results
    from baseball-reference.com
    """
    url = "http://www.baseball-reference.com/teams/NYY/2017-schedule-scores.shtml"

    soup = openUrl(url)

    table = soup.find('div', {'class': 'table_outer_container'})
    ths = table.find_all('th')

    data = []
    for item in table.find_all(lambda tag: tag.has_attr('data-stat')):
        data.append(item.text)

    # for item in table.find_all('td',  {'data-stat': 'date_game'}):
    #     print(item)
    #     print(item.text)
    #     print()

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

    # df_ap.to_csv('nyy-played2.csv', index=False)
    # df_up.to_csv('nyy-upcoming.csv', index=False)
    write_to_sheet(df=df_ap, sheet_name='schedule-played')
    write_to_sheet(df=df_up, sheet_name='schedule-upcoming')


def pitching_logs(team, year):
    """
    Scrape pitching logs from
    baseball-reference.com
    """
    team = team.upper()
    url = "http://www.baseball-reference.com/teams/tgl.cgi?team={}&t=p&year={}".format(team, year)

    soup = openUrl(url)

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

    # df.to_csv('pitching-logs2.csv', index=False)
    wirte_to_sheet(df=df, sheet_name='pitching=logs')

def forty_man():
    """
    Extract 40-man roster from
    baseball-reference.com
    """
    url = "http://www.baseball-reference.com/teams/NYY/2017-roster.shtml"

    soup = openUrl(url)
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
    write_to_sheet(df=df, sheet_name='40-man roster')

def current_injuries():
    """
    Extract current injuries table
    from baseball-reference.com
    """
    url = "http://www.baseball-reference.com/teams/NYY/2017.shtml"

    soup = openUrl(url)

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
    write_to_sheet(df=df, sheet_name='current injuries')

def transactions(year):
    """
    Extract transations from
    http://mlb.mlb.com/mlb/transactions
    """
    if year == 2017:
        today = datetime.date.today().strftime('%Y%m%d')
        url = "http://mlb.mlb.com/lookup/json/named.transaction_all.bam?start_date=20170101&end_date={}&team_id=147".format(today)
    else:
        url = "http://mlb.mlb.com/lookup/json/named.transaction_all.bam?start_date={0}0101&end_date={0}1231&team_id=147".format(year)


    team_id = {
               'angels': 108,
               'astros': 117,
               'athletics': 133,
               'blue jays': 141,
               'braves': 144,
               'brewers': 158,
               'cardinals': 138,
               'cubs': ,
               'diamondbacks': ,
               'dodgers': ,
               'giants': ,
               'indians': ,
               'mariners': ,
               'marlins': ,
               'mets': ,
               'nationals': ,
               'orioles': ,
               'padres': ,
               'phillies': ,
               'pirates': ,
               'rangers': ,
               'rays': ,
               'red sox': ,
               'reds': ,
               'rockies': ,
               'royals': ,
               'tigers': ,
               'twins': ,
               'white sox': ,
               'yankees': ,
               }


    # Open and read json object
    res = urlopen(url).read()
    j = json.loads(res.decode('utf-8'))

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

    team = 'nyy'   # placeholder
    sheet_name = '{}-transactions-{}'.format(team, year)
    write_to_sheet(df=df, sheet_name=sheet_name)



def game_preview():
    """
    Collect data on upcomming game
    from mlb.com/gameday
    """
    url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=07/04/2017&sortBy=gameDate&hydrate=linescore(runners),flags,team,review"

    res = urlopen(url).read()
    j = json.loads(res.decode('utf-8'))

    data = j['dates'][0]['games']

    for game in data:
        if game['status']['detailedState'] == 'Scheduled':
            game_url = "https://statsapi.mlb.com" + game['link']
            content_url = game['content']['link']

            away = game['teams']['away']['team']['name']
            home = game['teams']['home']['team']['name']

            res = urlopen(game_url).read()
            jgame = json.loads(res.decode('utf-8'))

            # Extract starting pitcher data
            pitchers = jgame['gameData']['probablePitchers']
            a_pit = pitchers['away']['fullName']
            h_pit = pitchers['home']['fullName']

            a_pit_name = ' '.join(reversed(a_pit.split(','))).strip()
            h_pit_name = ' '.join(reversed(h_pit.split(','))).strip()

            # Get weather
            weather = jgame['gameData']['weather']
            if weather:
                weather = "{} degrees, {}, wind: {}"\
                          .format(weather['temp'],
                                  weather['condition'],
                                  weather['wind'])
            else:
                weather = None

            # playerData = jgame['gameData']['players']
            # print(jgame['liveData']['boxscore']['teams']['home'].keys())
            test = []
            for val in jgame['liveData']['boxscore'] \
                          ['teams']['away']['players'].values() :

                if val['status']['description'] == 'Active':
                    player = val['person']['fullName']
                    test.append(player)
            print(sorted(test))
            # for k in playerData.keys():
                # print(playerData[k]['firstLastName'], playerData[k]['active'])

        break

    # hrefs = [a.string for a in soup.find_all('a', href=True)]
    # print(hrefs)
    # h5 = [x for x in soup.find_all('h5')]
    # print(h5)





#### TEST AREA
if __name__ == '__main__':
    # fanGraphs('pit', 'yankees')
    # fanGraphs('pit', 'all')
    # br_standings()
    # yankees_schedule()
    # pitching_logs('NYY', 2017)
    # game_preview()
    # forty_man()
    # current_injuries()
    transactions(2016)



