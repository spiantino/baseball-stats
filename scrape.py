from bs4 import BeautifulSoup, Comment
from urllib.request import urlopen
import pandas as pd
import re
import numpy as np

def openUrl(url):
    page = urlopen(url)
    return BeautifulSoup(page.read(), "html.parser")

def fanGraphs(type_, team):
    """
    Scrape data from fangraphs.com
    """
    # Turn args into acceptable parameters
    if type_ == 'batting':
        type_ = 'bat'

    elif type_ in ['pitch', 'pitching']:
        type_ = 'pit'

    team_id = {'angels': 1,
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

    print(df)

def baseball_reference():
    """
    Scrape MLB standings or Yankees schedule
    from baseball-reference.com
    """
    url = "http://www.baseball-reference.com/leagues/MLB-standings.shtml"
    soup = openUrl(url)

    h2 = soup.find_all('h2')
    headers = [x.string for x in h2[:6]]
    # print(h2[6:])

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
    comment=soup.find_all(string=lambda text: isinstance(text,Comment))
    comment_html = [x for x in comment if '<td' in x][-1].string

    table = BeautifulSoup(comment_html, "html.parser")

    # Locate table and extract data
    data=[]
    for item in table.find_all(lambda tag: tag.has_attr("data-stat")):
        data.append(item.string)

    # Construct league standings dataframe
    data = np.array(data).reshape([-1, 27])
    cols, dfdata = data[:1].reshape(-1,), data[1:]
    standings = pd.DataFrame(dfdata, columns=cols)

    # Save output for testing
    df.to_csv('test.csv', index=False)
    standings.to_csv('standings.csv', index=False)





#### TEST AREA
if __name__ == '__main__':
    # fanGraphs('bat', 'astros')
    # baseball_reference()



    # fanUrl = """http://www.fangraphs.com/leaders.aspx?pos=all&stats=pit&lg=all&qual=0&type=8&season=2017&month=0&season1=2017&ind=0&team={}&rost=&age=&filter=&players=""".format(9)

    # soup = openUrl(fanUrl)

    # table = soup.find('tbody')

    # names = [x.string for x in table.find_all('a')]
    # # g = table.fi

    # # print(names)
    # # print(table.find('tr'))



    # print(df)



