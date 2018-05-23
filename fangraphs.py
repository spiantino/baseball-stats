import pandas
from bs4 import BeautifulSoup

def fangraphs(soup):
    # Extract column headers
    thead = soup.find('thead')
    cols = [x.text for x in thead.find_all('th')]

    # Extract stats from table bdoy
    tbody = soup.find_all('tbody')[-1]
    all_rows = tbody.find_all('tr')
    all_row_data = [x.find_all('td') for x in all_rows]

    db_rows = []
    for row in all_row_data:
        row_data = [x.text for x in row]
        db_data = {k:v for k,v in zip(cols, row_data)}

        # Store type as numeric if possible
        for key in db_data.keys():
            try:
                db_data[key] = float(db_data[key])
            except:
                continue
        
        db_rows.append(db_data)

    return {
        'leaderboard': pandas.DataFrame(db_rows)
    }

