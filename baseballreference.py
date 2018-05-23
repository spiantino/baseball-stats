import pandas
from bs4 import BeautifulSoup

def schedule(soup):
    table = soup.find('table', {'id' : 'team_schedule'})

    # Extract schedule columns
    thead = table.find('thead')
    cols = [x.text.replace('\xa0', 'Field').replace('.', '') for x in thead.find_all('th')]
    upcoming_cols = cols[:6] + ['Time']

    # Extract schedule data
    tbody = soup.find('tbody')
    trows = tbody.find_all('tr')

    # Throw out rows that are duplicates of column headers
    trows = [x for x in trows if 'Gm#' not in x.text]

    # Extract schedule data one row at a time
    db_rows = []
    for row in trows:
        row_data = [x.text for x in
                    row.find_all(lambda tag: tag.has_attr('data-stat'))]

        # Past game
        if row_data[2] == 'boxscore':
            game_num = row_data[0]
            db_data = {k:v for k,v in zip(cols, row_data)}

        # Upcoming game
        elif row_data[2] == 'preview':
            row_data = row_data[:7]
            game_num = row_data[0]
            db_data = {k:v for k,v in zip(upcoming_cols, row_data)}

        db_rows.append(db_data)

    return {
        'schedule': pandas.DataFrame(db_rows)
    }
