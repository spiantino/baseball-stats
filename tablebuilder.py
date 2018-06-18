import requests
import json
import pandas as pd
from unidecode import unidecode

from dbclasses import Player, Game, Team
from utils import get_stadium_location

class TableBuilder:
    def __init__(self, game):
        self.game = game
        self._year = self.game._date.split('-')[0]

    def summary_info(self):
        details = self.game._game_details

        if self.game._side == 'home':
            wins = int(details['homeWins'])
            loss = int(details['homeLoss'])

        elif self.game._side == 'away':
            wins = int(details['awayWins'])
            loss = int(details['awayLoss'])

        game_num = wins + loss + 1

        title = "{} ({}-{}) @ {} ({}-{})".format(details['awayName'],
                                                 details['awayWins'],
                                                 details['awayLoss'],
                                                 details['homeName'],
                                                 details['homeWins'],
                                                 details['homeLoss'])

        time_and_place ='{}{} {}'.format(details['gameTime'],
                                         details['am_or_pm'],
                                         details['stadium'] )

        # Forecast from darsky.net api
        key = 'fb8f30e533bb7ae1a9a26b0ff68a0ed8'
        loc  = get_stadium_location(details['homeAbbr'])
        lat, lon = loc['lat'], loc['lon']
        url = 'https://api.darksky.net/forecast/{}/{},{}'.format(key,lat,lon)
        weather = json.loads(requests.get(url).text)

        condition = weather['currently']['summary']
        temp = weather['currently']['temperature']
        wind_speed = str(weather['currently']['windSpeed'])
        wind = wind_speed + 'mph ' + details['windDir']

        return {'game' : game_num,
                'title' : title,
                'details' : time_and_place,
                'condition' : condition,
                'temp' : temp,
                'wind' : wind}

    def starting_pitchers(self):
        pitchers = self.game._pitchers
        stats = ['Team', 'R/L', '#', 'Name', 'pit_WAR', 'W',
                 'L', 'ERA', 'IP', 'K/9', 'BB/9', 'HR/9', 'GB%']

        def extract_data(side):
            decoded = unidecode(pitchers[side]['name'])
            pitcher = Player(name=decoded)
            pit_data = pitcher.get_stats(stats=stats, pos='pit')

            pit_data['Name'] = pitchers[side]['name']
            pit_data['Team'] = pitchers[side]['team']
            pit_data['R/L'] = pitchers[side]['hand']
            pit_data['#'] = pitchers[side]['num']
            return pit_data

        away_pit_data = extract_data('away')
        home_pit_data = extract_data('home')

        df = pd.DataFrame([away_pit_data, home_pit_data])
        df = df[stats].rename({'pit_WAR' : 'WAR'}, axis='columns')
        df = df.fillna('-')
        return df

    def rosters(self, who='starters'):
        year = self._year
        batters = self.game._batters

        def extract_data(side):
            stats = ['AVG', 'OBP', 'SLG', 'HR', 'RBI',
                     'SB', 'bat_WAR', 'Off', 'Def']
            bat_data = []
            for pdata in batters[side]:
                decoded = unidecode(pdata['Name'])
                batter = Player(name=decoded)
                pstats = batter.get_stats(stats)

                tmp = pdata.copy()
                tmp.update(pstats)
                bat_data.append(tmp)

            return bat_data

        def construct_table(data):
            cols = ['Order', 'Position', 'Number', 'Name', 'bat_WAR',
                    'Slash Line', 'HR', 'RBI', 'SB', 'Off', 'Def']
            df = pd.DataFrame(data)

            df['Order'] = df.index + 1

            def make_slash_line(*x):
                stats = []
                for stat in x:
                    if isinstance(stat, float):
                        stat = '{:.3f}'.format(stat).lstrip('0')
                    stats.append(stat)
                return '/'.join(stats).replace('nan', '-')

            df['Slash Line'] = df[['AVG', 'OBP', 'SLG']]\
                                 .apply(lambda x: make_slash_line(*x), axis=1)

            df = df[cols]
            df = df.rename(columns={'bat_WAR' : 'WAR',
                                    'Off' : 'Off WAR',
                                    'Def' : 'Def WAR'})

            if self.game._state == 'Scheduled':
                df = df.loc[df['Position'] != 'P']
                df = df.sort_values(by='WAR', ascending=False)
                df = df.drop(columns='Order')

            df = df.fillna(value='-')
            return df

        away_starter_data = extract_data('away_batters')
        home_starter_data = extract_data('home_batters')

        away_starter_df = construct_table(away_starter_data)
        home_starter_df = construct_table(home_starter_data)

        if batters['home_bench']:
            away_bench_data = extract_data('away_bench')
            home_bench_data = extract_data('home_bench')

            away_bench_df = construct_table(away_bench_data)
            home_bench_df = construct_table(home_bench_data)
        else:
            away_bench_df = None
            home_bench_df = None

        return ((away_starter_df, home_starter_df),
                (away_bench_df, home_bench_df))

    def bullpen(self):
        bullpen = self.game._bullpen

        def construct_table(side):
            stats = ['pit_WAR', 'SV', 'ERA', 'IP',
                     'K/9', 'BB/9', 'HR/9', 'GB%']

            df_data = []
            for pitcher, vals in bullpen[side].items():
                decoded = unidecode(pitcher)
                p = Player(name=decoded)
                pstats = p.get_stats(stats, pos='pit')
                pstats.update({'Name' : pitcher})
                pstats.update({'#' : vals['number']})
                df_data.append(pstats)

            cols = ['Name', '#'] + stats
            df = pd.DataFrame(df_data)[cols]

            df = df.fillna(value='-')
            return df

        home_df = construct_table('home')
        away_df = construct_table('away')

        return(home_df, away_df)

    def standings(self):
        stats = ['Tm', 'W', 'L', 'last10', 'gb',
                 'div', 'Strk', 'Home', 'Road', 'W-L%']

        def extract_data(side):
            team_name = self.game._game[side]
            team = Team(name=team_name)
            div = team.get_team_division()
            teams = team.get_teams_by_division(div)

            df_data = []
            for name in teams:
                t = Team(name=name)
                tstats = t.get_stats(stats)
                df_data.append(tstats)

            return df_data

        def construct_table(data):
            df = pd.DataFrame(data)[stats]
            df = df.sort_values(by='W-L%', ascending=False)

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

        home_data = extract_data('home')
        away_data = extract_data('away')

        home_df = construct_table(home_data)
        away_df = construct_table(away_data)

        return (home_df, away_df)

