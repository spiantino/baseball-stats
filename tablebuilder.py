import requests
import json
import pandas as pd
import datetime
from unidecode import unidecode

from dbclasses import Player, Game, Team
from utils import (get_stadium_location,
                   get_last_week_dates,
                   subtract_dates,
                   normalize_name)

class TableBuilder:
    def __init__(self, game):
        self.game = game
        self._year = self.game._date.split('-')[0]

    def summary_info(self):
        details = self.game._game_details

        # Logos
        home = details['homeAbbr']
        away = details['awayAbbr']

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
        temp = round(weather['currently']['temperature'])
        wind_speed = str(weather['currently']['windSpeed'])
        wind = wind_speed + 'mph' + details['windDir'].split('mph')[-1]

        return {'home' : home,
                'away' : away,
                'game' : game_num,
                'title' : title,
                'details' : time_and_place,
                'condition' : condition,
                'temp' : temp,
                'wind' : wind}

    def starting_pitchers(self):
        pitchers = self.game._pitchers
        stats = ['Team', 'R/L', '#', 'Name', 'WAR', 'W', 'L',
                 'ERA', 'IP', 'K/9', 'BB/9', 'HR/9', 'WHIP', 'GB%']

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

        df = pd.DataFrame([away_pit_data, home_pit_data])[stats]
        df = df.fillna('-')
        return df

    def make_slash_line(self, *columns):
        stats = []
        for stat in columns:
            if isinstance(stat, float):
                stat = '{:.3f}'.format(stat).lstrip('0')
            stats.append(stat)
        return '/'.join(stats).replace('nan', '-')

    def rosters(self, who='starters'):
        year = self._year
        batters = self.game._batters

        def extract_data(side):
            stats = ['AVG', 'OBP', 'SLG', 'HR', 'RBI',
                     'SB', 'WAR', 'Off', 'Def']
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
            cols = ['Order', 'Position', 'Number', 'Name', 'WAR',
                    'Slash', 'HR', 'RBI', 'SB', 'Off', 'Def']
            df = pd.DataFrame(data)

            df['Order'] = df.index + 1
            df['Slash'] = df[['AVG', 'OBP', 'SLG']]\
                            .apply(lambda x: self.make_slash_line(*x), axis=1)

            df = df[cols]

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
            away_bench_df = pd.DataFrame()
            home_bench_df = pd.DataFrame()

        return ((away_starter_df, home_starter_df),
                (away_bench_df, home_bench_df))

    def bullpen(self):
        if self.game._state == 'Scheduled':
            return (pd.DataFrame(), pd.DataFrame())

        bullpen = self.game._bullpen

        def construct_table(side):
            stats = ['WAR', 'SV', 'ERA', 'IP', 'K/9',
                     'BB/9', 'HR/9', 'WHIP', 'GB%']

            df_data = []
            for pitcher, vals in bullpen[side].items():
                decoded = unidecode(pitcher)
                p = Player(name=decoded)

                pstats = p.get_stats(stats, pos='pit')
                pstats.update({'Name' : pitcher})
                pstats.update({'#' : vals['number']})

                # Find days since last active
                today = datetime.date.today().strftime('%Y-%m-%d')
                team = self.game._game[side]
                last = self.game.find_pitch_dates(decoded, team, n=1)
                if last:
                    days = subtract_dates(today, last[0])
                else:
                    days = None

                pstats.update({'Days' : days})

                df_data.append(pstats)

            cols = ['Name', '#'] + stats + ['Days']
            df = pd.DataFrame(df_data)[cols]

            df = df.sort_values(by='IP', ascending=False)
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

    def game_history(self):

        def construct_table(side):
            team_name = self.game._game[side]
            t = Team(name=team_name)
            sched = t.get_stat('Schedule')

            cols = ['Date', 'Time', 'Opp', 'Result', 'Score', 'GB']
            df = pd.DataFrame(sched)

            # Remove upcoming games
            df = df.loc[df[''] == 'boxscore']

            df['Opp'] = df[['Field', 'Opp']].apply(lambda x: ' '
                                            .join((x[0], x[1]))
                                            .strip(), axis=1)

            df['Score'] = df[['R', 'RA']].apply(lambda x: '{}-{}'
                                         .format(int(x[0]),int(x[1])), axis=1)

            def format_date(x):
                _, m, d = x.split()[:3]
                df_date = '{} {}'.format(m, d)
                dt_date = datetime.datetime.strptime(df_date, '%b %d')
                return dt_date.strftime("%m-%d")

            df['Date'] = df.Date.apply(lambda x: format_date(x))

            df = df.rename({'W/L' : 'Result'}, axis=1)
            df = df[cols]
            return df

        home_df = construct_table('home')
        away_df = construct_table('away')
        return (home_df, away_df)

    def bat_leaders(self, stat, n=10):
        p = Player()
        leaders = p.get_top_n_leaders(pos='bat',
                                      stat=stat,
                                      year=self._year,
                                      n=n)

        df = pd.DataFrame(leaders)

        df['Slash'] = df[['AVG', 'OBP', 'SLG']]\
                        .apply(lambda x: self.make_slash_line(*x), axis=1)

        df['Rank'] = df.index + 1

        if stat in ['HR', 'RBI']:
            cols = ['Rank'] + ['Name', 'Team'] + [stat]
        else:
            cols = ['Rank', 'Name', 'Team', 'WAR',
                    'Slash', 'HR', 'RBI', 'SB', 'BB%',
                    'K%', 'BABIP', 'Off', 'Def']
        return df[cols]

    def pit_leaders(self, stat, n=10, role='starter'):
        p = Player()

        if stat == 'WAR':
            ascending = False
        else:
            ascending = True

        leaders = p.get_top_n_pitchers(stat=stat,
                                       role=role,
                                       year=self._year,
                                       ascending=ascending,
                                       n=n)
        df = pd.DataFrame(leaders)

        def format_wl(*x):
            return '{}-{}'.format(x[0], x[1])

        df['W/L'] = df[['W', 'L']].apply(lambda x: format_wl(*x), axis=1)

        cols = ['Rank', 'Name', 'Team', 'WAR', 'W', 'L', 'ERA',
                'IP', 'K/9', 'BB/9', 'HR/9', 'WHIP', 'GB%']

        df['Rank'] = df.index + 1

        return df[cols]

    def elo(self):
        t = Team()
        data = list(t.get_elo_stats())
        df = pd.DataFrame(data)

        df = df.sort_values(by='Rating', ascending=False)

        # Round rating column
        df['Rating'] = df.Rating.round().astype(int)

        # Make Rank column
        df['Rank'] = df.Rating.rank(ascending=False).astype(int)

        # Format columns with percantages
        perc_cols = ['Division%', 'Playoff%', 'WorldSeries%']
        for col in perc_cols:
            df[col] = df[col].apply(lambda x: "{:.1%}".format(round(x, 3)))

        cols = ['Rank', 'Rating',
                'Team', 'Division%',
                'Playoff%', 'WorldSeries%']

        return df[cols]

    def pitcher_history(self, last_n=10):
        team = self.game._team
        t = Team(name=team)

        dates = t.get_past_game_dates(last_n=last_n)

        last_date = None
        data = []
        for date in dates:

            # Check if game is a double header
            idx = 1 if date == last_date else 0

            g = Game()
            g.query_game_preview_by_date(team=team, date=date, idx=idx)
            g.parse_all()

            # Skip if current game is not finalized
            if g._state not in ['Final', 'Game Over', 'Completed Early']:
                continue

            pitch_stats = g._pitcher_gamestats[g._side]

            short_date = datetime.datetime.strptime(date, '%Y-%m-%d')\
                                          .strftime('%m/%d %a')

            pitch_stats.update({'date' : short_date})
            pitch_stats.update({'opp' : g._opp})
            data.append(pitch_stats)

            last_date = date

        cols = ['date',  'opp', 'name', 'ipit', 'hrun',
                'runs', 'erun', 'walk', 'strk', 'gsc']

        df = pd.DataFrame(data)[cols]
        df = df.sort_values(by='date', ascending=False)
        return df

    def previous_week_bullpen(self):
        team = self.game._team
        side = self.game._side
        t = Team(name=team)

        last_week = get_last_week_dates()
        schedule  = t.get_past_game_dates(last_n=20)

        dates = [date for date in schedule if date in last_week]

        # dates = sorted(list(last_week.intersection(schedule)), reverse=True)

        last_date = None
        data = {}
        for date in schedule:

            # Check if game is a double header
            idx = 1 if date == last_date else 0

            g = Game()
            g.query_game_preview_by_date(team=team, date=date, idx=idx)
            g.parse_all()

            # Skip if current game is not finalized
            if g._state not in ['Final', 'Game Over', 'Completed Early']:
                continue

            pit_data = g._br_pit_data[side]
            data_strs = []
            for pitcher in pit_data.keys():
                if pit_data[pitcher]['entered'] > 1:
                    data_str = '{} - Entered: {} Pitch count: {} WPA: {}'\
                               .format(pitcher,
                                       pit_data[pitcher]['entered'],
                                       pit_data[pitcher]['pit'],
                                       pit_data[pitcher]['wpa'])
                    data_strs.append(data_str)

            # Account for double headers
            if date not in data.keys():
                data[date] = data_strs
                game_date = date
            else:
                game1_date = '{} (1)'.format(date)
                data[game1_date] = data[date]

                data.pop(date)

                game2_date = '{} (2)'.format(date)
                data[game2_date] = data_strs

                game_date = game2_date

            #Find the rest of the bullpen that did not play
            bullpen = g._bullpen[side]
            data[game_date] += bullpen.keys()

            # Store last seen date
            last_date = date

        # Add padding so array lengths match
        max_ = max([len(data[k]) for k in data.keys()])
        for k in data.keys():
            pad_len = max_ - len(data[k])
            padding = [None for _ in range(pad_len)]
            data[k].extend(padding)

        df = pd.DataFrame(data)

        return df

    def series_results(self, team_name=None):
        """
        Table with date, time, score, starter,
        ip, game score, for the current series
        """
        team = self.game._team if team_name == None else team_name
        t = Team(name=team)

        all_game_dates = t.get_past_game_dates(last_n=-1)

        today = datetime.date.today().strftime('%Y-%m-%d')
        last_opp,  last_date = None, None
        data = []
        for date in all_game_dates:

            # Check if game is a double header
            idx = 1 if date == last_date else 0

            g = Game()
            g.query_game_preview_by_date(team=team, date=date, idx=idx)
            g.parse_all()

            # Skip over today's game
            if date == today and g._state not in ['Final', 'Game Over']:
                continue

            # Stop collecting data when opponent changes
            if last_opp and last_opp != g._opp:
                break

            game_time = g._game_details['gameTime']
            am_or_pm  = g._game_details['am_or_pm']
            time = '{}{}'.format(game_time, am_or_pm)

            home_abbr = g._game_details['homeAbbr']
            away_abbr = g._game_details['awayAbbr']

            away_score = g._game_details['awayScore']
            home_score = g._game_details['homeScore']

            score = '{}-{}'.format(home_score, away_score)

            home_starter = unidecode(g._pitchers['home']['name'])
            away_starter = unidecode(g._pitchers['away']['name'])

            def extract_pit_data(side, starter):
                """
                If there is a name mismatch between mlb and br data,
                try normalizing the name first, then try returning
                the pitcher that entered in inning 1
                """
                # try:
                #     return g._br_pit_data[side][starter]
                # except:
                #     try:
                #         name = normalize_name(starter)
                #         return g._br_pit_data[side][name]
                #     except:
                #         try:
                #             data = g._br_pit_data[side]
                #             return [player for player in data.keys()
                #                            if data[player]['entered'] == 1][0]
                #         except:
                #             return {'ip' : None, 'gsc' : None}
                try:
                    return g._br_pit_data[side][starter]
                except:
                    pass

                try:
                    name = normalize_name(starter)
                    return g._br_pit_data[side][name]
                except:
                    pass

                try:
                    data = g._br_pit_data[side]
                    return [player for player in data.keys()
                                   if data[player]['entered'] == 1][0]
                except:
                    return {'ip' : None, 'gsc' : None}


            home_pit_data = extract_pit_data('home', home_starter)
            away_pit_data = extract_pit_data('away', away_starter)

            home_ip = home_pit_data['ip']
            away_ip = away_pit_data['ip']

            home_gsc = home_pit_data['gsc']
            away_gsc = away_pit_data['gsc']

            data.append([date, time, home_abbr,
                         away_abbr, score,
                         home_starter, home_ip,
                         home_gsc, away_starter,
                         away_ip, away_gsc])

            last_opp  = g._opp
            last_date = date

        cols = ['date', 'time', 'home', 'away', 'score',
                'home starter', 'home ip', 'home gs',
                'away starter', 'away ip', 'away gs']
        df = pd.DataFrame(data, columns=cols)
        return df

    def games_behind(self, *teams):
        dfs = []
        for team in teams:
            t = Team(name=team)

            gb = t.get_games_behind_history()
            df = pd.DataFrame(gb)

            def format_date(x):
                m, d = x.split()[1], x.split()[2]
                date = '{} {} {}'.format(m, d, self.game._year)
                dt_date = datetime.datetime.strptime(date, '%b %d %Y')
                return dt_date.strftime("%m-%d-%Y")

            df['Date'] = df.Date.apply(lambda x: format_date(x))

            # Drop duplicate rows (double headers)
            df = df.drop_duplicates()

            # Fill missing dates
            df = df.set_index('Date')
            start, end = df.index[0], df.index[-1]
            idx = pd.date_range(start, end).strftime('%m-%d-%Y')
            df = df.reindex(idx, fill_value=None)
            df = df.fillna(method='ffill')
            df = df.reset_index().rename(columns={'index' : 'Date'})

            df['GB'] = df.GB.apply(lambda x: 0 if isinstance(x, str) else x)
            dfs.append(df)

        return dfs




