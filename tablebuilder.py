import requests
import json
import pandas as pd
from unidecode import unidecode

from dbclasses import Player, Game, Team
from utils import get_stadium_location

class TableBuilder:
    def __init__(self, game_obj):
        self.game = game_obj
        self._year = self.game._date.split('-')[0]
        # self._game = Game()
        # self._game.query_game_preview_by_date(team, date)
        # self._game.parse_all()

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
        pitchers = self.game._starting_pitchers

        away_decoded = unidecode(pitchers['away']['name'])
        home_decoded = unidecode(pitchers['home']['name'])

        away_pitcher = Player(name=away_decoded)
        home_pitcher = Player(name=home_decoded)

        cols = ['Team', 'R/L', '#', 'Name', 'pit_WAR', 'W',
                'L', 'ERA', 'IP', 'K/9', 'BB/9', 'HR/9', 'GB%']

        home_pit_data, away_pit_data = {}, {}
        for stat in cols:
            home_val = home_pitcher.get_stat('fg', self._year, 'pit', stat)
            away_val = away_pitcher.get_stat('fg', self._year, 'pit', stat)

            home_pit_data.update({stat : home_val})
            away_pit_data.update({stat : away_val})

        home_pit_data['R/L'] = pitchers['home']['hand']
        away_pit_data['R/L'] = pitchers['away']['hand']

        home_pit_data['#'] = pitchers['home']['num']
        away_pit_data['#'] = pitchers['away']['num']

        df = pd.DataFrame([away_pit_data, home_pit_data])
        df = df[cols].rename({'pit_WAR' : 'WAR'}, axis='columns')
        return df
