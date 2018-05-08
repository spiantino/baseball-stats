import scrape
import argparse
import datetime
import pandas as pd
from collections import defaultdict

from dbcontroller import DBController
from scrape import convert_name

if __name__ == '__main__':
    today = datetime.date.today().strftime('%Y-%m-%d')
    current_year = today.split('-')[0]

    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--team', default='NYY')
    parser.add_argument('-d', '--date', default=today)
    args = parser.parse_args()

    # Gather global leaderboard data
    print("Scraping batting leaderboard...")
    scrape.fangraphs(state='bat', year=current_year)

    print("Scraping pitching leaderboard...")
    scrape.fangraphs(state='pit', year=current_year)

    Gather game previews
    print("Gathering game preivews...")
    scrape.game_preview()

    # Create database controller object
    dbc = DBController()

    # Query upcomming game and populate data
    game = dbc.get_team_game_preview(team=args.team, date=args.date)

    try:
        game_data = list(game)[0]
    except:
        # throw error (no game found for that team/date)
        raise ValueError("NO GAME FOUND")

    game_state = data['preview'][0]['gameData']['status']['detailedState']

    def summary_table(data=game_data):
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

        away_pit_stats = dbc.get_player(away_pit_name)[current_year]
        home_pit_stats = dbc.get_player(home_pit_name)[current_year]

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

    def rosters(who, year='2018'):
        live_data = data['preview'][0]['liveData']['boxscore']

        if who == 'starters':
            away_batters = live_data['teams']['away']['battingOrder']
            home_batters = live_data['teams']['home']['battingOrder']
        elif who == 'bench':
            away_batters = live_data['teams']['away']['bench']
            home_batters = live_data['teams']['home']['bench']

        away_batter_ids = ['ID{}'.format(x) for x in away_batters]
        home_batter_ids = ['ID{}'.format(x) for x in home_batters]

        for idx, playerid in emumerate(away_batter_ids):
            order = idx + 1
            batter = live_data['teams']['away']['players'][playerid]
            name = ' '.join((batter['name']['first'], batter['name']['last']))
            pos = batter['position']
            num = batter['shirtNum']

            batstats = ['seasonStats']['batting']
            avg = batstats['avg']
            obp = batstats['obp']
            slg = batstats['slg']
            hrs = batstats['homeRuns']
            rbi = batstats['rbi']
            sb  = batstats['stolenBases']

            slashline = '{}/{}/{}'.format(avg, obp, slg)

            # Query WAR stats from Players collection
            war_stats = dbc.get_player_war(player=name,
                                           type='batter',
                                           year=year)
            war  = war_stats['war']
            off  = war_stats['off']
            def_ = war_stats['def']

            # DO THE SAME FOR HOME TEAM ^


    def bullpen(year='2018'):
        live_data = data['preview'][0]['liveData']['boxscore']

        away_bullpen = live_data['teams']['away']['bullpen']
        home_bullpen = live_data['teams']['home']['bullpen']

        away_bullpen_ids = ['ID{}'.format(x) for x in away_bullpen]
        home_bullpen_ids = ['ID{}'.format(x) for x in home_bullpen]

        for palyerid in away_bullpen_ids:
            pitch = live_data['teams']['away']['players'][playerid]
            name = ' '.join((pitch['name']['first'], pitch['name']['last']))
            num = pitch['name']['shirtNum']
            hits = pitch['seasonStats']['pitching']['hits']
            runs = pitch['seasonStats']['pitching']['runs']
            eruns = pitch['seasonStats']['pitching']['earnedRuns']
            strikeouts = pitchs['seasonStats']['pitching']['strikeOuts']

            # Query stats from Players collection
            pitstats = dbc.get_player(name)[year]

            war = pitstats['pit_WAR']
            sv  = pitstats['SV']
            era = pitstats['ERA']
            ip  = pitstats['ip']
            k9  = pitstas['K/9']
            bb9 = pitstats['BB/9']
            hr9 = pitstats['HR/9']
            gb  = pitstats['GB%']

    # def game_history(*teams):
    #     # Trigger scrape for each game where detailed state not "final" ?
    #     hist = list(dbc.get_matchup_history(*teams))

    #     df_data = []
    #     for game in hist:
    #         date = game['date']
    #     time = game['preview'][0]['gameData']['datetime']['time']
    #     tzone = game['preview'][0]['gameData']['datetime']['timeZone']
    #     game_time = ' '.join((time, tzone))


    ## Rosters


    ## Standings
    all_teams = dbc.get_all_teams()
    standings_cols = ['Tm', 'W', 'L', 'last10', 'gb',
                      'div', 'Strk', 'Home', 'Road', 'W-L%']
    standings = pd.DataFrame(all_teams)[standings_cols]
    standings = standings.sort_values(by='W-L%', ascending=False)

    # Find home/away win %
    def win_percent(x):
        w, l = x.split('-')
        total = int(w) + int(l)
        percent = float(w) / total
        return round(percent, 3)

    standings['home_rec'] = standings.Home.apply(lambda x: win_percent(x))
    standings['away_rec'] = standings.Road.apply(lambda x: win_percent(x))

    # Drop unused cols
    standings.drop('W-L%', axis=1, inplace=True)
    standings.drop('Home', axis=1, inplace=True)
    standings.drop('Road', axis=1, inplace=True)

    # Leaderboards
    bat_leaders = dbc.get_top_n_leaders(kind='bat',
                                        stat='WAR',
                                        year=current_year,
                                        n=10)

    pit_leaders = dbc.get_top_n_leaders(kind='pit',
                                        stat='WAR',
                                        year=current_year,
                                        n=10)

    hr_leaders = dbc.get_top_n_leaders(kind='home_run',
                                       stat='HR',
                                       year=current_year,
                                       n=10)

    def combine_dicts_in_list(list_of_dicts):
        data = defaultdict(list)
        for d in list_of_dicts:
            for k, v in d.items():
                data[k].append(v)
        return data

    bat_lb_data = combine_dicts_in_list(bat_leaders)
    pit_lb_data = combine_dicts_in_list(pit_leaders)
    hr_lb_data  = combine_dicts_in_list(hr_leaders)

    bat_df = pd.DataFrame(bat_lb_data).rename({'bat_rank' : 'Rank',
                                               'bat_WAR'  : 'WAR'}, axis=1)
    pit_df = pd.DataFrame(pit_lb_data).rename({'pit_rank' : 'Rank',
                                               'pit_WAR'  : 'WAR'}, axis=1)

    hr_df = pd.DataFrame(hr_lb_data)[['Name', 'Team', 'HR']]

    # Make slash line column for batting df
    def make_slash_line(*x):
        avg, obp, slg = x
        return '{}/{}/{}'.format(avg, obp, slg)

    bat_df['AVG/OBP/SLG'] = bat_df[['AVG', 'OBP', 'SLG']]\
                           .apply(lambda x: make_slash_line(*x), axis=1)

    bat_cols = ['Rank', 'Name', 'Team', 'WAR', 'AVG/OBP/SLG', 'HR',
                'RBI', 'SB', 'BB%', 'K%', 'BABIP', 'Off', 'Def']

    bat_df = bat_df[bat_cols]#.sort_values(by='Rank', ascending=True)

    pit_cols = ['Rank', 'Name', 'Team', 'WAR', 'W/L',
                'ERA', 'IP', 'K/9', 'BB/9', 'HR/9', 'GB%']

    def format_wl(*x):
        return '{}-{}'.format(x[0], x[1])

    pit_df['W/L'] = pit_df[['W', 'L']].apply(lambda x: format_wl(*x), axis=1)

    pit_df = pit_df[pit_cols]#.sort_values(by='Rank', ascending=True)

    bat_df['Rank'] = bat_df.index + 1
    pit_df['Rank'] = pit_df.index + 1

    print(bat_df)
    print(pit_df)
    print(hr_df)

    # Table of relievers
    relievers = dbc.get_top_n_pitchers(kind='reliever',
                                       year=current_year,
                                       by='pit_WAR',
                                       ascending=False,
                                       n=10)
    reliever_cols = ['Rank', 'Name', 'Team', 'WAR']

    reliever_data = defaultdict(list)
    for reliever in relievers:
        for k, v, in reliever.items():
            reliever_data[k].append(v)

    rdf = pd.DataFrame(reliever_data).rename({'pit_WAR' : 'WAR'}, axis=1)
    rdf['Rank'] = rdf.index + 1
    rdf = rdf[reliever_cols]
    print(rdf)

    # Top pitchers sorted by ERA
    top_era_pitchers = dbc.get_top_n_pitchers(kind='starter',
                                              year=current_year,
                                              by='ERA',
                                              ascending=True,
                                              n=10)

    top_era_data = combine_dicts_in_list(top_era_pitchers)
    top_era_df = pd.DataFrame(top_era_data)[['Name', 'Team', 'ERA']]
    print(top_era_df)

    # League ELO
    elo_cols = ['Team', 'Rating', 'Division%', 'Playoff%', 'WorldSeries%']
    elo_stats = dbc.get_elo_stats()
    elo_data = combine_dicts_in_list(elo_stats)
    elo_df = pd.DataFrame(elo_data)
    elo_df = elo_df[elo_cols].sort_values(by='Rating', ascending=False)
    print(elo_df)


