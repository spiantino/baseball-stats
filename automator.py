import subprocess
import datetime
import pytz
import sched
import time

from dbclasses import Game
import main

class Automator:
    def __init__(self, teams=['NYY', 'NYM', 'HOU']):
        self.teams = teams
        self.sched = sched.scheduler(time.time, time.sleep)
        self.priority = 1
        self.today = datetime.datetime.today().strftime('%Y-%m-%d')
        self.game = Game()
        self.tasks_completed = False
        self.new_day = False

    def scrape_games(self):
        print("scraping today's games...")
        main.scrape_games()

    def schedule_scrape(self):
        if not self.game.todays_games_in_db():
            self.sched.enter(3600, self.priority, self.scrape_games)
            self.sched.run()

    def find_start_times(self):
        all_times = self.game.get_all_start_times()
        self.times = {team : time for team, time in all_times.items()
                                                  if team in self.teams}

        self.execute_times = {team : time - datetime.timedelta(minutes=50)
                                      for team, time in self.times.items()}

    def store_current_time(self):
        now = datetime.datetime.now()
        self.now = now.astimezone(pytz.timezone('America/New_York'))

    def get_delays(self):
        # 1 second for testing:
        # self.delays = {team : 1 for team in self.execute_times.keys()}

        self.delays = {team : (self.now - time).total_seconds()
                                for team, time in self.execute_times.items()}

    def schedule_tasks(self):
        for team, delay in self.delays.items():
            self.sched.enter(delay,
                             self.priority,
                             self.execute_tasks,
                             argument=(team,))
            self.priority += 1

        self.sched.run()

    def pull_code(self):
        print("pulling code...")
        gitcmd = ['git', 'pull']
        process = subprocess.Popen(gitcmd, stdout=subprocess.PIPE)
        output = process.communicate()[0]
        print(str(output.strip()))

    def make_pdf(self, team):
        main.run(team)

    def send_emails(self, team):
        pass

    def check_for_new_day(self):
        today = datetime.datetime.today().strftime('%Y-%m-%d')
        if self.today !=  today:
            self.today = today
            self.new_day = True

    def execute_tasks(self, team):
        self.pull_code()
        self.scrape_games()
        self.make_pdf(team)
        self.send_emails(team)

    def run(self):
        self.store_current_time()
        self.find_start_times()
        self.get_delays()
        self.schedule_tasks()

    def reset(self):
        self.priority = 1
        self.tasks_completed = False
        self.new_day = False


if __name__ == '__main__':
    auto = Automator()

    while True:

        while not auto.game.todays_games_in_db():
            auto.scrape_games()

            if not auto.game.todays_games_in_db():
                time.sleep(3600)

        if not auto.tasks_completed:
            auto.run()
            auto.tasks_completed = True

        while not auto.new_day:
            auto.check_for_new_day()

            if not auto.new_day:
                time.sleep(3600)

        auto.reset()

