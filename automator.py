import subprocess
import datetime
import pytz
import sched
import time
import pickle
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

from dbclasses import Game
import main
import scrape

class Automator:
    def __init__(self, teams=['NYY', 'NYM', 'HOU']):
        self.teams = teams
        self.sched = sched.scheduler(time.time, time.sleep)
        self.priority = 1
        self.today = datetime.datetime.today().strftime('%Y-%m-%d')
        self.game = Game()
        self.new_day = False
        with open('elist.pkl', 'rb') as f:
            self.elist = pickle.load(f)

    def scrape_previews(self):
        self.store_current_time()

        print("Scraping today's games at {} EST".format(self.nowf))

        scrape.game_previews()

    # def schedule_scrape(self):
    #     if not self.game.todays_games_in_db():
    #         self.sched.enter(3600, self.priority, self.scrape_games)
    #         self.sched.run()

    def find_start_times(self):
        all_times = self.game.get_all_start_times()
        self.times = {team : time for team, time in all_times.items()
                                                  if team in self.teams}

        self.execute_times = {team : time - datetime.timedelta(minutes=50)
                                      for team, time in self.times.items()}

    def store_current_time(self):
        now = datetime.datetime.now()
        self.now = now.astimezone(pytz.timezone('America/New_York'))
        self.nowf = self.now.strftime("%-I:%M%p")

    def get_delays(self):
        # 1 second for testing:
        # self.delays = {team : 1 for team in self.execute_times.keys()}

        self.delays = {team : (time -self.now).total_seconds()
                           for team, time in self.execute_times.items()}

    def schedule_tasks(self):
        for team, delay in self.delays.items():
            self.sched.enter(delay,
                             self.priority,
                             self.execute_tasks,
                             argument=(team,))
            self.priority += 1

        self.display_tasks()
        self.sched.run()

    def display_tasks(self):
        for team, time in self.execute_times.items():
            timef = time.strftime("%-I:%M%p")
            print("Tasks for {} scheduled at {} EST".format(team, timef))

    def pull_code(self):
        print("pulling code...")
        gitcmd = ['git', 'pull']
        process = subprocess.Popen(gitcmd, stdout=subprocess.PIPE)
        output = process.communicate()[0]
        print(str(output.strip()))

    def make_pdf(self, team):
        main.run(team)

    def send_emails(self, team):
        fn = '{}-{}.pdf'.format(team, self.today)
        recips = self.elist[team]

        msg = MIMEMultipart()
        msg['Subject'] = "{} Game Preview".format(team)
        msg['From'] = 'mlbpreviews@gmail.com'
        msg['To'] = '{}-fans@not-a-real-email.com'.format(team)

        text = "Game preview for {}".format(team)
        body = MIMEText(text)
        msg.attach(body)

        with open(fn, 'rb') as f:
            pdf = f.read()
            attachment = MIMEApplication(pdf)
            attachment['Content-Disposition'] = 'attachment; filename={}'\
                                                             .format(fn)
            msg.attach(attachment)

        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login('mlbpreviews@gmail.com', 'mlb$913ab')
        s.send_message(msg, 'mlbpreviews@gmail.com', recips)
        s.close() # or s.quit() ?


    def check_for_new_day(self):
        today = datetime.datetime.today().strftime('%Y-%m-%d')
        if self.today !=  today:
            self.today = today
            self.new_day = True

    def execute_tasks(self, team):
        self.pull_code()
        self.scrape_previews()
        self.make_pdf(team)
        self.send_emails(team)

    def run(self):
        self.store_current_time()
        self.find_start_times()
        self.get_delays()
        self.schedule_tasks()

    def reset(self):
        self.priority = 1
        self.new_day = False


if __name__ == '__main__':
    auto = Automator()

    while True:

        # Scrape to get game start times
        auto.scrape_previews()

        # Add tasks to scheduler
        auto.run()
        print("All tasks completed. Waiting for tomorrow's games...")

        # Change this to wait until the next day
        # instead of checking each hour for the next day?
        while not auto.new_day:
            auto.check_for_new_day()

            if not auto.new_day:
                time.sleep(3600)

        auto.reset()

# If script is run after the execute time but before the game starts, change execute time to now


