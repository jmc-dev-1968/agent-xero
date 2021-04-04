
import os
from config import config
from datetime import datetime

class Log:

  def __init__(self, echo=False):

    self.echo = echo
    self.log_dir = config["log-dir"] if config["log-dir"] != "" else os.getcwd()
    self.log_file = config["log-file"] if config["log-file"] != "" else "log.txt"
    self.log = open(self.log_dir + '/' + self.log_file, 'a', encoding='utf-8')
    self.write("LOG OPENED")

  def write(self, msg):

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = "{} {}".format(ts, msg)
    self.log.write(log_msg + "\n")
    if self.echo:
      print(log_msg)

  def close(self):

    self.write("LOG CLOSED")
    self.log.close()