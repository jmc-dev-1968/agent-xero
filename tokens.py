
from datetime import datetime
import json
from pprint import pprint

class Tokens:

  access_token = None
  refresh_token = None
  expiration_datetime = None
  updated_datetime = None

  def __init__(self):
    self.fetch()

  def fetch(self):

    with open('tokens.json') as json_file:
      data = json.load(json_file)
      self.access_token = data['access_token']
      self.refresh_token = data['refresh_token']
      self.expiration_datetime = data['expiration_datetime']
      self.updated_datetime = data['updated_datetime']


  def dictize(self):

    data = {
      "access_token": self.access_token,
      "refresh_token": self.refresh_token,
      "expiration_datetime": self.expiration_datetime,
      "updated_datetime": self.updated_datetime
    }
    return data


  def update(self):

    self.updated_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = self.dictize()
    with open('tokens.json', 'w', encoding='utf-8') as f:
      json.dump(data, f, ensure_ascii=False, indent=4)


  def print(self):
    data = self.dictize()
    pprint(data)