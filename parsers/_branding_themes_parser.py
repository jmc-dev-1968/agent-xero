
import json
import re
import os.path
from util import clean_date
from archiver import Archiver

class BrandingThemesParser():

  def __init__(self, log, data_dir):

    self.log = log
    self.data_dir = data_dir

  def parse(self):

    data_type = "branding-themes"

    proc_dir = "processing/default"
    json_file_name = "{}/{}/{}.json".format(self.data_dir, proc_dir, data_type)
    csv_file_name = "{}/{}/{}.csv".format(self.data_dir, proc_dir, data_type)

    if not os.path.isfile(json_file_name):
      self.log.write("ERROR {} file does not exist, did you forget to extract this?".format(json_file_name))
      return False

    with open(json_file_name, encoding='utf-8') as f:
      data = json.load(f)

    collection = 'BrandingThemes'
    if collection not in data:
      self.log.write("ERROR '{}' collection not found in JSON file".format(collection))
      return

    # zero ts in json header
    zero_created_datetime = clean_date(data['DateTimeUTC'])

    col_header = "BrandingThemeID,Name,LogoUrl,Type,SortOrder,CreatedDateUTC"

    csv_file = open(csv_file_name, 'w', encoding='utf-8')
    csv_file.write(re.sub(r'\n', '', col_header) + "\n")

    i = 0

    for theme in data['BrandingThemes']:

      i = i + 1

      id = theme['BrandingThemeID']
      name = theme['Name']
      url = theme['LogoUrl'] if 'LogoUrl' in theme else ''
      type = theme['Type'] if 'Type' in theme else ''
      sort_order = theme['SortOrder'] if 'SortOrder' in theme else ''
      created_date = clean_date(theme['CreatedDateUTC'])

      columns = [id, name, url, type, sort_order, created_date]

      prep_columns = list(map(lambda col: "\"" + str(col) + "\"", columns))
      line = ",".join(prep_columns) + "\n"

      csv_file.write(line)

    csv_file.close()
    self.log.write("INFO [{}] CSV file created {} ({:,} records)".format(data_type, csv_file_name, i))

    ark = Archiver(self.log)
    ark.archive(data_type, json_file_name)
