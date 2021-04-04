
import os
from datetime import datetime
from shutil import copyfile
from util import csv_2_xlsx, make_dir, parse_file_name
from config import config

class Archiver:

  def __init__(self, log):
    self.log = log
    self.data_dir = config["data-dir"] if config["data-dir"] != "" else os.getcwd()


  def archive(self, data_type, file_names):
    ds = datetime.now().strftime("%Y-%m-%d")
    ts = datetime.now().strftime("%Y-%m-%dT%H%M%S")
    ark_dir = "{}/archive/{}/{}".format(self.data_dir, data_type, ds)
    make_dir(ark_dir)
    files = [file_names] if type(file_names) is not list else file_names
    for file in files:
      _, naked_file_name, stem, ext = parse_file_name(file)
      ark_file_name = "{}/{}--{}.{}".format(ark_dir, stem, ts, ext)
      copyfile(file, ark_file_name)
      self.log.write("INFO [{}] {} archived to {}".format(data_type, naked_file_name, ark_file_name))


  def copy(self, data_type, file_name, copy_dest_dir, excelize=False, xlsx_formats=None):
    dirs = {
      "current": self.data_dir + "/current",
      "master": self.data_dir + "/master"
    }
    _, naked_file_name, _, ext = parse_file_name(file_name)
    target_file_name = dirs[copy_dest_dir] + "/" + naked_file_name
    if os.path.exists(target_file_name):
      os.remove(target_file_name)
    copyfile(file_name, target_file_name)
    self.log.write("INFO [{}] {} copied to {}".format(data_type, naked_file_name, target_file_name))
    if copy_dest_dir == 'current' and excelize:
      if ext.lower() != 'csv':
        print("ERROR : cannt excelize a {} file".format(ext))
        exit()
      formats = [] if  xlsx_formats is None else xlsx_formats
      xlsx_file_name = csv_2_xlsx(target_file_name, "", formats)
      self.log.write("INFO [{}] {} re-saved as {}".format(data_type, naked_file_name, xlsx_file_name))


  def archive_v1(self, data_type, json_file_name, csv_file_name,
              copy_to_archive=True, copy_to_current=True, copy_to_master=True,
              excelize=False, xlsx_formats=None):

    if copy_to_archive:
      # make json/csv copies for archive
      ds = datetime.now().strftime("%Y-%m-%d")
      ts = datetime.now().strftime("%Y-%m-%dT%H%M%S")
      ark_dir = "{}/archive/{}/{}".format(self.data_dir, data_type, ds)
      make_dir(ark_dir)
      ark_file_name = "{}/{}--{}.json".format(ark_dir, data_type, ts)
      copyfile(json_file_name, ark_file_name)
      self.log.write("INFO [{}] JSON archived to {}".format(data_type, ark_file_name ))
      ark_file_name = "{}/{}--{}.csv".format(ark_dir, data_type, ts)
      copyfile(csv_file_name, ark_file_name)
      self.log.write("INFO [{}] CSV archived to {}".format(data_type, ark_file_name ))

    if copy_to_current:
      # copy csv to current
      target_file_name = csv_file_name.replace("/processing/default/", "/current/")
      if os.path.exists(target_file_name):
        os.remove(target_file_name)
      copyfile(csv_file_name, target_file_name)
      self.log.write("INFO [{}] CSV copied to {}".format(data_type, target_file_name))
      if excelize:
        # make xlsx copy
        formats = [] if  xlsx_formats is None else xlsx_formats
        xlsx_file_name = csv_2_xlsx(target_file_name, "", formats)
        self.log.write("INFO [{}] CSV re-saved as XLSX {})".format(data_type, xlsx_file_name))

    if copy_to_master:
      # copy csv to master
      target_file_name = csv_file_name.replace("/processing/default/", "/master/")
      if os.path.exists(target_file_name):
        os.remove(target_file_name)
      copyfile(csv_file_name, target_file_name)
      self.log.write("INFO [{}] CSV copied to {}".format(data_type, target_file_name))


