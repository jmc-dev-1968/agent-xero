
import re
from datetime import datetime
import os
import glob
import csv
from xlsxwriter.workbook import Workbook
from config import config
import shutil

cwd = config["work-dir"] if config["work-dir"] != "" else os.getcwd()

def make_dir(dir, chmod='755'):
  oct = int(chmod, 8)
  if not os.path.exists(dir):
    os.makedirs(dir, oct)
    print("directory {} created, permissions set to {}".format(dir, str(chmod)))


# delete subdirs under dir, but NOT the dir itself (if dir exists)
def delete_subdirs(dir, dry_run=False):
  if not os.path.exists(dir):
    print("directory {} does not exist".format(dir))
    return
  subdirs = [f.path for f in os.scandir(dir) if f.is_dir()]
  if dry_run:
    for subdir in subdirs:
      print(subdir)
  else:
    for subdir in subdirs:
      try:
        shutil.rmtree(subdir)
        print("directory {} deleted".format(subdir))
      except OSError as e:
        print("error: {} : {}".format(subdir, e.strerror))


# delete files under dir (if dir exists)
def delete_files(dir, wildcard="*", dry_run=False):
  if not os.path.exists(dir):
    print("directory {} does not exist".format(dir))
    return
  files = glob.glob('{}/{}'.format(dir, wildcard))
  if not files:
    return
  if dry_run:
    for file in files:
      print(file)
  else:
    i = 0
    for file in files:
      os.remove(file)
      i  += 1
    print("directory {}, all {} files deleted ({} files)".format(dir, wildcard, str(i)))


def clean_date(unix_date_str):
  parts = re.match(r'^/Date\((.+)\)/$', unix_date_str)
  dt = parts.groups(1)[0]
  if re.match(r'.+?\+\d{4}$', dt):
    # TODO : add offset processing
    dt, offset = dt.split("+")
  date = datetime.fromtimestamp(int(dt) / 1000) # trunc ms
  return  date.strftime("%Y-%m-%d %H:%M:%S")


def zero_to_empty(str):
  clean = "" if str.strip()=="0" else str
  return clean


def csv_2_xlsx(csv_file, xlsx_file="", formats=[]):

  if xlsx_file=="":
    xlsx_file = csv_file[:-4] + '.xlsx'

  if os.path.exists(xlsx_file):
    os.remove(xlsx_file)

  workbook = Workbook(xlsx_file)
  worksheet = workbook.add_worksheet('DATA')

  def_format = workbook.add_format()
  def_format.set_font('Calibri')
  def_format.set_font_size(8)

  num_format_1 = workbook.add_format({'num_format': '#,##0'})
  num_format_1.set_font('Consolas')
  num_format_1.set_font_size(8)

  num_format_2 = workbook.add_format({'num_format': '#,##0.00'})
  num_format_2.set_font('Calibri')
  num_format_2.set_font_size(8)

  date_format_1 = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss'})
  date_format_1.set_font('Calibri')
  date_format_1.set_font_size(8)

  date_format_2 = workbook.add_format({'num_format': 'yyyy-mm-dd'})
  date_format_2.set_font('Calibri')
  date_format_2.set_font_size(8)

  with open(csv_file, 'rt', encoding='utf8') as f:
    reader = csv.reader(f)
    for r, row in enumerate(reader):
      for c, col in enumerate(row):
        # header
        if r==0 or not formats:
          worksheet.write(r, c, col, def_format)
        else:
          if formats[c] == '0':
            col = "0"  if col.strip()=="" else col
            worksheet.write_number(r, c, int(col), num_format_1)
          elif formats[c] == '0.00':
            col = "0"  if col.strip()=="" else col
            worksheet.write_number(r, c, float(col), num_format_2)
          elif formats[c] == 'long':
            worksheet.write_datetime(r, c, datetime.strptime(col, "%Y-%m-%d %H:%M:%S"), date_format_1)
          elif formats[c] == 'short':
            worksheet.write_datetime(r, c, datetime.strptime(col, "%Y-%m-%d"), date_format_2)
          else:
            worksheet.write(r, c, col, def_format)

  workbook.close()

  return xlsx_file


def flatten_json_list(json_list):
  json_str = str(json_list)
  if json_str == "[]":
    json_str = ""
  return json_str


def parse_file_name(full_file_name):
  slash_parts = full_file_name.split(os.path.sep)
  path = (os.path.sep).join(slash_parts[:-1])
  file = slash_parts[-1]
  period_parts = (file.split('.'))
  ext = period_parts[-1]
  stem = ".".join(period_parts[:-1])
  return path, file, stem, ext
