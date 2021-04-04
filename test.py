
import re
from datetime import datetime
import csv
import util
import api
from log import Log
#from parsers_old.parse_invoice_line_items import parse
from tokens import Tokens
from json_parser import Parser
from api import API
import pandas as pd
import os
from merger import Merger
from archiver import Archiver


log = Log(echo=True)
tokens = Tokens()
api = API(tokens, log)
#api.test_call()
success, invoice_cnt = api.fetch_invoice_details(hours_delta=48, tz_offset=7, dry_run=True)
log.close()

exit()

log = Log(echo=True)

formats = [
  '', '', '', '', '0.00',
  '0.00', '0.00', '0.00', '', '', '', '',
  '', 'short',
  '', '', '', '', '0.00', '0.00',
  '0.00', 'long', '', '', ''
]

new_header_master_file = '/home/jeff/data2/processing/invoices/invoice-headers.csv'
ark = Archiver(log)
ark.copy('invoice-headers', new_header_master_file, 'current', excelize=True, xlsx_formats=formats)

log.close()

exit()




log = Log(echo=True)
m = Merger(log)
m.merge_invoice_delta()
log.close()

exit()


students1 = [('jack', 34, 'Sydeny', 'Australia'),
            ('Riti', 30, 'Delhi', 'India'),
            ('Vikas', 31, 'Mumbai', 'India'),
            ('Neelu', 32, 'Bangalore', 'India')]

students2 = [('John', 16, 'New York', 'US'),
            ('Mike', 17, 'las vegas', 'US')]


# Create a DataFrame object
df_1 = pd.DataFrame(students1, columns=['Name', 'Age', 'City', 'Country']) #, index=['A','B','C','D'])
df_2 = pd.DataFrame(students2, columns=['Name', 'Age', 'City', 'Country']) #, index=['A','B'])

print(df_1)
print(df_2)

df_3 = pd.concat([df_1, df_2], ignore_index=True)
print(df_3)

exit()



s = "HEAVEN HILL | DOMAINE CANTON LIQUEUR 75CL/28% | SPIRIT |Liqueur\n"
print("[{}]".format(s))
c = s.strip("\n")
print("[{}]".format(c))

exit()

files = ["file.json"]

file_names = []
if type(files) is not list:
  file_names = [files]
else:
  file_names = files

print (type(files))
print (type(file_names))
print (file_names)

exit()

def parse_file_name(full_file_name):
  slash_parts = full_file_name.split(os.path.sep)
  path = (os.path.sep).join(slash_parts[:-1])
  file = slash_parts[-1]
  period_parts = (file.split('.'))
  ext = period_parts[-1]
  stem = ".".join(period_parts[:-1])
  return path, file, stem, ext

full_file = "/var/usr/www/data.json"

_, _, file, stem = parse_file_name(full_file)
print(file)
print(stem)


exit()

log = Log(echo=True)
tokens = Tokens()
api = API(tokens, log)
api.fetch_invoice_details(hours_delta=0, tz_offset=7)
log.close()
exit()

invoices_csv_file = '/home/jeff/data2/master/invoices.csv'
#data = pd.read_csv(invoices_csv_file, index_col='InvoiceID')
df = pd.read_csv(invoices_csv_file, index_col='InvoiceID', usecols=['InvoiceID', 'UpdatedDateUTC'])

#print(df.head())

df['UpdatedDateUTC'] = pd.to_datetime(df['UpdatedDateUTC'])

#print(df.head())

df2 = df[(df['UpdatedDateUTC']> "2021-03-02 22:24")]
print(df2.shape[0])

exit()

col_header = """
  Type,InvoiceID,InvoiceNumber,
ContactID,  Name,
SubTotal,  TotalTax,Total
""" #.replace(' ', '').replace('\t', '').replace('\n', '')

col_header = re.sub(r"[\n\t\s]", "", col_header)
print(col_header)

exit()


log = Log(echo=True)
tokens = Tokens()
api = API(tokens, log)
api.test_call()
#api.refresh_token()
log.close()
exit()

#log = Log(echo=False)
#parser = Parser(log)
#log.close()
#exit()


tokens = Tokens()
tokens.print()
tokens.update()
tokens.print()

exit()

util.csv_2_xlsx('./test-data.csv', '', ['','0.00', '0'])
exit()


mylog = Log(True)
mylog.write("test")
mylog.close()
exit()

api.fetch_invoice_details()
exit()

r = api.refresh_token()
exit()

x = api.fetch_token("refresh")
print(x)
exit()

with open("refresh_token", encoding='utf-8') as f:
  refresh_token = f.read().replace('\n', '')
print(refresh_token)

exit()


cols = [100, 200, 300]
new_cols = list(map(lambda col: "\"" + str(col) + "\"", cols))

#print(new_cols)

for col in new_cols:
  print(col)

exit()


themes = {}
with open("./csv/branding_themes.csv", "r") as f:
    reader = csv.reader(f, delimiter = ",")
    for i, line in enumerate(reader):
        if i > 0:
          themes[line[0]] = line[1]
          #print("line[{}] = {}".format(i, line))

for key in themes.keys():
  print("{} ==> {}".format(key, themes[key]))


exit()

blob = "/Date(1611029290047+0000)/"

date_part = re.match(r'^/Date\((.+?)\+0000\)/$', blob).groups(1)[0]
date = datetime.fromtimestamp(int(date_part)/1000)  # trunc ms
date_str = date.strftime("%Y-%m-%d %H:%M:%S")
#print(date_str)



def clean_date(unix_date_str):

  parts = re.match(r'^/Date\((.+?)\+(\d{4})\)/$', unix_date_str)
  dt = parts.groups(1)[0]
  offset = parts.groups(1)[1]
  date = datetime.fromtimestamp(int(dt) / 1000) # trunc ms
  return  date.strftime("%Y-%m-%d %H:%M:%S")


def clean_date2(unix_date_str):
  parts = re.match(r'^/Date\((.+)\)/$', unix_date_str)
  dt = parts.groups(1)[0]
  if re.match(r'.+?\+\d{4}$', dt):
    # TODO : add offset processing
    dt, offset = dt.split("+")
  date = datetime.fromtimestamp(int(dt) / 1000) # trunc ms
  return  date.strftime("%Y-%m-%d %H:%M:%S")



blob = "/Date(1611029290047+0000)/"
clean = clean_date2(blob)
print(clean)

blob = "/Date(1611029290047)/"
clean = clean_date2(blob)
print(clean)


def fetch_token(type):
  if type not in ['access', 'refresh']:
    return ()
  file = "{}_token".format(type)
  if not os.path.exists(file):
    return ""
  with open(file, "r") as f:
    value = f.read().replace('\n', '')
    return value


def update_token(type, value):
  if type not in ['access', 'refresh']:
    exit()
  file = "{}_token".format(type)
  with open(file, "w") as f:
    f.write(value)