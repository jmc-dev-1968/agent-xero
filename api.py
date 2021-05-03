
import requests
from config import config
import codecs
import json
import os
import time
from datetime import datetime, timedelta
import base64
import pprint as pp
import pandas as pd

class API:

  def __init__(self, tokens, log):

    self.tokens = tokens
    self.log = log
    self.data_dir = config["data-dir"] if config["data-dir"]  != "" else os.getcwd()

  
  def fetch_data(self, data_type, uuid=""):
  
    naked_data_types = ['items', 'invoices', 'branding-themes', 'contacts', 'accounts']
    guid_data_types = ['invoice']
  
    if data_type not in naked_data_types + guid_data_types:
      print("first parameter must be in data types : ({})".format(",".join(naked_data_types + guid_data_types)))
      exit()
  
    if data_type in guid_data_types and not uuid:
      print("second parameter must be a uuid for data types : ({})".format(",".join(guid_data_types)))
      exit()
  
    retries_limit = config["api-call-retries"]
  
    xero_endpoint = config["xero-endpoint"]
  
    url = ''
    proc_dir = 'processing/default'
    if data_type == "items":
      url = xero_endpoint + '/Items'
    elif data_type == "invoices":
      url = xero_endpoint + '/Invoices'
    elif data_type == "branding-themes":
      url = xero_endpoint + '/BrandingThemes'
    elif data_type == "contacts":
      url = xero_endpoint + '/Contacts/?includeArchived=true'
    elif data_type == "accounts":
      url = xero_endpoint + '/Accounts'
    elif data_type == "invoice":
      url = xero_endpoint + '/Invoices/{uuid}'.format(uuid=uuid)
      proc_dir = 'processing/invoices'
    else:
      self.log.write("ERROR must supply a defined data type ({} is invalid)".format(data_type))
      exit()

    # append root dir
    proc_dir = format("{}/{}").format(self.data_dir, proc_dir)

    expiration_datetime = datetime. strptime(self.tokens.expiration_datetime, "%Y-%m-%d %H:%M:%S")

    if datetime.now() > expiration_datetime:
      self.refresh_token()

    tenant_id = config['xero-tenant-id']
  
    # start session
    session = requests.session()

    # json file
    if uuid:
      self.log.write("INFO extracting '{} ({})' data from Xero ... ".format(data_type, uuid))
      file_name = "{}/{}--{}.json".format(proc_dir, data_type, uuid)
    else:
      self.log.write("INFO extracting '{}' data from Xero ... ".format(data_type))
      file_name = "{}/{}.json".format(proc_dir, data_type)


    retries = 0
    while 1==1:
      # call api
      headers = {
        'content-type': 'application/json',
        'accept': 'application/json',
        'xero-tenant-id': tenant_id,
        'authorization': 'Bearer ' + self.tokens.access_token
      }
      response = requests.get(url, headers=headers)
      # check payload
      json_text = json.loads(response.text)
      # TODO : error handling for (a) general HTTP error and (b) expired token  (b is whats handled below)
      if 'Detail' in json_text:
        print(json_text)
        self.log.write("ERROR api {} extraction failed : {}/{}".format(data_type, json_text["Title"], json_text["Detail"]))
        success = self.refresh_token()
        if not success:
          if retries == retries_limit:
            self.log.write("INFO max retries ({}) reached, aborting {} extraction".format(retries_limit, data_type))
            return False
          else:
            retries = retries + 1
            self.log.write("INFO attempting retry #{}".format(retries))
      else:
        file = codecs.open(file_name, "w", "utf-8")
        file.write(response.text)
        file.close()
        self.log.write("INFO api {} extraction succeeded data saved to : {}".format(data_type, file_name))
        break

    # success
    return True


  def test_call(self, do_refresh_token=False):
    xero_endpoint = config["xero-endpoint"]
    expiration_datetime = datetime.strptime(self.tokens.expiration_datetime, "%Y-%m-%d %H:%M:%S")
    if datetime.now() > expiration_datetime:
      self.refresh_token()
    tenant_id = config['xero-tenant-id']
    headers = {
      'content-type': 'application/json',
      'accept': 'application/json',
      'xero-tenant-id': tenant_id,
      'authorization': 'Bearer ' + self.tokens.access_token
    }
    # grab short request
    url = xero_endpoint + "Currencies"
    response = requests.get(url, headers=headers)
    json_text = json.loads(response.text)
    pp.pprint(json_text)
    # try one more time
    if do_refresh_token and 'Detail' in json_text:
      success = self.refresh_token()
      if success:
        headers = {
          'content-type': 'application/json',
          'accept': 'application/json',
          'xero-tenant-id': tenant_id,
          'authorization': 'Bearer ' + self.tokens.access_token
        }
        response = requests.get(url, headers=headers)
        json_text = json.loads(response.text)
        pp.pprint(json_text)


  def fetch_invoice_details(self, cutoff=None, hours_delta=None, tz_offset=0, dry_run=False):

    # TODO add logic to prevent illogical flags (i.e. cutoff and  hours_delta)
    # TODO add logic to process all invoices (initialization)

    milliseconds = 1010
    seconds = 0.001 * milliseconds

    invoices_csv_file = self.data_dir + "/processing/default/invoices.csv"
    if not os.path.isfile(invoices_csv_file):
      self.log.write("ERROR {} file does not exist, did you forget to extract this?".format(invoices_csv_file))
      return False

    # read in invoice id's (we just need the id and updated date)
    df = pd.read_csv(invoices_csv_file, index_col='InvoiceID', usecols=['InvoiceID', 'UpdatedDateUTC'])
    df['UpdatedDateUTC'] = pd.to_datetime(df['UpdatedDateUTC'])

    utc_now = datetime.utcnow()
    if cutoff:
      cutoff_datetime = datetime.strptime(cutoff, "%Y-%m-%d %H:%M:%S")
    else:
      cutoff_datetime = utc_now + timedelta(hours=int(tz_offset - hours_delta))

    # grab invoices newer than cutoff date
    cutoff_datetime_str = cutoff_datetime.strftime("%Y-%m-%d %H:%M:%S")
    df_delta = df[(df['UpdatedDateUTC'] > cutoff_datetime_str)]

    invoice_cnt = df_delta.shape[0]

    if dry_run:
      print("{:,} invoices found".format(invoice_cnt))
      return True, 0

    if invoice_cnt == 0:
      return True, 0

    i = 0
    for id, _ in df_delta.iterrows():
      success = self.fetch_data('invoice', id)
      if not success:
        return False, i
      i = i + 1
      # governor
      time.sleep(seconds)

    return True, i
  
  def refresh_token(self):
  
    url = "https://identity.xero.com/connect/token"
  
    client_id = config['client_id']
    client_secret = config['client_secret']

    client_id_secret = "{}:{}".format(client_id, client_secret)
    authorization =  "Basic " + base64.urlsafe_b64encode(client_id_secret.encode()).decode()

    refresh_token = self.tokens.refresh_token

    # start session
    session = requests.session()
  
    headers = {
      'grant_type': 'refresh_token',
      'content-type': 'application/x-www-form-urlencoded',
      'authorization': authorization
    }
  
    payload = {
      'grant_type': 'refresh_token',
      'refresh_token': refresh_token,
      #'client_id': client_id,
      #'client_secret': client_secret
    }
  
    response = requests.post(url, data=payload, headers=headers)
  
    data = response.text
    json_resp = json.loads(data)
  
    if 'error' in json_resp:
  
      self.log.write("ERROR refresh_token refresh failed : {}".format(json_resp["error"]))
      return False
  
    else:
  
      refresh_token = json_resp['refresh_token']
      access_token = json_resp['access_token']
      expire_in_secs = int(json_resp['expires_in'])

      expiration_datetime = datetime.now() + timedelta(seconds=expire_in_secs)

      self.tokens.refresh_token = refresh_token
      self.tokens.access_token = access_token
      self.tokens.expiration_datetime =  expiration_datetime.strftime("%Y-%m-%d %H:%M:%S")
      self.tokens.update()

      self.log.write("INFO refresh_token refresh succeeded")
      self.log.write("INFO new access_token expiration date is " + self.tokens.expiration_datetime)

      return True
