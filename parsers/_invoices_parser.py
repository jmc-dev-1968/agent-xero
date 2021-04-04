
import json
import re
import os.path
from util import clean_date
from archiver import Archiver
import csv

class InvoicesParser():

  def __init__(self, log, data_dir):

    self.log = log
    self.data_dir = data_dir

  def parse(self):

    data_type = "invoices"
    xero_url_accrec = "https://go.xero.com/AccountsReceivable/View.aspx?InvoiceID="
    xero_url_accpay = "https://go.xero.com/AccountsPayable/View.aspx?InvoiceID="

    proc_dir = "processing/default"
    json_file_name = "{}/{}/{}.json".format(self.data_dir, proc_dir, data_type)
    csv_file_name = "{}/{}/{}.csv".format(self.data_dir, proc_dir, data_type)

    if not os.path.isfile(json_file_name):
      self.log.write("ERROR {} file does not exist, did you forget to extract this?".format(json_file_name))
      return False

    with open(json_file_name, encoding='utf-8') as f:
      data = json.load(f)

    collection = 'Invoices'
    if collection not in data:
      self.log.write("ERROR '{}' collection not found in JSON file".format(collection))
      return

    # zero ts in json header
    zero_created_datetime = clean_date(data['DateTimeUTC'])

    col_header = """
    InvoiceID,Type,InvoiceNumber,Reference,
    AmountDue,AmountPaid,AmountCredited,CurrencyRate,IsDiscounted,HasAttachments,HasErrors,
    ContactID,Name,Date,BrandingThemeID,BrandingThemeName,Status,LineAmountTypes,
    SubTotal,TotalTax,Total,UpdatedDateUTC,CurrencyCode,ProcessingNotes, URL
    """

    csv_file = open(csv_file_name, 'w', encoding='utf-8')
    csv_file.write(re.sub(r"[\n\t\s]", "", col_header) + "\n")

    # read in branding themes
    themes_csv_file = self.data_dir + "/processing/default/branding-themes.csv"
    if not os.path.isfile(themes_csv_file):
      self.log.write("ERROR {} file does not exist, did you forget to extract this?".format(themes_csv_file))
      return
    themes = {}
    with open(themes_csv_file, "r") as f:
        reader = csv.reader(f, delimiter = ",")
        for j, line in enumerate(reader):
            if j > 0:
              themes[line[0]] = line[1]

    i = 0

    for invoice in data['Invoices']:

      i = i + 1

      type = invoice['Type'] if 'Type' in invoice else ''
      invoice_id = invoice['InvoiceID']
      invoice_number = invoice['InvoiceNumber'] if 'InvoiceNumber' in invoice else ''
      reference = invoice['Reference'] if 'Reference' in invoice else ''
      amount_due = invoice['AmountDue'] if 'AmountDue' in invoice else 0.00
      amount_paid = invoice['AmountPaid'] if 'AmountPaid' in invoice else 0.00
      amount_credited = invoice['AmountCredited'] if 'AmountCredited' in invoice else 0.00
      currency_rate = invoice['CurrencyRate'] if 'CurrencyRate' in invoice else 0.00
      is_discounted = invoice['IsDiscounted'] if 'IsDiscounted' in invoice else ''
      has_attachments = invoice['HasAttachments'] if 'HasAttachments' in invoice else ''
      has_errors = invoice['HasErrors'] if 'HasErrors' in invoice else ''

      if 'Contact' in invoice and invoice['Contact']:
        contact = invoice['Contact']
        contact_id = contact['ContactID']
        name = contact['Name'] if 'Name' in contact else ''
      else:
        contact = ""
        contact_id = ""
        name = ""

      # use DateString
      date = (invoice['DateString'])[:10] if 'DateString' in invoice else ''

      branding_theme_id = invoice['BrandingThemeID'] if 'BrandingThemeID' in invoice else ''
      status = invoice['Status'] if 'Status' in invoice else ''
      line_amount_types = invoice['LineAmountTypes'] if 'LineAmountTypes' in invoice else ''
      sub_total = invoice['SubTotal'] if 'SubTotal' in invoice else ''
      total_tax = invoice['TotalTax'] if 'TotalTax' in invoice else ''
      total = invoice['Total'] if 'Total' in invoice else ''
      updated_date_utc = clean_date(invoice['UpdatedDateUTC']) if 'UpdatedDateUTC' in invoice else ''
      currency_code = invoice['CurrencyCode'] if 'CurrencyCode' in invoice else ''

      if type == "ACCPAY":
        url  = xero_url_accpay + invoice_id
      elif type == "ACCREC":
        url  = xero_url_accrec + invoice_id
      else:
        url = ""

      # get branding theme name
      processing_notes = ""
      if branding_theme_id in themes.keys():
        branding_theme_name =  themes[branding_theme_id]
      else:
        branding_theme_name =  ""
        processing_note = "branding theme id not found"

      columns = [
        invoice_id, type, invoice_number, reference, amount_due,
        amount_paid, amount_credited, currency_rate, is_discounted, has_attachments, has_errors, contact_id,
        name,  date,
        branding_theme_id, branding_theme_name, status, line_amount_types, sub_total, total_tax,
        total, updated_date_utc, currency_code, processing_notes, url
      ]

      prep_columns = list(map(lambda col: "\"" + str(col) + "\"", columns))
      line = ",".join(prep_columns) + "\n"

      csv_file.write(line)

    csv_file.close()
    self.log.write("INFO [{}] CSV file created {} ({:,} records)".format(data_type, csv_file_name, i))

    formats = [
      '', '', '', '', '0.00',
      '0.00', '0.00', '0.00', '', '', '', '',
      '', 'short',
      '', '', '', '', '0.00', '0.00',
      '0.00', 'long', '', '', ''
    ]

    ark = Archiver(self.log)
    ark.archive(data_type, json_file_name)
    ark.archive(data_type, csv_file_name)
    ark.copy(data_type, csv_file_name, 'master')
    ark.copy(data_type, csv_file_name, 'current', excelize=True, xlsx_formats=formats)
