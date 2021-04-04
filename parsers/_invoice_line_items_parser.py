
import json
import re
import csv
import os
from util import clean_date
from archiver import Archiver

class InvoiceLineItemsParser():

  def __init__(self, log, data_dir):

    self.log = log
    self.data_dir = data_dir

  def parse(self):

    h_data_type = "invoice-headers"
    d_data_type = "invoice-line-items"

    proc_dir = "processing/invoices"
    json_dir = "{}/{}".format(self.data_dir, proc_dir)
    json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]

    if not json_files:
      self.log.write("ERROR {}/*.json files do not exist, did you forget to extract this?".format(json_dir))
      return False

    xero_url_accrec = "https://go.xero.com/AccountsReceivable/View.aspx?InvoiceID="
    xero_url_accpay = "https://go.xero.com/AccountsPayable/View.aspx?InvoiceID="

    csv_header_file_name = "{}/{}-delta.csv".format(json_dir, h_data_type)
    csv_detail_file_name = "{}/{}-delta.csv".format(json_dir, d_data_type)

    header_col_header = """
    Type,InvoiceID,InvoiceNumber,Reference,
    AmountDue,AmountPaid,AmountCredited,CurrencyRate,IsDiscounted,HasAttachments,HasErrors,
    ContactID,Name,Date,BrandingThemeID,BrandingThemeName,Status,LineAmountTypes,
    SubTotal,TotalTax,Total,UpdatedDateUTC,CurrencyCode,ProcessingNotes, URL
    """

    detail_col_header = """
    InvoiceID,InvoiceNumber,Type,Date,LineItemNumber,
    LineItemID,ItemCode,ItemCode2,Description,UnitAmount,TaxAmount,LineAmount,AccountCode,Quantity
    """

    csv_header_file = open(csv_header_file_name, 'w', encoding='utf-8')
    csv_header_file.write(re.sub(r"[\n\t\s]", "", header_col_header) + "\n")

    csv_detail_file = open(csv_detail_file_name, 'w', encoding='utf-8')
    csv_detail_file.write(re.sub(r"[\n\t\s]", "", detail_col_header) + "\n")

    # read in branding themes
    themes_csv_file = self.data_dir + "/processing/default/branding-themes.csv"
    if not os.path.isfile(themes_csv_file):
      msg = "ERROR {} file does not exist, did you forget to extract this?"
      print(msg)
      self.log.write(msg)
      return
    themes = {}
    with open(themes_csv_file, "r") as f:
        reader = csv.reader(f, delimiter = ",")
        for j, line in enumerate(reader):
            if j > 0:
              themes[line[0]] = line[1]

    i = 0
    j = 0

    for file in json_files:

      i = i + 1

      self.log.write("INFO [invoice-line-items] processing file {}".format(file))

      json_file_name = "{}/{}".format(json_dir, file)

      with open(json_file_name, encoding='utf-8') as f:
        data = json.load(f)

      collection = 'Invoices'
      if collection not in data:
        self.log.write("ERROR '{}' collection not found in JSON file {}".format(collection, file))
        continue

      # zero ts in json header
      zero_created_datetime = clean_date(data['DateTimeUTC'])

      # single invoice, but is part of a collection
      invoice = data['Invoices'][0]

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
        type, invoice_id, invoice_number, reference, amount_due,
        amount_paid, amount_credited, currency_rate, is_discounted, has_attachments, has_errors, contact_id,
        name,  date,
        branding_theme_id, branding_theme_name, status, line_amount_types, sub_total, total_tax,
        total, updated_date_utc, currency_code, processing_notes, url
      ]

      prep_columns = list(map(lambda col: "\"" + str(col) + "\"", columns))
      line = ",".join(prep_columns) + "\n"
      csv_header_file.write(line)

      # process line items
      if 'LineItems' not in invoice:
        self.log.write("WARN no line items found for invoice {}".format(invoice_id))
        continue

      line_items = invoice['LineItems']

      # artificial li number
      line_item_num = 0
      for line_item in line_items:

        # total line items
        j = j + 1

        line_item_num = line_item_num + 1

        li_lid = line_item['LineItemID']
        li_item_code = line_item['ItemCode'] if 'ItemCode' in line_item else ''
        li_desc = line_item['Description'] if 'Description' in line_item else ''
        li_unit_amt = line_item['UnitAmount'] if 'UnitAmount' in line_item else 0.00
        li_tax_amt = line_item['TaxAmount'] if 'TaxAmount' in line_item else 0.00
        li_line_amt = line_item['LineAmount'] if 'LineAmount' in line_item else 0.00
        li_acct_code = line_item['AccountCode'] if 'AccountCode' in line_item else ''
        li_qty = line_item['Quantity'] if 'Quantity' in line_item else 0.00

        # desc field allows cr's, strip here
        li_desc = li_desc.strip("\n")

        # some codes have an addiitonal code piped on to them
        li_item_code2 = ""
        parts = li_item_code.split("|")
        if len(parts) == 2:
          li_item_code = parts[0].strip()
          li_item_code2 = parts[1].strip()

        columns = [invoice_id, invoice_number,type,date,line_item_num,
                   li_lid,li_item_code,li_item_code2,li_desc,
                   li_unit_amt,li_tax_amt,li_line_amt,li_acct_code,li_qty]

        prep_columns = list(map(lambda col: "\"" + str(col) + "\"", columns))
        line = ",".join(prep_columns) + "\n"
        csv_detail_file.write(line)

    csv_header_file.close()
    self.log.write("INFO [{}] CSV file created {} ({:,} records)".format('invoice-headers', csv_header_file_name, i))

    csv_detail_file.close()
    self.log.write("INFO [{}] CSV file created {} ({:,} records)".format('invoice-line-items', csv_detail_file_name, i))

    ark = Archiver(self.log)
    files = list(map(lambda file: "{}/{}".format(json_dir, file), json_files))
    ark.archive('invoices', files)
    ark.archive(h_data_type, csv_header_file_name)
    ark.archive(d_data_type, csv_detail_file_name)
