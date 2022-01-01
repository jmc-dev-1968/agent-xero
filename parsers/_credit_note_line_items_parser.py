
import json
import re
import csv
import os
from util import clean_date
from archiver import Archiver

class CreditNoteLineItemsParser():

  def __init__(self, log, data_dir):

    self.log = log
    self.data_dir = data_dir

  def parse(self, is_init=False):

    h_data_type = "credit-note-headers"
    d_data_type = "credit-note-line-items"

    proc_dir = "processing/credit-notes"
    json_dir = "{}/{}".format(self.data_dir, proc_dir)
    json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]

    if not json_files:
      self.log.write("ERROR {}/*.json files do not exist, did you forget to extract this?".format(json_dir))
      return False

    csv_header_file_name = "{}/{}.csv".format(json_dir, h_data_type)
    csv_detail_file_name = "{}/{}.csv".format(json_dir, d_data_type)

    header_col_header = """
    Type,CreditNoteID,CreditNoteNumber,Reference,RemainingCredit,
    ContactID,Name,Date,BrandingThemeID,BrandingThemeName,Status,
    SubTotal,TotalTax,Total,UpdatedDateUTC,
    ProcessingNotes
    """

    detail_col_header = """
    CreditNoteID,CreditNoteNumber,
    DisplayItemCode,DisplayItemCode2,Description,UnitAmount,TaxType,TaxAmount,LineAmount,AccountCode,Quantity,
    ItemID,ItemName,ItemCode,ItemCode2,
    ProcessingNotes
    """

    csv_header_file = open(csv_header_file_name, 'w', encoding='utf-8')
    flat_header_col_header = re.sub(r"[\n\t\s]", "", header_col_header)
    csv_header_file.write(flat_header_col_header + "\n")

    csv_detail_file = open(csv_detail_file_name, 'w', encoding='utf-8')
    flat_detail_col_header = re.sub(r"[\n\t\s]", "", detail_col_header)
    csv_detail_file.write(flat_detail_col_header + "\n")

    # read in branding themes
    themes_csv_file = self.data_dir + "/processing/default/branding-themes.csv"
    if not os.path.isfile(themes_csv_file):
      msg = "ERROR {} file does not exist, did you forget to extract this?".format(themes_csv_file)
      print(msg)
      self.log.write(msg)
      return
    themes = {}
    with open(themes_csv_file, "r") as f:
        reader = csv.reader(f, delimiter = ",")
        for j, line in enumerate(reader):
            if j > 0:
              themes[line[0]] = line[1]

    file_cnt = 0
    inv_cnt = 0
    inv_li_cnt = 0

    for file in json_files:

      file_cnt += 1
      cur_inv_cnt = 0
      cur_inv_li_cnt = 0

      json_file_name = "{}/{}".format(json_dir, file)

      with open(json_file_name, encoding='utf-8') as f:
        data = json.load(f)

      collection = 'CreditNotes'
      if collection not in data:
        self.log.write("ERROR '{}' collection not found in JSON file {}".format(collection, file))
        continue

      # zero ts in json header
      #zero_created_datetime = clean_date(data['DateTimeUTC'])

      for credit_note in data['CreditNotes']:

        cur_inv_cnt+=1

        type = credit_note['Type'] if 'Type' in credit_note else ''
        credit_note_id = credit_note['CreditNoteID']
        credit_note_number = credit_note['CreditNoteNumber'] if 'CreditNoteNumber' in credit_note else ''
        reference = credit_note['Reference'] if 'Reference' in credit_note else ''
        remaining_credit = credit_note['RemainingCredit'] if 'RemainingCredit' in credit_note else 0.00

        if 'Contact' in credit_note and credit_note['Contact']:
          contact = credit_note['Contact']
          contact_id = contact['ContactID']
          contact_name = contact['Name'] if 'Name' in contact else ''
        else:
          contact_id = ""
          contact_name = ""

        # use DateString
        date = (credit_note['DateString'])[:10] if 'DateString' in credit_note else ''

        branding_theme_id = credit_note['BrandingThemeID'] if 'BrandingThemeID' in credit_note else ''
        status = credit_note['Status'] if 'Status' in credit_note else ''
        sub_total = credit_note['SubTotal'] if 'SubTotal' in credit_note else 0.00
        total_tax = credit_note['TotalTax'] if 'TotalTax' in credit_note else 0.00
        total = credit_note['Total'] if 'Total' in credit_note else 0.00
        updated_date_utc = clean_date(credit_note['UpdatedDateUTC']) if 'UpdatedDateUTC' in credit_note else ''

        # omit unless Patrick asks for this later
        #fully_paid_on_date = clean_date(credit_note['FullyPaidOnDate']) if 'FullyPaidOnDate' in credit_note else ''

        # get branding theme name
        processing_notes = ""
        if branding_theme_id in themes.keys():
          branding_theme_name =  themes[branding_theme_id]
        else:
          branding_theme_name =  ""
          processing_notes = "branding theme id not found"

        columns = [
          type, credit_note_id, credit_note_number, reference, remaining_credit,
          contact_id, contact_name,  date, branding_theme_id, branding_theme_name, status,
          sub_total, total_tax, total, updated_date_utc,
          processing_notes
        ]

        prep_columns = list(map(lambda col: "\"" + str(col) + "\"", columns))
        line = ",".join(prep_columns) + "\n"
        csv_header_file.write(line)

        # process line items
        if 'LineItems' not in credit_note:
          self.log.write("WARN no line items found for credit_note {}".format(credit_note_id))
          continue

        line_items = credit_note['LineItems']

        # artificial li number
        line_item_num = 0

        for line_item in line_items:

          cur_inv_li_cnt += 1

          li_processing_notes = ""

          line_item_num = line_item_num + 1

          li_disp_item_code = line_item['ItemCode'] if 'ItemCode' in line_item else ''
          li_desc = line_item['Description'] if 'Description' in line_item else ''
          li_unit_amt = line_item['UnitAmount'] if 'UnitAmount' in line_item else 0.00
          li_tax_type = line_item['TaxType'] if 'TaxType' in line_item else ''
          li_tax_amt = line_item['TaxAmount'] if 'TaxAmount' in line_item else 0.00
          li_line_amt = line_item['LineAmount'] if 'LineAmount' in line_item else 0.00
          li_acct_code = line_item['AccountCode'] if 'AccountCode' in line_item else ''
          li_qty = line_item['Quantity'] if 'Quantity' in line_item else 0.00

          # api returns a item record (slightly different from the "displayed" item on the UI)
          if 'Item' in line_item:
            item = line_item['Item']
            li_item_id = item['ItemID'] if 'ItemID' in item else ''
            li_item_name = item['Name'] if 'Name' in item else ''
            li_item_code = item['Code'] if 'Code' in item else ''
          else:
            li_item_id = ''
            li_item_name = ''
            li_item_code = ''

          # clean up invalid chars
          (li_desc, num_sub) = re.subn(r"[\n\r\"]", " ", li_desc)
          if  num_sub > 0:
            li_processing_notes = "invalid characters found in Description/Name field(s)"
          (li_item_name, num_sub) = re.subn(r"[\n\r\"]", " ", li_item_name)
          if  num_sub > 0 and not li_processing_notes:
            li_processing_notes = "invalid characters found in Description/Name field(s)"

          # some codes have an addiitonal code piped on to them (BC convention)
          li_disp_item_code2 = ""
          parts = li_disp_item_code.split("|")
          if len(parts) == 2:
            li_disp_item_code = parts[0].strip()
            li_disp_item_code2 = parts[1].strip()

          li_item_code2 = ""
          parts = li_item_code.split("|")
          if len(parts) == 2:
            li_item_code = parts[0].strip()
            li_item_code2 = parts[1].strip()

          columns = [credit_note_id, credit_note_number,
                     li_disp_item_code, li_disp_item_code2, li_desc, li_unit_amt, li_tax_type, li_tax_amt, li_line_amt, li_acct_code, li_qty,
                     li_item_id, li_item_name, li_item_code, li_item_code2,
                     li_processing_notes]

          prep_columns = list(map(lambda col: "\"" + str(col) + "\"", columns))
          line = ",".join(prep_columns) + "\n"
          csv_detail_file.write(line)

      inv_cnt = inv_cnt + cur_inv_cnt
      inv_li_cnt = inv_li_cnt + cur_inv_li_cnt

      self.log.write("INFO [credit-note-line-items] file processed {}, {:,} records found".format(file, cur_inv_cnt))


    csv_header_file.close()
    self.log.write("INFO [{}] CSV file created {} ({:,} records)".format('credit-note-headers', csv_header_file_name, inv_cnt))

    csv_detail_file.close()
    self.log.write("INFO [{}] CSV file created {} ({:,} records)".format('credit-note-line-items', csv_detail_file_name, inv_li_cnt))

    ark = Archiver(self.log)
    files = list(map(lambda file: "{}/{}".format(json_dir, file), json_files))

    ark.archive('credit-notes', files, is_init)

    ark.archive(h_data_type, csv_header_file_name)
    ark.archive(d_data_type, csv_detail_file_name)
    ark.copy(h_data_type, csv_header_file_name, 'master')
    ark.copy(d_data_type, csv_detail_file_name, 'master')

    cols = flat_header_col_header.split(",")
    format_dict = {
      "RemainingCredit": "0.00",
      "Date": "short",
      "SubTotal": "0.00",
      "TotalTax": "0.00",
      "Total": "0.00",
      "UpdatedDateUTC": "long"
    }
    formats = list(map(lambda col: format_dict[col] if col in format_dict else "", cols))
    ark.copy('credit-note-headers', csv_header_file_name, 'current', excelize=True, xlsx_formats=formats)

    cols = flat_detail_col_header.split(",")
    format_dict = {
      "UnitAmount": "0.00",
      "TaxAmount": "0.00",
      "LineAmount": "0.00",
      "Quantity": "0.00"
    }
    formats = list(map(lambda col: format_dict[col] if col in format_dict else "", cols))
    ark.copy('credit-note-line-items', csv_detail_file_name, 'current', excelize=True, xlsx_formats=formats)
