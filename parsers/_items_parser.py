
import json
import re
import os.path
from util import clean_date
from archiver import Archiver

class ItemsParser():

  def __init__(self, log, data_dir):

    self.log = log
    self.data_dir = data_dir

  def parse(self):

    data_type = "items"
    xero_url = "https://go.xero.com/Accounts/Inventory/"

    proc_dir = "processing/default"
    json_file_name = "{}/{}/{}.json".format(self.data_dir, proc_dir, data_type)
    csv_file_name = "{}/{}/{}.csv".format(self.data_dir, proc_dir, data_type)

    if not os.path.isfile(json_file_name):
      self.log.write("ERROR {} file does not exist, did you forget to extract this?".format(json_file_name))
      return False

    with open(json_file_name, encoding='utf-8') as f:
      data = json.load(f)

    collection = 'Items'
    if collection not in data:
      self.log.write("ERROR '{}' collection not found in JSON file".format(collection))
      return

    # zero ts in json header
    zero_created_datetime = clean_date(data['DateTimeUTC'])

    col_header = """
    ItemID,Code,Code2,Description,PurchaseDescription,UpdatedDateUTC,PurchasedUnitPrice,PurchasedCOGSAccountCode,
    PurchasedTaxType,SalesUnitPrice,SalesAccountCode,SalesTaxType,Name,IsTrackedAsInventory,
    InventoryAssetAccountCode,TotalCostPool,QuantityOnHand,IsSold,IsPurchased,
    SupplierCode,ProductDescription,ProductSegment1,ProductSegment2,
    PurchaseSupplierCode,PurchaseProductDescription,PurchaseProductSegment1,PurchaseProductSegment2,
    ProcessingNotes, URL 
    """

    csv_file = open(csv_file_name, 'w', encoding='utf-8')
    csv_file.write(re.sub(r"[\n\t\s]", "", col_header) + "\n")

    i = 0

    for item in data[collection]:

      i = i + 1

      item_id = item['ItemID']
      code = item['Code']
      description = item['Description']

      purchase_description = item['PurchaseDescription'] if 'PurchaseDescription' in item else ''
      updated_date_utc = clean_date(item['UpdatedDateUTC']) if 'UpdatedDateUTC' in item else ''

      if 'PurchaseDetails' in item and item['PurchaseDetails']:
        details = item['PurchaseDetails']
        purchase_unit_price = details['UnitPrice'] if 'UnitPrice' in details else ''
        purchase_cogs_account_code = details['COGSAccountCode'] if 'COGSAccountCode' in details else ''
        purchase_tax_type = details['TaxType'] if 'TaxType' in details else ''
      else:
        purchase_unit_price = ''
        purchase_cogs_account_code = ''
        purchase_tax_type = ''

      if 'SalesDetails' in item and item['SalesDetails']:
        details = item['SalesDetails']
        sales_unit_price = details['UnitPrice'] if 'UnitPrice' in details else ''
        sales_account_code = details['AccountCode'] if 'AccountCode' in details else ''
        sales_tax_type = details['TaxType'] if 'TaxType' in details else ''
      else:
        sales_unit_price = ''
        sales_account_code = ''
        sales_tax_type = ''

      name = item['Name']
      is_tracked_as_inventory = item['IsTrackedAsInventory'] if 'IsTrackedAsInventory' in item else' '
      inventory_asset_account_code = item['InventoryAssetAccountCode'] if 'InventoryAssetAccountCode' in item else' '
      total_cost_pool = item['TotalCostPool'] if 'TotalCostPool' in item else' '
      quantity_on_hand = item['QuantityOnHand'] if 'QuantityOnHand' in item else' '
      is_sold = item['IsSold'] if 'IsSold' in item else' '
      is_purchased = item['IsPurchased'] if 'IsPurchased' in item else' '

      # some codes have an addiitonal code piped on to them
      code2 = ""
      parts = code.split("|")
      if len(parts) == 2:
        code = parts[0].strip()
        code2 = parts[1].strip()

      processing_notes = ""

      supplier_code = ""
      product_description = ""
      product_segment_1 = ""
      product_segment_2 = ""

      purchase_supplier_code = ""
      purchase_product_description = ""
      purchase_product_segment_1 = ""
      purchase_product_segment_2 = ""

      # parse desc's for supplier code, desc, product segment 1, product segment 2
      parts = description.split("|")
      if len(parts) != 4:
        processing_notes = "malformed [Description] field"
      else:
        supplier_code = parts[0].strip()
        product_description = parts[1].strip()
        product_segment_1 = parts[2].strip()
        product_segment_2 = parts[3].strip()

      parts = purchase_description.split("|")
      if len(parts) != 4:
        if not processing_notes:
          ProcessingNotes = "malformed [PurchaseDescription] field"
        else:
          processing_notes = processing_notes + "/" + "malformed [PurchaseDescription] field"
      else:
        purchase_supplier_code = parts[0].strip()
        purchase_product_description = parts[1].strip()
        purchase_product_segment_1 = parts[2].strip()
        purchase_product_segment_2 = parts[3].strip()

      url = xero_url + item_id

      columns = [
        item_id, code, code2, description, purchase_description, updated_date_utc, purchase_unit_price,
        purchase_cogs_account_code,
        purchase_tax_type, sales_unit_price, sales_account_code, sales_tax_type, name, is_tracked_as_inventory,
        inventory_asset_account_code, total_cost_pool, quantity_on_hand, is_sold, is_purchased,
        supplier_code, product_description, product_segment_1, product_segment_2,
        purchase_supplier_code, purchase_product_description, purchase_product_segment_1, purchase_product_segment_2,
        processing_notes, url
      ]

      prep_columns = list(map(lambda col: "\"" + str(col) + "\"", columns))
      line = ",".join(prep_columns) + "\n"

      csv_file.write(line)

    csv_file.close()
    self.log.write("INFO [{}] CSV file created {} ({:,} records)".format(data_type, csv_file_name, i))

    formats = [
      '', '', '', '', '', 'long', '0.00',
      '',
      '', '0.00', '', '', '', '',
      '', '0.00', '0.00', '', '',
      '', '', '', '',
      '', '', '', '',
      '', ''
    ]

    ark = Archiver(self.log)
    ark.archive(data_type, json_file_name)
    ark.archive(data_type, csv_file_name)
    ark.copy(data_type, csv_file_name, 'master')
    ark.copy(data_type, csv_file_name, 'current', excelize=True, xlsx_formats=formats)
