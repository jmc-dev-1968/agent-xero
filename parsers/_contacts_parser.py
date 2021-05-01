
import json
import re
import os.path
from util import clean_date, zero_to_empty
from archiver import Archiver

class ContactsParser():

  def __init__(self, log, data_dir):

    self.log = log
    self.data_dir = data_dir

  def parse(self):

    data_type = "contacts"
    xero_url = "https://go.xero.com/Contacts/View/"

    proc_dir = "processing/default"
    json_file_name = "{}/{}/{}.json".format(self.data_dir, proc_dir, data_type)
    csv_file_name = "{}/{}/{}.csv".format(self.data_dir, proc_dir, data_type)

    if not os.path.isfile(json_file_name):
      self.log.write("ERROR {} file does not exist, did you forget to extract this?".format(json_file_name))
      return False

    with open(json_file_name, encoding='utf-8') as f:
      data = json.load(f)

    collection = 'Contacts'
    if collection not in data:
      self.log.write("ERROR '{}' collection not found in JSON file".format(collection))
      return

    # zero ts in json header
    #zero_created_datetime = clean_date(data['DateTimeUTC'])

    col_header = """
    ContactID,AccountNumber,ContactStatus,Name,FirstName,LastName,EmailAddress,SkypeUserName,Segment1,Segment2,Segment3,BankAccountDetails,TaxNumber,
    Street_City,Street_Region,Street_PostalCode,Street_Country,
    POBOX_AddressLine1,POBOX_AddressLine2,POBOX_AddressLine3,POBOX_AddressLine4,POBOX_City,POBOX_Region,POBOX_PostalCode,POBOX_Country,POBOX_AttentionTo,
    DEFAULT_PhoneNumber,DEFAULT_PhoneAreaCode,DEFAULT_PhoneCountryCode,
    MOBILE_PhoneNumber,MOBILE_PhoneAreaCode,MOBILE_PhoneCountryCode,
    FAX_PhoneNumber,FAX_PhoneAreaCode,FAX_PhoneCountryCode,
    DDI_PhoneNumber,DDI_PhoneAreaCode,DDI_PhoneCountryCode,
    UpdatedDateUTC,IsSupplier,IsCustomer,
    ProcessingNotes,URL 
    """

    csv_file = open(csv_file_name, 'w', encoding='utf-8')
    csv_file.write(re.sub(r"[\n\t\s]", "", col_header) + "\n")

    i = 0

    for contact in data[collection]:

      i = i + 1

      contact_id = contact['ContactID']

      account_number = contact['AccountNumber'] if 'AccountNumber' in contact else ''
      contact_status = contact['ContactStatus'] if 'ContactStatus' in contact else ''
      name = contact['Name'] if 'Name' in contact else ''
      first_name = contact['FirstName'] if 'FirstName' in contact else ''
      last_name = contact['LastName'] if 'LastName' in contact else ''
      email_address = contact['EmailAddress'] if 'EmailAddress' in contact else ''

      # parse segments
      skype_user_name = contact['SkypeUserName'] if 'SkypeUserName' in contact else ''
      parts = skype_user_name.split("|")
      if len(parts) == 3:
        segment_1 = parts[0].strip()
        segment_2 = parts[1].strip()
        segment_3 = parts[2].strip()
        processing_notes = ""
      else:
        segment_1 = ""
        segment_2 = ""
        segment_3 = ""
        processing_notes = "malformed [SkypeUserName] field"

      bank_account_details = contact['BankAccountDetails'] if 'BankAccountDetails' in contact else ''
      tax_number = zero_to_empty(contact['TaxNumber']) if 'TaxNumber' in contact else ''

      # (ugly) initializer
      street_city, street_region, street_postalcode, street_country = "", "", "", ""
      pobox_addressline1, pobox_addressline2, pobox_address_line3, pobox_address_line4, pobox_city, pobox_region = "", "", "", "", "", ""
      pobox_postal_code, pobox_country, pobox_attention_to = "", "", ""

      if 'Addresses' in contact and contact['Addresses']:
        addresses = contact['Addresses']
        for address in addresses:
          if address['AddressType'] ==  'STREET':
            street_city = zero_to_empty(address['City']) if 'City' in address else ''
            street_region = zero_to_empty(address['Region']) if 'Region' in address else ''
            street_postalcode = zero_to_empty(address['PostalCode']) if 'PostalCode' in address else ''
            street_country = zero_to_empty(address['Country']) if 'Country' in address else ''
          elif address['AddressType'] ==  'POBOX':
            pobox_addressline1 = zero_to_empty(address['AddressLine1']) if 'AddressLine1' in address else ''
            pobox_addressline2 = zero_to_empty(address['AddressLine2']) if 'AddressLine2' in address else ''
            pobox_address_line3 = zero_to_empty(address['AddressLine3']) if 'AddressLine3' in address else ''
            pobox_address_line4 = zero_to_empty(address['AddressLine4']) if 'AddressLine4' in address else ''
            pobox_city = zero_to_empty(address['City']) if 'City' in address else ''
            pobox_region = zero_to_empty(address['Region']) if 'Region' in address else ''
            pobox_postal_code = zero_to_empty(address['PostalCode']) if 'PostalCode' in address else ''
            pobox_country = zero_to_empty(address['Country']) if 'Country' in address else ''
            pobox_attention_to = zero_to_empty(address['AttentionTo']) if 'AttentionTo' in address else ''
          else:
            # TODO : other type of address (write note to log)
            pass

      # (ugly) initializer
      ddi_phone_number, ddi_phone_area_code, ddi_phone_country_code = "", "", ""
      default_phone_number, default_phone_area_code, default_phone_country_code = "", "", ""
      fax_phone_number, fax_phone_area_code, fax_phone_country_code = "", "", ""
      mobile_phone_number, mobile_phone_area_code, mobile_phone_country_code = "", "", ""

      if 'Phones' in contact and contact['Phones']:
        phones = contact['Phones']
        for phone in phones:
          if phone['PhoneType'] ==  'DDI':
            ddi_phone_number = zero_to_empty(phone['PhoneNumber']) if 'PhoneNumber' in phone else ''
            ddi_phone_area_code = zero_to_empty(phone['PhoneAreaCode']) if 'PhoneAreaCode' in phone else ''
            ddi_phone_country_code = zero_to_empty(phone['PhoneCountryCode']) if 'PhoneCountryCode' in phone else ''
          elif phone['PhoneType'] ==  'DEFAULT':
            default_phone_number = zero_to_empty(phone['PhoneNumber']) if 'PhoneNumber' in phone else ''
            default_phone_area_code = zero_to_empty(phone['PhoneAreaCode']) if 'PhoneAreaCode' in phone else ''
            default_phone_country_code = zero_to_empty(phone['PhoneCountryCode']) if 'PhoneCountryCode' in phone else ''
          elif phone['PhoneType'] == 'FAX':
            fax_phone_number = zero_to_empty(phone['PhoneNumber']) if 'PhoneNumber' in phone else ''
            fax_phone_area_code = zero_to_empty(phone['PhoneAreaCode']) if 'PhoneAreaCode' in phone else ''
            fax_phone_country_code = zero_to_empty(phone['PhoneCountryCode']) if 'PhoneCountryCode' in phone else ''
          elif phone['PhoneType'] ==  'MOBILE':
            mobile_phone_number = zero_to_empty(phone['PhoneNumber']) if 'PhoneNumber' in phone else ''
            mobile_phone_area_code = zero_to_empty(phone['PhoneAreaCode']) if 'PhoneAreaCode' in phone else ''
            mobile_phone_country_code = zero_to_empty(phone['PhoneCountryCode']) if 'PhoneCountryCode' in phone else ''
          else:
            # TODO : other type of phone (write note to log)
            pass

      updated_date_utc = clean_date(contact['UpdatedDateUTC']) if 'UpdatedDateUTC' in contact else ''
      is_supplier = contact['IsSupplier'] if 'IsSupplier' in contact else ''
      is_customer = contact['IsCustomer'] if 'IsCustomer' in contact else ''

      url = xero_url + contact_id

      columns = [
        contact_id, account_number, contact_status, name, first_name, last_name, email_address, skype_user_name, segment_1, segment_2, segment_3, bank_account_details,tax_number,
        street_city, street_region, street_postalcode, street_country,
        pobox_addressline1, pobox_addressline2, pobox_address_line3, pobox_address_line4, pobox_city, pobox_region,
        pobox_postal_code, pobox_country, pobox_attention_to,
        default_phone_number, default_phone_area_code, default_phone_country_code,
        mobile_phone_number, mobile_phone_area_code, mobile_phone_country_code,
        fax_phone_number, fax_phone_area_code, fax_phone_country_code,
        ddi_phone_number, ddi_phone_area_code, ddi_phone_country_code,
        updated_date_utc, is_supplier, is_customer,
        processing_notes, url
      ]

      prep_columns = list(map(lambda col: "\"" + str(col) + "\"", columns))
      line = ",".join(prep_columns) + "\n"

      csv_file.write(line)

    csv_file.close()
    self.log.write("INFO [{}] CSV file created {} ({:,} records)".format(data_type, csv_file_name, i))

    formats = [
      '', '', '', '', '', '', '', '', '', '', '', '', '',
      '', '', '', '',
      '', '', '', '', '', '',
      '', '', '',
      '', '', '',
      '', '', '',
      '', '', '',
      '', '', '',
      'long', '', '',
      '', ''
    ]

    ark = Archiver(self.log)
    ark.archive(data_type, json_file_name)
    ark.archive(data_type, csv_file_name)
    ark.copy(data_type, csv_file_name, 'master')
    ark.copy(data_type, csv_file_name, 'current', excelize=True, xlsx_formats=formats)
