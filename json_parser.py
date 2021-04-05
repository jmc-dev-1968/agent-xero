
import os
from config import config

from parsers._branding_themes_parser import  BrandingThemesParser
from parsers._items_parser import ItemsParser
from parsers._contacts_parser import ContactsParser
from parsers._invoices_parser import InvoicesParser
from parsers._invoice_line_items_parser import InvoiceLineItemsParser


class Parser:

  def __init__(self, log):
    self.log = log
    self.data_dir = config["data-dir"] if config["data-dir"] != "" else os.getcwd()

  def parse(self, type):
    if  type == "branding-themes":
      parser = BrandingThemesParser(self.log, self.data_dir)
      parser.parse()
    elif  type == "items":
      parser = ItemsParser(self.log, self.data_dir)
      parser.parse()
    elif  type == "contacts":
      parser = ContactsParser(self.log, self.data_dir)
      parser.parse()
    elif  type == "invoices":
      parser = InvoicesParser(self.log, self.data_dir)
      parser.parse()
    elif  type == "invoices-line-items":
      parser = InvoiceLineItemsParser(self.log, self.data_dir)
      parser.parse()
