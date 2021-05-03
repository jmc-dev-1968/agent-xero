
import sys
from api import API
from json_parser import Parser
from tokens import Tokens
from log import Log
from config import config
import util
from merger import Merger


def prelim():
  root = config["data-dir"]

  util.make_dir(root + '/archive/items')
  util.make_dir(root + '/archive/contacts')
  util.make_dir(root + '/archive/invoices')
  util.make_dir(root + '/archive/invoice-headers')
  util.make_dir(root + '/archive/invoice-line-items')

  util.make_dir(root + '/processing/default', '700') # no sftp access
  util.make_dir(root + '/processing/invoices', '700') # no sftp access
  util.make_dir(root + '/current')
  util.make_dir(root + '/master', '700') # no sftp access


def refresh(log):
  tokens = Tokens()
  api = API(tokens, log)
  api.refresh_token()


def test(log):
  tokens = Tokens()
  api = API(tokens, log)
  api.test_call()


def process_single(log, do_fetch=True, do_parse=True):
  root = config["data-dir"]

  if do_fetch:
    tokens = Tokens()
    api = API(tokens, log)
    util.delete_files(root + '/processing/default', '*.json')
    if not api.fetch_data("items"):
      return False
    if not api.fetch_data("branding-themes"):
      return False
    if not api.fetch_data("contacts"):
      return False
    if not api.fetch_data("invoices"):
      return False

  if do_parse:
    util.delete_files(root + '/processing/default', '*.csv')
    parser = Parser(log)
    parser.parse('branding-themes')
    parser.parse('items')
    parser.parse('contacts')
    parser.parse('invoices')

  return True


def process_multiple(log, do_fetch=True, do_parse=True, do_merge=True):
  root = config["data-dir"]

  if do_fetch:
    tokens = Tokens()
    api = API(tokens, log)
    util.delete_files(root + '/processing/invoices', '*.json')
    success, invoice_cnt = api.fetch_invoice_details(hours_delta=30, tz_offset=7)
    if success  and invoice_cnt > 0:
      log.write("INFO api invoices extraction succeeded {:,} invoices saved to : {}".format(invoice_cnt, '/processing/invoices'))
    elif success  and invoice_cnt == 0:
      log.write("INFO api no invoices extracted (no new/updated invoices in refresh period)")
      return True
    else:
      log.write("ERROR api invoices extraction failed {:,} invoices saved to : {}".format(invoice_cnt, '/processing/invoices'))
      return False

  if do_parse:
    util.delete_files(root + '/processing/invoices', '*.csv')
    parser = Parser(log)
    parser.parse('invoices-line-items')

  if do_merge:
    merger = Merger(log)
    merger.merge_invoice_delta()

  return True


if __name__ == "__main__":
  task = sys.argv[1]
  log = Log(echo=False)

  if task == 'test':
    test(log)
    log.close()
    exit()

  if task == 'refresh_token':
    log.echo=True
    refresh(log)
    log.echo=False
    log.close()
    exit()

  prelim()

  if task == 'process_data':
    process_single(log, do_fetch=True, do_parse=True)
    process_multiple(log, do_fetch=True, do_parse=True, do_merge=True)
  else:
    pass

  log.close()

