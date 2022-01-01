
import sys
from api import API
from json_parser import Parser
from tokens import Tokens
from log import Log
from config import config
import util
from merger import Merger


# leave only /master dir files
def clean_dirs():

  root = config["data-dir"]
  util.delete_subdirs(root + '/archive')
  util.delete_subdirs(root + '/processing')
  util.delete_files(root + '/current', "*")


def prelim():

  root = config["data-dir"]

  util.make_dir(root + '/archive/items')
  util.make_dir(root + '/archive/contacts')
  util.make_dir(root + '/archive/invoices')
  util.make_dir(root + '/archive/credit-notes')
  util.make_dir(root + '/archive/invoice-headers')
  util.make_dir(root + '/archive/invoice-line-items')
  util.make_dir(root + '/archive/credit-note-headers')
  util.make_dir(root + '/archive/credit-note-line-items')

  util.make_dir(root + '/processing/default', '700') # no sftp access
  util.make_dir(root + '/processing/invoices', '700') # no sftp access
  util.make_dir(root + '/processing/credit-notes', '700') # no sftp access
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


def process_simple(log, do_fetch=True, do_parse=True):

  root = config["data-dir"]

  if do_fetch:
    tokens = Tokens()
    api = API(tokens, log)

    util.delete_files(root + '/processing/default', '*.json')
    util.delete_files(root + '/processing/credit-notes', '*.json')

    if not api.fetch_data("items"):
      return False
    if not api.fetch_data("branding-themes"):
      return False
    if not api.fetch_data("contacts"):
      return False
    # process all credit-notes
    success, page_count = api.fetch_data_by_page("credit-notes")
    if not success:
      return False

  if do_parse:

    util.delete_files(root + '/processing/default', '*.csv')
    util.delete_files(root + '/processing/credit-notes', '*.csv')

    parser = Parser(log)
    parser.parse('branding-themes')
    parser.parse('items')
    parser.parse('contacts')
    parser.parse('credit-note-line-items')

  return True


def process_invoices(log, do_fetch=True, do_parse=True, do_merge=True, do_init=False):

  root = config["data-dir"]

  if do_fetch:
    tokens = Tokens()
    api = API(tokens, log)
    util.delete_files(root + '/processing/invoices', '*.json')

    if do_init:
      util.delete_files(root + '/archive/invoices/init', '*.json')
      util.delete_files(root + '/master', 'invoice-*.csv')
      success, page_cnt = api.fetch_data_by_page("invoices")
    else:
      success, page_cnt = api.fetch_data_by_page("invoices", hours_delta=30, tz_offset=7)

    if success  and page_cnt > 0:
      log.write("INFO api invoices extraction succeeded {:,} pages saved to : {}".format(page_cnt, '/processing/invoices'))
    elif success  and page_cnt == 0:
      log.write("INFO api no invoices extracted (no new/updated invoices in refresh period)")
      return True
    else:
      log.write("ERROR api invoices extraction failed {:,} invoices saved to : {}".format(page_cnt, '/processing/invoices'))
      return False

  if do_parse:
    util.delete_files(root + '/processing/invoices', '*.csv')
    parser = Parser(log)
    parser.parse('invoices-line-items', is_init=do_init)

  if do_merge:
    merger = Merger(log)
    merger.merge_invoice_delta()

  return True


if __name__ == "__main__":

  task = sys.argv[1]

  if task == 'clean':
    clean_dirs()
    exit()

  log = Log(echo=False)
  tokens = Tokens()
  api = API(tokens, log)

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
    process_simple(log, do_fetch=True, do_parse=True)
    process_invoices(log, do_fetch=True, do_parse=True, do_merge=True, do_init=False)
  elif task == 'init_data':
    process_simple(log, do_fetch=True, do_parse=True)
    process_invoices(log, do_fetch=True, do_parse=True, do_merge=True, do_init=True)
  else:
    pass

  log.close()

