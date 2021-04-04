
from config import config
import os
import pandas as pd
import csv
from archiver import Archiver

class Merger:

    def __init__(self, log):
        self.log = log
        self.data_dir = config["data-dir"] if config["data-dir"] != "" else os.getcwd()


    def merge_invoice_delta(self):

        # master + (daily) delta files            
        header_master_file = "{}/master/invoice-headers.csv".format(self.data_dir)
        header_delta_file = "{}/processing/invoices/invoice-headers-delta.csv".format(self.data_dir)
        detail_master_file = "{}/master/invoice-line-items.csv".format(self.data_dir)
        detail_delta_file = "{}/processing/invoices/invoice-line-items-delta.csv".format(self.data_dir)

        # read in as df's
        df_header_master = pd.read_csv(header_master_file, index_col='InvoiceID')
        df_header_delta = pd.read_csv(header_delta_file, index_col='InvoiceID')
        df_detail_master = pd.read_csv(detail_master_file)#, index_col='LineItemID')
        df_detail_delta = pd.read_csv(detail_delta_file)#, index_col='LineItemID')

        hm_cnt = df_header_master.shape[0]
        hd_cnt = df_header_delta.shape[0]
        #print("{:,} rows in header master".format(hm_cnt))
        #print("{:,} rows in header delta".format(hd_cnt))

        dm_cnt = df_detail_master.shape[0]
        dd_cnt = df_detail_delta.shape[0]
        #print("{:,} rows in detail master".format(dm_cnt))
        #print("{:,} rows in detail delta".format(dd_cnt))

        h_del_cnt = 0
        d_del_cnt = 0
        # loop through invoice header delta
        for id, row in df_header_delta.iterrows():
            # id record exists delete it (will be re-inserted next)
            if id in df_header_master.index.values:
                # delete header row
                h_del_cnt = h_del_cnt + 1 
                df_header_master.drop(id, inplace=True)
                # delete related detail rows
                d_del_cnt = d_del_cnt + df_detail_master[df_detail_master['InvoiceID']==id].shape[0]
                df_detail_master.drop(df_detail_master[df_detail_master['InvoiceID']==id].index, inplace=True)

        # concat master files (with deletes) + delta files = UPSERTED master files
        df_new_header_master = pd.concat([df_header_master, df_header_delta])
        df_new_detail_master = pd.concat([df_detail_master, df_detail_delta])

        new_header_master_file = "{}/processing/invoices/invoice-headers.csv".format(self.data_dir)
        new_detail_master_file = "{}/processing/invoices/invoice-line-items.csv".format(self.data_dir)

        if os.path.exists(new_header_master_file):
            os.remove(new_header_master_file)
        df_new_header_master.to_csv(new_header_master_file, header=True, index=True, quoting=csv.QUOTE_ALL)

        if os.path.exists(new_detail_master_file):
            os.remove(new_detail_master_file)
        df_new_detail_master.to_csv(new_detail_master_file, header=True, index=False, quoting=csv.QUOTE_ALL)

        self.log.write("INFO [invoice-headers] {:,} invoice records inserted into header master".format(df_new_header_master.shape[0] - hm_cnt))
        self.log.write("INFO [invoice-headers] {:,} invoice records updated in header master".format(hd_cnt - (df_new_header_master.shape[0] - hm_cnt)))
        self.log.write("INFO [invoice-headers] master file written to {}".format(new_header_master_file))

        self.log.write("INFO [invoice-details] {:,} invoice records inserted into detail master".format(df_new_detail_master.shape[0] - dm_cnt))
        self.log.write("INFO [invoice-details] {:,} invoice records updated in detail master".format(dd_cnt - (df_new_detail_master.shape[0] - dm_cnt)))
        self.log.write("INFO [invoice-details] master file written to {}".format(new_detail_master_file))

        ark = Archiver(self.log)
        ark.archive('invoice-headers', new_header_master_file)
        ark.archive('invoice-line-items', new_detail_master_file)
        ark.copy('invoice-headers', new_header_master_file, 'master')
        ark.copy('invoice-line-items', new_detail_master_file, 'master')

        formats = [
            '', '', '', '', '0.00',
            '0.00', '0.00', '0.00', '', '', '', '',
            '', 'short',
            '', '', '', '', '0.00', '0.00',
            '0.00', 'long', '', '', ''
        ]
        ark.copy('invoice-headers', new_header_master_file, 'current', excelize=True, xlsx_formats=formats)

        formats = ['', '', '', 'short', '0',
                   '', '', '', '',
                   '0.00', '0.00', '0.00', '', '0.00']
        ark.copy('invoice-line-items', new_detail_master_file, 'current', excelize=True, xlsx_formats=formats)
