
import os

config = {}

config["client_id"] = os.environ["CLIENT_ID"]
config["client_secret"] = os.environ["CLIENT_SECRET"]
config["xero-tenant-id"] = os.environ["XERO_TENANT_ID"]

config["scopes"] = "offline_access accounting.transactions openid profile email accounting.contacts accounting.settings"

config["xero-endpoint"] = "https://api.xero.com/api.xro/2.0/"
config["api-call-retries"] = 3

config["work-dir"] = "."
config["log-dir"] = "/home/jeff/host-share"
config["data-dir"] = "/home/jeff/host-share/data"

config["log-file"] = "xero-log.txt"
