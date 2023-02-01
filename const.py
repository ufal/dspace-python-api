import enum
import sys

import expected
from support.logs import log, Severity

user = "test@test.edu"
password = "dspace"
# password = "admin"
# user = "m@edu.com"
# password = "dspace"

# http or https
use_ssl = False
# host = "localhost"
host = "dev-5.pc"
# fe_port = ":4000"
fe_port = None
# be_port = ":8080"
be_port = None
be_location = "/server/"

# command that imports items into oai
# in github action, this command is correct
import_command = "docker exec -it dspace /dspace/bin/dspace oai import -c > /dev/null 2> /dev/null"
# if run on dev-5 under other than devops users (with sudo rights, obviously)
# import_command = "sudo docker exec -it dspace /dspace/bin/dspace oai import -c > /dev/null 2> /dev/null"

# when run locally on windows. Might need replacement of path
# import_command = "cd C:/dspace/bin && dspace oai import -c > NUL 2> NUL"

"""
test to find out trivial mistakes
"""
if use_ssl != expected.exp_SSL or host != expected.exp_host or fe_port != expected.exp_FE_port \
        or be_port != expected.exp_BE_port or import_command != expected.exp_import_command:
    main = "Host settings are not what is expected for tests!!"
    print(main)
    print(main, file=sys.stderr)
    log(main + ":", Severity.WARN)
    log("use_ssl: " + str(use_ssl), Severity.WARN)
    log("host: " + str(host), Severity.WARN)
    log("fe_port: " + str(fe_port), Severity.WARN)
    log("be_port: " + str(be_port), Severity.WARN)
    log("import_command: " + str(import_command), Severity.WARN)
    log("Please check expected.py and correct values in const.py", Severity.WARN)
else:
    log("Host settings are what is expected", Severity.DEBUG)

"""
 when starting tests, import everything once, to have most recent views
 (if False, some items might be in dspace but not in OAI and would not be detected,
 since only items that are freshly created are imported. This ensures even items
 created before tests start ARE in OAI-PMH)
 recommended to set to True
"""
ENABLE_IMPORT_AT_START = True
# ENABLE_IMPORT_AT_START = False

on_dev_5 = host == "dev-5.pc"

# there should be no need to modify this part, unless adding new tests.
# mainly concatenates and parses settings above
protocol = "https://" if use_ssl else "http://"
url = protocol + host
FE_url = url + (fe_port if fe_port else "")
BE_url = url + (be_port if be_port else "") + be_location
OAI_url = BE_url + "oai/"
OAI_req = OAI_url + "request?verb=ListRecords&metadataPrefix=oai_dc&set="
OAI_openaire_dc = OAI_url + "openaire_data?verb=ListRecords&metadataPrefix=oai_dc&set="
OAI_openaire_datacite = OAI_url + "openaire_data?verb=ListRecords&metadataPrefix=oai_datacite&set="
OAI_olac = OAI_url + "request?verb=ListRecords&metadataPrefix=olac&set="
OAI_cmdi = OAI_url + "request?verb=ListRecords&metadataPrefix=cmdi&set="
API_URL = BE_url + "api/"
IMPORT_DATA_PATH = "data/license_import/"
COM = "BB-TEST-COM"
com_UUID = None
COL = "BB-TEST-COL"
col_UUID = None
ITM_prefix = "BB-TEST-ITM-"
EMBEDDED = "_embedded"


class ItemType(enum.Enum):
    ITEM = 1
    COMMUNITY = 2
    COLLECTION = 3
