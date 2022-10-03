# this file contains expected values for tests
# sometimes SOMEONE changes them in main.py and then errors might not be too readable.
# if the values are not what is present here, warnings will be shown

exp_host = "dev-5.pc"
exp_FE_port = None
exp_BE_port = None
exp_SSL = False
exp_import_command = "docker exec -it dspace /dspace/bin/dspace oai import -c > /dev/null 2> /dev/null"