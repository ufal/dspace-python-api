import json
from support.dspace_proxy import rest_proxy
from support.item_checking import import_license, check_com_col, assure_item_from_file, get_test_soup
from support.logs import log
 
print('Going to import licenses.')
# Opening JSON file
with open('import/data/license_definitions.json') as json_file:
    licenseDefinitions = json.load(json_file)

    for license in licenseDefinitions:
        import_license(license["name"], license["definition"], license["labelId"], license["confirmation"], license["requiredInfo"])
        print(f'License: {license} imported!')