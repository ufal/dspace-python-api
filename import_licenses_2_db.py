import json
from support.item_checking import import_license
 
def import_licenses():
    print('Going to import licenses.')
    # Opening JSON file
    with open('import/data/license_definitions_v2.json') as json_file:
        licenseDefinitions = json.load(json_file)

        for license in licenseDefinitions:
            import_license(license["name"], license["definition"], license["labelId"], license["confirmation"], license["requiredInfo"])
            print(f'License: {license} imported!')
