import json
from support.dspace_proxy import rest_proxy
from support.item_checking import import_license, check_com_col, assure_item_from_file, get_test_soup
from support.logs import log
 
print('Going to import license labels.')
# Opening JSON file
with open('import/data/license_labels.json') as json_file:
    licenseLabelsJson = json.load(json_file)

    for licenseLabel in licenseLabelsJson:
        import_license_label(licenseLabel["id"], licenseLabel["label"], licenseLabel["title"], licenseLabel["extended"])
        print(f'License label: {licenseLabel} imported!')

