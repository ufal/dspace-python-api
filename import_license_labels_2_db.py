import json

from support.item_checking import import_license_label

print('Going to import license labels.')
# Opening JSON file
with open('import/data/license_labels.json') as json_file:
    licenseLabelsJson = json.load(json_file)

    for licenseLabel in licenseLabelsJson:
        import_license_label(licenseLabel["id"], licenseLabel["label"], licenseLabel["title"], licenseLabel["extended"])
        print(f'License label: {licenseLabel} imported!')

