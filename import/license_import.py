import json
import sys
sys.path.append("../")

import const

from support.dspace_proxy import rest_proxy

def import_license_label_json():
    url = const.API_URL + 'licenses/import/labels'
    x = open("C:\dspace-blackbox-testing\data\jm.license_label.json")
    license_labels = [json.loads(s) for s in x.readlines()]
    x.close()
    rest_proxy.d.api_post(url, None, license_labels)

def import_extended_mapping_label_json():
    url = const.API_URL + 'licenses/import/extendedMapping'
    x = open("C:\dspace-blackbox-testing\data\jm.license_label_extended_mapping.json")
    extended_mapping = [json.loads(s) for s in x.readlines()]
    x.close()
    rest_proxy.d.api_post(url, None, extended_mapping)

def import_license_json():
    url = const.API_URL + 'licenses/import/licenses'
    x = open("C:\dspace-blackbox-testing\data\jm.license_definition.json")
    license = [json.loads(s) for s in x.readlines()]
    x.close()
    rest_proxy.d.api_post(url, None, license)

import_license_label_json()
import_extended_mapping_label_json()
import_license_json()