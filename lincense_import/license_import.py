import json


import const

from support.dspace_proxy import rest_proxy

def import_data(request_mapping, file_name):
    url = const.API_URL + request_mapping
    x = open(const.IMPORT_DATA_PATH + file_name)
    json_array = [json.loads(s) for s in x.readlines()]
    x.close()
    response = rest_proxy.d.api_post(url, None, json_array)
    print(response)


def import_licenses(label_file_name, mapping_file_name, license_file_name):
    import_data('licenses/import/labels', label_file_name)
    import_data('licenses/import/extendedMapping', mapping_file_name)
    import_data('licenses/import/licenses', license_file_name)

