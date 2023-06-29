import logging
import os

from migration_const import ICON_PATH
from utils import read_json, do_api_post, convert_response_to_json


def import_license_label(labels, statistics):
    """
    Import data into database.
    Mapped tables: license_label, extended_mapping, license_definitions
    """
    json_name = 'license_label.json'
    url = 'core/clarinlicenselabels'
    imported = 0
    # import license_label
    json_a = read_json(json_name)
    if not json_a:
        logging.info("License_label JSON is empty.")
        return
    for i in json_a:
        json_p = {'label': i['label'], 'title': i['title'], 'extended': i['is_extended'], 'icon': None}
        # find image with label name
        try:
            image_path = ICON_PATH + i['label'].lower() + ".png"
            if os.path.exists(image_path):
                with open(image_path, "rb") as image:
                    file = image.read()
                    json_p['icon'] = list(file)
        except Exception as e:
            logging.error(
                "Exception while reading label image with name: " + i['label'].lower() + ".png occurred: " + e)
        try:
            response = do_api_post(url, None, json_p)
            created_label = convert_response_to_json(response)
            imported += 1
            del created_label['license']
            del created_label['_links']
            labels[i['label_id']] = created_label
        except Exception as e:
            logging.error('POST request ' + response.url + ' failed. Status code ' + str(response.status_code))
    statistics['license_label'] = (len(json_a), imported)


def import_license_definition(labels, eperson_id, statistics):
    """
    Import data into database.
    Mapped tables: extended_mapping, license_definitions
    """
    json_name = 'license_definition.json'
    url = 'clarin/import/license'
    json_name_ext_map = 'license_label_extended_mapping.json'
    # read license label extended mapping
    extended_label = dict()
    json_a = read_json(json_name_ext_map)
    if not json_a:
        logging.info("Extended_mapping JSON is empty.")
        return
    for i in json_a:
        if i['license_id'] in extended_label.keys():
            extended_label[i['license_id']].append(labels[i['label_id']])
        else:
            extended_label[i['license_id']] = [labels[i['label_id']]]
    # import license_definition
    imported = 0
    json_a = read_json(json_name)
    if not json_a:
        logging.info("License_definitions JSON is empty.")
        return
    for i in json_a:
        json_p = {'name': i['name'], 'definition': i['definition'], 'confirmation': i['confirmation'],
                  'requiredInfo': i['required_info'], 'clarinLicenseLabel': labels[i['label_id']]}
        if i['license_id'] in extended_label:
            json_p['extendedClarinLicenseLabels'] = extended_label[i['license_id']]
        param = {'eperson': eperson_id[i['eperson_id']]}
        try:
            response = do_api_post(url, param, json_p)
            imported += 1
        except Exception as e:
            logging.error('POST request ' + response.url + ' failed. Status code ' + str(response.status_code))
    statistics['license_definition'] = (len(json_a), imported)
    logging.info("License_label, Extended_mapping, License_definitions were successfully imported!")
