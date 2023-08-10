import logging
import os

from migration_const import ICON_PATH
from data_pump.utils import read_json, do_api_post, convert_response_to_json, \
    save_dict_as_json


def import_license(eperson_id_dict, statistics_dict, save_dict):
    """
    Import data into database.
    Mapped tables: license_label, extended_mapping, license_definitions
    """
    # import license label
    label_json_name = 'license_label.json'
    saved_label_json_name = ' label_dict.json'
    label_url = 'core/clarinlicenselabels'
    imported_label = 0
    labels_dict = {}
    # import license_label
    label_json_list = read_json(label_json_name)
    if not label_json_list:
        logging.info("License_label JSON is empty.")
        return
    for label in label_json_list:
        label_json_p = {
            'label': label['label'],
            'title': label['title'],
            'extended': label['is_extended'],
            'icon': None
        }
        # find image with label name
        try:
            image_path = ICON_PATH + label['label'].lower() + ".png"
            if os.path.exists(image_path):
                with open(image_path, "rb") as image:
                    f = image.read()
                    label_json_p['icon'] = list(f)
        except Exception as e:
            logging.error(
                "Exception while reading label image with name: " + label[
                    'label'].lower() + ".png occurred: " + str(e))
        try:
            response = do_api_post(label_url, {}, label_json_p)
            created_label = convert_response_to_json(response)
            imported_label += 1
            del created_label['license']
            del created_label['_links']
            labels_dict[label['label_id']] = created_label
        except Exception as e:
            logging.error('POST request ' + label_url +
                          ' failed. Exception: ' + str(e))

    # save label dict as json
    if save_dict:
        save_dict_as_json(saved_label_json_name, labels_dict)
    statistics_val = (len(label_json_list), imported_label)
    statistics_dict['license_label'] = statistics_val

    # import license definition and exteended mapping
    license_json_name = 'license_definition.json'
    license_url = 'clarin/import/license'
    ext_map_json_name = 'license_label_extended_mapping.json'
    # read license label extended mapping
    ext_map_dict = {}
    ext_map_json_list = read_json(ext_map_json_name)
    if not ext_map_json_list:
        logging.info("Extended_mapping JSON is empty.")
        return
    for ext_map in ext_map_json_list:
        if ext_map['license_id'] in ext_map_dict.keys():
            ext_map_dict[ext_map['license_id']].append(labels_dict[ext_map['label_id']])
        else:
            ext_map_dict[ext_map['license_id']] = [labels_dict[ext_map['label_id']]]
    # import license_definition
    imported_license = 0
    license_json_list = read_json(license_json_name)
    if not license_json_list:
        logging.info("License_definitions JSON is empty.")
        return
    for license_ in license_json_list:
        license_json_p = {
            'name': license_['name'],
            'definition': license_['definition'],
            'confirmation': license_['confirmation'],
            'requiredInfo': license_['required_info'],
            'clarinLicenseLabel': labels_dict[license_['label_id']]
        }
        if license_['license_id'] in ext_map_dict:
            license_json_p['extendedClarinLicenseLabels'] = \
                ext_map_dict[license_['license_id']]
        params = {'eperson': eperson_id_dict[license_['eperson_id']]}
        try:
            response = do_api_post(license_url, params, license_json_p)
            if response.ok:
                imported_license += 1
            else:
                raise Exception(response)
        except Exception as e:
            logging.error('POST request ' + license_url +
                          ' failed. Exception: ' + str(e))

    statistics_val = (len(license_json_list), imported_license)
    statistics_dict['license_definition'] = statistics_val
    logging.info("License_label, Extended_mapping, License_definitions "
                 "were successfully imported!")
