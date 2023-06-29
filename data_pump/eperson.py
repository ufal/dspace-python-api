import logging
import json

from const import API_URL
from utils import read_json, convert_response_to_json, do_api_post


def import_eperson(metadata, eperson_id, email2epersonId, metadatavalue, metadata_field_id, statistics):
    """
    Import data into database.
    Mapped tables: eperson, metadatavalue
    """
    json_name = 'eperson.json'
    url = 'clarin/import/eperson'
    imported = 0
    json_a = read_json(json_name)
    if not json_a:
        logging.info("Eperson JSON is empty.")
        return
    for i in json_a:
        metadata_val = metadata.get_metadata_value(metadatavalue, metadata_field_id, 7, i['eperson_id'])
        json_p = {'selfRegistered': i['self_registered'], 'requireCertificate': i['require_certificate'],
                  'netid': i['netid'], 'canLogIn': i['can_log_in'], 'lastActive': i['last_active'],
                  'email': i['email'], 'password': i['password'], 'welcomeInfo': i['welcome_info'],
                  'canEditSubmissionMetadata': i['can_edit_submission_metadata']}
        email2epersonId[i['email']] = i['eperson_id']
        if metadata_val:
            json_p['metadata'] = metadata_val
        param = {'selfRegistered': i['self_registered'], 'lastActive': i['last_active']}
        try:
            response = do_api_post(url, param, json_p)
            eperson_id[i['eperson_id']] = convert_response_to_json(response)['id']
            imported += 1
        except Exception as e:
            logging.error('POST request ' + response.url + ' for id: ' + str(i['eperson_id']) +
                          ' failed. Status: ' + str(response.status_code))

    statistics['eperson'] = (len(json_a), imported)
    logging.info("Eperson was successfully imported!")


def import_group2eperson(eperson_id, group_id, statistics):
    """
    Import data into database.
    Mapped tables: epersongroup2eperson
    """
    json_name = 'epersongroup2eperson.json'
    url = 'clarin/eperson/groups/'
    imported = 0
    json_a = read_json(json_name)
    if not json_a:
        logging.info("Epersongroup2eperson JSON is empty.")
        return
    for i in json_a:
        try:
            do_api_post(url + group_id[i['eperson_group_id']][0] + '/epersons', None,
                        API_URL + 'eperson/groups/' + eperson_id[i['eperson_id']])
            imported += 1
        except Exception as e:
            json_e = json.loads(e.args[0])
            logging.error('POST request ' + json_e['path'] + ' failed. Status: ' + str(json_e['status']))

    statistics['epersongroup2eperson'] = (len(json_a), imported)
    logging.info("Epersongroup2eperson was successfully imported!")

        
