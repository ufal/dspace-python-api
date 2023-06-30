import logging
import json

from const import API_URL
from utils import read_json, convert_response_to_json, do_api_post


def import_eperson(metadata_class, eperson_id_dict, email2epersonId_dict,
                   statistics_dict):
    """
    Import data into database.
    Mapped tables: eperson, metadatavalue
    """
    eperson_json_name = 'eperson.json'
    eperson_url = 'clarin/import/eperson'
    imported_eperson = 0
    eperson_json_a = read_json(eperson_json_name)
    if not eperson_json_a:
        logging.info("Eperson JSON is empty.")
        return
    for i in eperson_json_a:
        metadatavalue_eperson_dict = metadata_class.get_metadata_value(7,
                                                                       i['eperson_id'])
        eperson_json_p = {'selfRegistered': i['self_registered'],
                          'requireCertificate': i['require_certificate'],
                          'netid': i['netid'], 'canLogIn': i['can_log_in'],
                          'lastActive': i['last_active'], 'email': i['email'],
                          'password': i['password'], 'welcomeInfo': i['welcome_info'],
                          'canEditSubmissionMetadata':
                              i['can_edit_submission_metadata']}
        email2epersonId_dict[i['email']] = i['eperson_id']
        if metadatavalue_eperson_dict:
            eperson_json_p['metadata'] = metadatavalue_eperson_dict
        params = {'selfRegistered': i['self_registered'],
                  'lastActive': i['last_active']}
        try:
            response = do_api_post(eperson_url, params, eperson_json_p)
            eperson_id_dict[i['eperson_id']] = convert_response_to_json(response)['id']
            imported_eperson += 1
        except Exception:
            logging.error('POST request ' + response.url + ' for id: ' +
                          str(i['eperson_id']) +
                          ' failed. Status: ' + str(response.status_code))

    statistics_val = (len(eperson_json_a), imported_eperson)
    statistics_dict['eperson'] = statistics_val
    logging.info("Eperson was successfully imported!")


def import_group2eperson(eperson_id_dict, group_id_dict, statistics_dict):
    """
    Import data into database.
    Mapped tables: epersongroup2eperson
    """
    group2eperson_json_name = 'epersongroup2eperson.json'
    group2eperson_url = 'clarin/eperson/groups/'
    imported_group2eper = 0
    group2eperson_json_a = read_json(group2eperson_json_name)
    if not group2eperson_json_a:
        logging.info("Epersongroup2eperson JSON is empty.")
        return
    for i in group2eperson_json_a:
        try:
            do_api_post(group2eperson_url + group_id_dict[i['eperson_group_id']][0] +
                        '/epersons', None,
                        API_URL + 'eperson/groups/' + eperson_id_dict[i['eperson_id']])
            imported_group2eper += 1
        except Exception as e:
            e_json = json.loads(e.args[0])
            logging.error('POST request ' +
                          e_json['path'] + ' failed. Status: ' + str(e_json['status']))

    statistics_val = (len(group2eperson_json_a), imported_group2eper)
    statistics_dict['epersongroup2eperson'] = statistics_val
    logging.info("Epersongroup2eperson was successfully imported!")
