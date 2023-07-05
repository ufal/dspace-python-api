import logging

from const import API_URL
from utils import read_json, convert_response_to_json, do_api_post, save_dict_as_json


def import_eperson(metadata_class,
                   eperson_id_dict,
                   email2epersonId_dict,
                   statistics_dict,
                   save_dict=False):
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
    for eperson in eperson_json_a:
        metadatavalue_eperson_dict = \
            metadata_class.get_metadata_value(7, eperson['eperson_id'])
        eperson_json_p = {
            'selfRegistered': eperson['self_registered'],
            'requireCertificate': eperson['require_certificate'],
            'netid': eperson['netid'],
            'canLogIn': eperson['can_log_in'],
            'lastActive': eperson['last_active'],
            'email': eperson['email'],
            'password': eperson['password'],
            'welcomeInfo': eperson['welcome_info'],
            'canEditSubmissionMetadata': eperson['can_edit_submission_metadata']
        }
        email2epersonId_dict[eperson['email']] = eperson['eperson_id']
        if metadatavalue_eperson_dict:
            eperson_json_p['metadata'] = metadatavalue_eperson_dict
        params = {
            'selfRegistered': eperson['self_registered'],
            'lastActive': eperson['last_active']
        }
        try:
            response = do_api_post(eperson_url, params, eperson_json_p)
            eperson_id_dict[eperson['eperson_id']] = convert_response_to_json(
                response)['id']
            imported_eperson += 1
        except Exception as e:
            logging.error('POST request ' + eperson_url + ' for id: ' +
                          str(eperson['eperson_id']) +
                          ' failed. Exception: ' + str(e))

    # save eperson dict as json
    if save_dict:
        save_dict_as_json(eperson_json_name, eperson_id_dict)
    statistics_val = (len(eperson_json_a), imported_eperson)
    statistics_dict['eperson'] = statistics_val
    logging.info("Eperson was successfully imported!")


def import_group2eperson(eperson_id_dict,
                         group_id_dict,
                         statistics_dict):
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
    for group2eperson in group2eperson_json_a:
        group_url = group2eperson_url
        try:
            group_url += group_id_dict[group2eperson['eperson_group_id']][0] + \
                '/epersons'
            eperson_url = API_URL + 'eperson/groups/' + eperson_id_dict[
                group2eperson['eperson_id']]
            do_api_post(group_url, {}, eperson_url)
            imported_group2eper += 1
        except Exception as e:
            logging.error('POST request ' +
                          group_url + ' failed. Exception: ' + str(e))

    statistics_val = (len(group2eperson_json_a), imported_group2eper)
    statistics_dict['epersongroup2eperson'] = statistics_val
    logging.info("Epersongroup2eperson was successfully imported!")
