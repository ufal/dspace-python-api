import logging
from utils import read_json, do_api_post


def import_user_metadata(bitstream_id_dict, userRegistration_id_dict, statistics_dict):
    """
        Import data into database.
        Mapped tables: user_metadata, license_resource_user_allowance
        """
    user_met_url = 'clarin/import/usermetadata'
    imported_user_met = 0
    # read license_resource_user_allowance
    # mapping transaction_id to mapping_id
    user_allowance_dict = dict()
    user_allowance_json_a = read_json("license_resource_user_allowance.json")
    if not user_allowance_json_a:
        logging.info("License_resource_user_allowance JSON is empty.")
        return
    for i in user_allowance_json_a:
        user_allowance_dict[i['transaction_id']] = i

    # read license_resource_mapping
    # mapping bitstream_id to mapping_id
    resource_mapping_json_a = read_json('license_resource_mapping.json')
    mappings_dict = dict()
    if not resource_mapping_json_a:
        logging.info("License_resource_mapping JSON is empty.")
        return
    for i in resource_mapping_json_a:
        mappings_dict[i['mapping_id']] = i['bitstream_id']

    # read user_metadata
    user_met_json_a = read_json()
    if not user_met_json_a:
        logging.info("User_metadata JSON is empty.")
        return
    for i in user_met_json_a:
        if i['transaction_id'] not in user_allowance_dict:
            continue
        data_user_all_dict = user_allowance_dict[i['transaction_id']]
        user_met_json_p = [{'metadataKey': i['metadata_key'],
                            'metadataValue': i['metadata_value']}]
        try:
            params = {'bitstreamUUID': bitstream_id_dict[mappings_dict[
                data_user_all_dict['mapping_id']]],
                'createdOn': data_user_all_dict['created_on'],
                'token': data_user_all_dict['token'],
                'userRegistrationId': userRegistration_id_dict[i['eperson_id']]}
            do_api_post(user_met_url, params, user_met_json_p)
            imported_user_met += 1
        except Exception:
            logging.error('POST response ' + user_met_url +
                          ' failed for user registration id: ' + str(i['eperson_id']) +
                          ' and bitstream id: ' +
                          str(mappings_dict[data_user_all_dict['mapping_id']]))

    statistics_val = (len(user_met_json_a), imported_user_met)
    statistics_dict['user_metadata'] = statistics_val
    logging.info("User metadata successfully imported!")
