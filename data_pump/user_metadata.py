import logging
from data_pump.utils import read_json, do_api_post


def import_user_metadata(bitstream_id_dict,
                         user_registration_id_dict,
                         statistics_dict):
    """
    Import data into database.
    Mapped tables: user_metadata, license_resource_user_allowance
    """
    user_met_url = 'clarin/import/usermetadata'
    user_met_json_name = 'user_metadata.json'
    imported_user_met = 0
    # read license_resource_user_allowance
    # mapping transaction_id to mapping_id
    user_allowance_dict = {}
    user_allowance_json_list = read_json("license_resource_user_allowance.json")
    if not user_allowance_json_list:
        logging.info("License_resource_user_allowance JSON is empty.")
        return
    for user_allowance in user_allowance_json_list:
        user_allowance_dict[user_allowance['transaction_id']] = user_allowance

    # read license_resource_mapping
    # mapping bitstream_id to mapping_id
    resource_mapping_json_list = read_json('license_resource_mapping.json')
    mappings_dict = {}
    if not resource_mapping_json_list:
        logging.info("License_resource_mapping JSON is empty.")
        return
    for resource_mapping in resource_mapping_json_list:
        mappings_dict[resource_mapping['mapping_id']] = resource_mapping['bitstream_id']

    # read user_metadata
    user_met_json_list = read_json(user_met_json_name)
    if not user_met_json_list:
        logging.info("User_metadata JSON is empty.")
        return

    # Group user metadata by `transaction_id`. The endpoint must receive list of all metadata with the same
    # transaction_id` because if the endpoint will be called for every `user_metadata` there will be a huge amount
    # of `license_resource_user_allowance` records with not correct mapping with the `user_metadata` table.
    user_met_json_dict = {}
    for user_met in user_met_json_list:
        if user_met['transaction_id'] not in user_allowance_dict:
            continue

        # If the user_metadata with transaction_id has some values in the list - append the list, otherwise
        # create a new one
        if user_met['transaction_id'] in user_met_json_dict:
            user_met_json_dict[user_met['transaction_id']].append(user_met)
        else:
            user_met_json_dict[user_met['transaction_id']] = [user_met]

    # Go through dict and import user_metadata
    for user_met_key in user_met_json_dict.keys():
        # Get list of all user_metadata following `transaction_id`
        user_met_list = user_met_json_dict[user_met_key]
        # Get user_registration data for importing
        data_user_all_dict = user_allowance_dict[user_met_list[0]['transaction_id']]
        # Get `eperson_id` for importing
        eperson_id = user_met_list[0]['eperson_id']

        # Prepare user_metadata list for request
        user_met_list_request = []
        for user_met in user_met_list:
            user_met_list_request.append(
                {'metadataKey': user_met['metadata_key'],
                 'metadataValue': user_met['metadata_value']
                 })

        try:
            # Prepare params for the import endpoint
            params = {
                'bitstreamUUID': bitstream_id_dict[mappings_dict[
                    data_user_all_dict['mapping_id']]],
                'createdOn': data_user_all_dict['created_on'],
                'token': data_user_all_dict['token'],
                'userRegistrationId': user_registration_id_dict[eperson_id]
            }
            response = do_api_post(user_met_url, params, user_met_list_request)
            if response.ok:
                imported_user_met += 1
            else:
                raise Exception(response)
        except Exception as e:
            logging.error('POST response ' + user_met_url +
                          ' failed for user registration id: ' +
                          str(user_met['eperson_id']) +
                          ' and bitstream id: ' +
                          str(mappings_dict[data_user_all_dict['mapping_id']]) +
                          '. Exception: ' + str(e))

    statistics_val = (len(user_met_json_dict), imported_user_met)
    statistics_dict['user_metadata'] = statistics_val
    logging.info("User metadata successfully imported!")
