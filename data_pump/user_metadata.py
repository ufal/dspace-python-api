import logging
from utils import read_json, do_api_post


def import_user_metadata(bitstream_id, userRegistration_id, statistics):
    """
        Import data into database.
        Mapped tables: user_metadata, license_resource_user_allowance
        """
    imported = 0
    # read license_resource_user_allowance
    # mapping transaction_id to mapping_id
    user_allowance = dict()
    json_a = read_json("license_resource_user_allowance.json")
    if not json_a:
        logging.info("License_resource_user_allowance JSON is empty.")
        return
    for i in json_a:
        user_allowance[i['transaction_id']] = i

    # read license_resource_mapping
    # mapping bitstream_id to mapping_id
    resource_mapping = read_json('license_resource_mapping.json')
    mappings = dict()
    if not resource_mapping:
        logging.info("License_resource_mapping JSON is empty.")
        return
    for i in resource_mapping:
        mappings[i['mapping_id']] = i['bitstream_id']

    # read user_metadata
    json_a = read_json("user_metadata.json")
    if not json_a:
        logging.info("User_metadata JSON is empty.")
        return
    for i in json_a:
        if i['transaction_id'] not in user_allowance:
            continue
        dataUA = user_allowance[i['transaction_id']]
        json_p = [{'metadataKey': i['metadata_key'], 'metadataValue': i['metadata_value']}]
        try:
            param = {'bitstreamUUID': bitstream_id[mappings[dataUA['mapping_id']]],
                     'createdOn': dataUA['created_on'], 'token': dataUA['token'],
                     'userRegistrationId': userRegistration_id[i['eperson_id']]}
            do_api_post('clarin/import/usermetadata', param, json_p)
            imported += 1
        except Exception as e:
            logging.error('POST response clarin/import/usermetadata failed for user registration id: ' + str(
                i['eperson_id'])
                          + ' and bitstream id: ' + str(mappings[dataUA['mapping_id']]))

    statistics['user_metadata'] = (len(json_a), imported)
    logging.info("User metadata successfully imported!")
