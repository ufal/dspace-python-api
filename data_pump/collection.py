import logging

from utils import read_json, convert_response_to_json, do_api_post


def import_collection(metadata_class,
                      handle_class,
                      group_id_dict,
                      community_id_dict,
                      collection_id_dict,
                      collection2logo_dict,
                      statistics_dict):
    """
    Import data into database.
    Mapped tables: collection, community2collection, metadatavalue, handle
    """
    collection_json_name = 'collection.json'
    com2col_json_name = 'community2collection.json'
    metadata_json_name = 'metadatavalue.json'
    collection_url = 'core/collections'
    imported_coll = 0
    imported_group = 0
    collection_json_a = read_json(collection_json_name)
    comm2coll_json_a = read_json(com2col_json_name)
    coll2comm_dict = {}
    if not comm2coll_json_a:
        logging.info("Community2collection JSON is empty.")
        return
    for comm2coll in comm2coll_json_a:
        coll2comm_dict[comm2coll['collection_id']] = comm2coll['community_id']

    # because the role DEFAULT_READ is without old group id in collection
    coll2group_dict = {}
    metadata_json_a = read_json(metadata_json_name)

    if metadata_json_a is not None:
        for metadata in metadata_json_a:
            if metadata['resource_type_id'] == 6 and \
                    'COLLECTION_' in metadata['text_value'] and\
                    '_DEFAULT_READ' in metadata['text_value']:
                text = metadata['text_value']
                positions = [ind for ind, ch in enumerate(text) if ch == '_']
                coll2group_dict[int(text[positions[0] + 1: positions[1]])] = \
                    metadata['resource_id']

    if not collection_json_a:
        logging.info("Collection JSON is empty.")
        return
    for collection in collection_json_a:
        collection_json_p = {}
        metadata_col_dict =\
            metadata_class.get_metadata_value(3, collection['collection_id'])
        if metadata_col_dict:
            collection_json_p['metadata'] = metadata_col_dict
        handle_col = handle_class.get_handle(3, collection['collection_id'])
        if handle_col:
            collection_json_p['handle'] = handle_col
        params = {'parent': community_id_dict[coll2comm_dict[
            collection['collection_id']]]}
        try:
            response = do_api_post(collection_url, params, collection_json_p)
            coll_id = convert_response_to_json(response)['id']
            collection_id_dict[collection['collection_id']] = coll_id
            imported_coll += 1
        except Exception:
            logging.error(
                'POST request ' + response.url + ' for id: ' +
                str(collection['collection_id']) + 'failed. Status: ' +
                str(response.status_code))

        # add to collection2logo, if collection has logo
        if collection['logo_bitstream_id'] is not None:
            collection2logo_dict[collection['collection_id']] = \
                collection["logo_bitstream_id"]

        # greate group
        # template_item_id, workflow_step_1, workflow_step_3, admin are not implemented,
        # because they are null in all data
        if collection['workflow_step_2']:
            try:
                response = do_api_post(collection_url + coll_id +
                                       '/workflowGroups/editor', None, {})
                group_id_dict[collection['workflow_step_2']] = [
                    convert_response_to_json(response)['id']]
                imported_group += 1
            except Exception:
                logging.error('POST request ' + response.url +
                              ' failed. Status: ' + str(response.status_code))
        if collection['submitter']:
            try:
                response = do_api_post(collection_url + coll_id + '/submittersGroup',
                                       None, {})
                group_id_dict[collection['submitter']] = \
                    [convert_response_to_json(response)['id']]
                imported_group += 1
            except Exception:
                logging.error('POST request ' + response.url +
                              ' failed. Status: ' + str(response.status_code))
        if collection['collection_id'] in coll2group_dict:
            try:
                response = do_api_post(collection_url + coll_id +
                                       '/bitstreamReadGroup', None, {})
                group_id_dict[coll2group_dict[collection['collection_id']]] = [
                    convert_response_to_json(response)['id']]
                imported_group += 1
            except Exception:
                logging.error('POST request ' + response.url +
                              ' failed. Status: ' + str(response.status_code))
            try:
                response = do_api_post(collection_url + coll_id +
                                       '/itemReadGroup', None, {})
                group_id_dict[coll2group_dict[collection['collection_id']]].append(
                    convert_response_to_json(response)['id'])
                imported_group += 1
            except Exception:
                logging.error('POST request ' + response.url +
                              ' failed. Status: ' + str(response.status_code))

    statistics_val = (len(collection_json_a), imported_coll)
    statistics_dict['collection'] = statistics_val
    statistics_val = (0, statistics_dict['epersongroup'][1] + imported_group)
    statistics_dict['epersongroup'] = statistics_val
    logging.info("Collection and Community2collection were successfully imported!")
