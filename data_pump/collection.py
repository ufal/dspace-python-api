import logging

from utils import read_json, convert_response_to_json, do_api_post


def import_collection(metadata, group_id, handle, community_id, collection_id,
                      collection2logo, imported_handle, metadatavalue, metadata_field_id, statistics):
    """
    Import data into database.
    Mapped tables: collection, community2collection, metadatavalue, handle
    """
    json_name_col = 'collection.json'
    json_name_com2col = 'community2collection.json'
    json_name_metadata = 'metadatavalue.json'
    url_col ='core/collections'
    url_step = 'core/collections/'
    importedColl = 0
    importedGroup = 0
    json_a = read_json(json_name_col)
    comm_2_coll_json = read_json(json_name_com2col)
    coll2comm = dict()
    if not comm_2_coll_json:
        logging.info("Community2collection JSON is empty.")
        return
    for i in comm_2_coll_json:
        coll2comm[i['collection_id']] = i['community_id']

    # because the role DEFAULT_READ is without old group id in collection
    coll2group = dict()
    metadata_json = read_json(json_name_metadata)

    if metadata_json:
        for i in metadata_json:
            if i['resource_type_id'] == 6 and 'COLLECTION_' in i['text_value'] and '_DEFAULT_READ' in i[
                'text_value']:
                text = i['text_value']
                positions = [ind for ind, ch in enumerate(text) if ch == '_']
                coll2group[int(text[positions[0] + 1: positions[1]])] = i['resource_id']
    if not json_a:
        logging.info("Collection JSON is empty.")
        return
    for i in json_a:
        json_p = {}
        metadata_col = metadata.get_metadata_value(metadatavalue, metadata_field_id, 3, i['collection_id'])
        if metadata_col:
            json_p['metadata'] = metadata_col
        if (3, i['collection_id']) in handle:
            handle_col = handle[(3, i['collection_id'])][0]
            json_p['handle'] = handle_col['handle']
            imported_handle += 1
        params = {'parent': community_id[coll2comm[i['collection_id']]]}
        try:
            response = do_api_post(url_col, params, json_p)
            coll_id = convert_response_to_json(response)['id']
            collection_id[i['collection_id']] = coll_id
            importedColl += 1
        except Exception as e:
            logging.error(
                'POST request ' + response.url + ' for id: ' + str(i['collection_id']) + 'failed. Status: ' +
                str(response.status_code))

        # add to collection2logo, if collection has logo
        if i["logo_bitstream_id"] != None:
            collection2logo[i['collection_id']] = i["logo_bitstream_id"]

        # greate group
        # template_item_id, workflow_step_1, workflow_step_3, admin are not implemented,
        # because they are null in all data
        if i['workflow_step_2']:
            try:
                response = do_api_post(url_step + coll_id + '/workflowGroups/editor', None, {})
                group_id[i['workflow_step_2']] = [convert_response_to_json(response)['id']]
                importedGroup += 1
            except Exception as e:
                logging.error('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))
        if i['submitter']:
            try:
                response = do_api_post(url_step + coll_id + '/submittersGroup', None, {})
                group_id[i['submitter']] = [convert_response_to_json(response)['id']]
                importedGroup += 1
            except Exception as e:
                logging.error('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))
        if i['collection_id'] in coll2group:
            try:
                response = do_api_post(url_step + coll_id + '/bitstreamReadGroup', None, {})
                group_id[coll2group[i['collection_id']]] = [convert_response_to_json(response)['id']]
                importedGroup += 1
            except Exception as e:
                logging.error('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))
            try:
                response = do_api_post(url_step + coll_id + '/itemReadGroup', None, {})
                group_id[coll2group[i['collection_id']]].append(convert_response_to_json(response)['id'])
                importedGroup += 1
            except Exception as e:
                logging.error('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))

    statistics['collection'] = (len(json_a), importedColl)
    statistics['epersongroup'] = (0, statistics['epersongroup'][1] + importedGroup)
    logging.info("Collection and Community2collection were successfully imported!")
