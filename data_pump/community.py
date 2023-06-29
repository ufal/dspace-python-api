import logging

from utils import read_json, convert_response_to_json, do_api_post


def import_community(metadata, group_id, handle, community_id, community2logo,
                     imported_handle, metadatavalue, metadata_field_id, statistics):
    """
    Import data into database.
    Mapped tables: community, community2community, metadatavalue, handle
    """
    json_name_com = 'community.json'
    json_name_com2com = 'community2community.json'
    url = 'core/communities'
    importedComm = 0
    importedGroup = 0
    json_comm = read_json(json_name_com)
    json_comm2comm = read_json(json_name_com2com)
    parent = dict()
    child = dict()
    if json_comm2comm:
        for i in json_comm2comm:
            parent_id = i['parent_comm_id']
            child_id = i['child_comm_id']
            if parent_id in parent.keys():
                parent[parent_id].append(child_id)
            else:
                parent[parent_id] = [child_id]
            if child_id in child.keys():
                child[child_id].append(parent_id)
            else:
                child[child_id] = parent_id
        statistics['community'] = (len(json_comm), 0)
    if not json_comm:
        logging.info("Community JSON is empty.")
        return
    counter = 0
    while json_comm:
        json_p = {}
        # process community only when:
        # comm is not parent and child
        # comm is parent and not child
        # parent comm exists
        # else process it later
        i = json_comm[counter]
        i_id = i['community_id']
        if (i_id not in parent.keys() and i_id not in child.keys()) or i_id not in child.keys() or child[
            i_id] in community_id.keys():
            # resource_type_id for community is 4
            if (4, i['community_id']) in handle:
                handle_comm = handle[(4, i['community_id'])][0]
                json_p['handle'] = handle_comm['handle']
                imported_handle += 1
            metadatavalue_comm = metadata.get_metadata_value(metadatavalue, metadata_field_id, 4, i['community_id'])
            if metadatavalue_comm:
                json_p['metadata'] = metadatavalue_comm
            # create community
            parent_id = None
            if i_id in child:
                parent_id = {'parent': community_id[child[i_id]]}
            try:
                response = do_api_post(url, parent_id, json_p)
                resp_community_id = convert_response_to_json(response)['id']
                community_id[i['community_id']] = resp_community_id
                importedComm += 1
            except Exception as e:
                logging.error('POST request ' + response.url + ' for id: ' + str(i_id) + ' failed. Status: ' +
                              str(response.status_code))

            # add to community2logo, if community has logo
            if i["logo_bitstream_id"] != None:
                community2logo[i_id] = i["logo_bitstream_id"]

            # create admingroup
            if i['admin'] != None:
                try:
                    response = do_api_post('core/communities/' + resp_community_id + '/adminGroup', None, {})
                    group_id[i['admin']] = [convert_response_to_json(response)['id']]
                    importedGroup += 1
                except Exception as e:
                    logging.error('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))
            del json_comm[counter]
        else:
            counter += 1
        if counter == len(json_comm):
            counter = 0

    if 'community' in statistics:
        statistics['community'] = (statistics['community'][0], importedComm)
    statistics['epersongroup'] = (0, importedGroup)
    logging.info("Community and Community2Community were successfully imported!")
