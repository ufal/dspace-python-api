import logging

from const import API_URL
from utils import read_json, convert_response_to_json, do_api_get_all, do_api_post


def import_epersongroup(metadata, group_id, metadatavalue, metadata_field_id, statistics):
    """
    Import data into database.
    Mapped tables: epersongroup
    """
    json_name = 'epersongroup.json'
    url_all = 'eperson/groups'
    url = 'eperson/groups'
    imported = 0
    json_a = read_json(json_name)
    # group Administrator and Anonymous already exist
    # we need to remember their id
    try:
        response = do_api_get_all(url_all)
        existing_data = convert_response_to_json(response)['_embedded']['groups']
    except Exception:
        logging.error('GET request ' + response.url + ' failed.')

    if existing_data:
        for i in existing_data:
            if i['name'] == 'Anonymous':
                group_id[0] = [i['id']]
            elif i['name'] == 'Administrator':
                group_id[1] = [i['id']]
            else:
                logging.error('Unrecognized eperson group ' + i['name'])

    if not json_a:
        logging.info("Epersongroup JSON is empty.")
        return
    for i in json_a:
        id = i['eperson_group_id']
        # group Administrator and Anonymous already exist
        # group is created with dspace object too
        if id != 0 and id != 1 and id not in group_id:
            # get group metadata
            metadata_group = metadata.get_metadata_value(
                metadatavalue, metadata_field_id, 6, i['eperson_group_id'])
            name = metadata_group['dc.title'][0]['value']
            del metadata_group['dc.title']
            # the group_metadata contains the name of the group
            json_p = {'name': name, 'metadata': metadata_group}
            try:
                response = do_api_post(url, None, json_p)
                group_id[i['eperson_group_id']] = [
                    convert_response_to_json(response)['id']]
                imported += 1
            except Exception:
                logging.error('POST request ' + response.url + ' for id: ' + str(i['eperson_group_id']) +
                              ' failed. Status: ' + str(response.status_code))
    if 'epersongroup' in statistics:
        statistics['epersongroup'] = (
            len(json_a), statistics['epersongroup'][1] + imported)
    else:
        statistics['epersongroup'] = (len(json_a), imported)
    logging.info("Eperson group was successfully imported!")


def import_group2group(group_id, statistics):
    """
    Import data into database.
    Mapped tables: group2group
    """
    json_name = 'group2group.json'
    url = 'clarin/eperson/groups/'
    imported = 0
    json_a = read_json(json_name)
    if not json_a:
        logging.info("Group2group JSON is empty.")
        return
    for i in json_a:
        parents = group_id[i['parent_id']]
        childs = group_id[i['child_id']]
        for parent in parents:
            for child in childs:
                try:
                    response = do_api_post(url + parent + '/subgroups', None,
                                           API_URL + 'eperson/groups/' + child)
                    imported += 1
                except Exception as e:
                    # Sometimes the Exception `e` is type of `int`
                    if isinstance(e, int):
                        logging.error('POST request ' + url + parent + '/subgroups' +
                                      ' failed.')
                    else:
                        logging.error('POST request ' + response.url + ' for id: ' + str(parent) +
                                      ' failed. Status: ' + str(response.status_code))
    statistics['group2group'] = (len(json_a), imported)
    logging.info("Group2group was successfully imported!")
