import logging

from const import API_URL
from data_pump.utils import read_json, convert_response_to_json, do_api_get_all, \
    do_api_post, save_dict_as_json


def import_epersongroup(metadata_class,
                        group_id_dict,
                        statistics_dict,
                        save_dict):
    """
    Import data into database.
    Mapped tables: epersongroup
    """
    group_json_name = 'epersongroup.json'
    saved_group_json_name = 'epersongroup_dict.json'
    group_url = 'eperson/groups'
    imported = 0
    group_json_list = read_json(group_json_name)
    # group Administrator and Anonymous already exist
    # we need to remember their id if we haven't done it yet
    # load_admin_anonymous_groups(var.group_id_dict)
    if not group_json_list:
        logging.info("Epersongroup JSON is empty.")
        return
    for group in group_json_list:
        group_id = group['eperson_group_id']
        # group Administrator and Anonymous already exist
        # group is created with dspace object too
        if group_id not in (0, 1) and group_id not in group_id_dict:
            # get group metadata
            metadatavalue_group_dict = \
                metadata_class.get_metadata_value(6, group['eperson_group_id'])
            if 'dc.title' not in metadatavalue_group_dict:
                logging.error('Metadata for group ' + str(group_id) +
                              ' does not contain title!')
                continue
            name = metadatavalue_group_dict['dc.title'][0]['value']
            del metadatavalue_group_dict['dc.title']
            # the group_metadata contains the name of the group
            json_p = {'name': name, 'metadata': metadatavalue_group_dict}
            try:
                response = do_api_post(group_url, {}, json_p)
                group_id_dict[group['eperson_group_id']] = [
                    convert_response_to_json(response)['id']]
                imported += 1
            except Exception as e:
                logging.error('POST request ' + group_url + ' for id: ' +
                              str(group['eperson_group_id']) +
                              ' failed. Exception: ' + str(e))

    # save group dict as json
    if save_dict:
        save_dict_as_json(saved_group_json_name, group_id_dict)

    if 'epersongroup' in statistics_dict:
        statistics_val = (len(group_json_list), statistics_dict['epersongroup'][1] +
                          imported)
        statistics_dict['epersongroup'] = statistics_val
    else:
        statistics_val = (len(group_json_list), imported)
        statistics_dict['epersongroup'] = statistics_val
    logging.info("Eperson group was successfully imported!")


def get_existing_epersongroups(group_url):
    """
    Get all existing eperson groups from database.
    """
    existing_data_dict = None
    try:
        response = do_api_get_all(group_url)
        existing_data_dict = convert_response_to_json(response)['_embedded']['groups']
    except Exception as e:
        logging.error('GET request ' + group_url + ' failed. Exception: ' + str(e))
    return existing_data_dict


def import_group2group(group_id_dict,
                       statistics_dict):
    """
    Import data into database.
    Mapped tables: group2group
    """
    group2group_json_name = 'group2group.json'
    group2group_url = 'clarin/eperson/groups'
    imported = 0
    group2group_json_list = read_json(group2group_json_name)
    if not group2group_json_list:
        logging.info("Group2group JSON is empty.")
        return

    for group2group in group2group_json_list:
        parents_a = group_id_dict[group2group['parent_id']]
        childs_a = group_id_dict[group2group['child_id']]
        for parent in parents_a:
            for child in childs_a:
                parent_url = group2group_url + '/' + parent + '/subgroups'
                try:
                    child_url = API_URL + 'eperson/groups/' + child
                    response = do_api_post(parent_url, {}, child_url)
                    if response.ok:
                        imported += 1
                    else:
                        raise Exception(response)
                except Exception as e:
                    logging.error('POST request ' + parent_url + ' for id: ' +
                                  str(parent) + ' failed. Exception: ' + str(e))

    statistics_val = (len(group2group_json_list), imported)
    statistics_dict['group2group'] = statistics_val
    logging.info("Group2group was successfully imported!")

def load_admin_anonymous_groups(group_id_dict):
    """
    Load Administrator and Anonymous groups into dict.
    This data already exists in database.
    Remember its id.
    """
    group_url = 'eperson/groups'
    existing_data_dict = get_existing_epersongroups(group_url)
    if existing_data_dict is not None:
        for existing_data in existing_data_dict:
            if existing_data['name'] == 'Anonymous':
                group_id_dict[0] = [existing_data['id']]
            elif existing_data['name'] == 'Administrator':
                group_id_dict[1] = [existing_data['id']]
            else:
                logging.error('Unrecognized eperson group ' + existing_data['name'])

def get_existing_epersongroups(group_url):
    """
    Get all existing eperson groups from database.
    """
    existing_data_dict = None
    try:
        response = do_api_get_all(group_url)
        existing_data_dict = convert_response_to_json(response)['_embedded']['groups']
    except Exception as e:
        logging.error('GET request ' + group_url + ' failed. Exception: ' + str(e))
    return existing_data_dict
