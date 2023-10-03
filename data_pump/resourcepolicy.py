import logging
import psycopg2

from data_pump.utils import read_json, convert_response_to_json, do_api_post

from const import CLARIN_DSPACE_7_NAME, CLARIN_DSPACE_7_HOST, \
    CLARIN_DSPACE_7_USER, CLARIN_DSPACE_7_PASSWORD, COMMUNITY, COLLECTION,\
    ITEM, BUNDLE, BITSTREAM
from migration_const import ACTIONS_LIST


def import_resource_policies(community_id_dict,
                             collection_id_dict,
                             item_id_dict,
                             bundle_id_dict,
                             bitstream_id_dict,
                             eperson_id_dict,
                             group_id_dict,
                             statistics_dict):
    res_policy_json_name = 'resourcepolicy.json'
    res_policy_url = 'authz/resourcepolicies'
    res_policy_json_list = read_json(res_policy_json_name)
    imported = 0
    unimported = 0
    def_read = 0
    for res_policy in res_policy_json_list:
        params = {}
        try:
            # find object id based on its type
            type = res_policy['resource_type_id']
            if type == COMMUNITY:
                params['resource'] = community_id_dict[res_policy['resource_id']]
            elif type == COLLECTION:
                params['resource'] = collection_id_dict[res_policy['resource_id']]
            elif type == ITEM:
                params['resource'] = item_id_dict[res_policy['resource_id']]
            elif type == BUNDLE:
                params['resource'] = bundle_id_dict[res_policy['resource_id']]
            elif type == BITSTREAM:
                params['resource'] = bitstream_id_dict[res_policy['resource_id']]
            # in resource there is action as id, but we need action as text
            actionId = res_policy['action_id']
            # control, if action is entered correctly
            if actionId < 0 or actionId >= len(ACTIONS_LIST):
                logging.error('Cannot do POST request ' + res_policy_url + ' for id: ' +
                              str(res_policy['policy_id']) + ' because action id: '
                              + str(actionId) + ' does not exist.')
                unimported += 1
                continue
            # create object for request
            json_p = {'action': ACTIONS_LIST[actionId], 'startDate':
                      res_policy['start_date'],
                      'endDate': res_policy['end_date'], 'name': res_policy['rpname'],
                      'policyType': res_policy['rptype'], 'description':
                          res_policy['rpdescription']}
            # resource policy has defined eperson or group, not the both
            # get eperson if it is not none
            if res_policy['eperson_id'] is not None:
                params['eperson'] = eperson_id_dict[res_policy['eperson_id']]
                # create resource policy
                response = do_api_post(res_policy_url, params, json_p)
                response = convert_response_to_json(response)
                response['id']
                imported += 1
                continue

            # get group if it is not none
            elif res_policy['epersongroup_id'] is not None:
                group_list = group_id_dict[res_policy['epersongroup_id']]
                if len(group_list) > 1:
                    def_read += 1
                for group in group_list:
                    params['group'] = group
                    response = do_api_post(res_policy_url, params, json_p)
                    response = convert_response_to_json(response)
                    response['id']
                    imported += 1
            else:
                logging.error('Cannot do POST request ' + res_policy_url + ' for id: ' +
                              str(res_policy['policy_id']) +
                              ' because neither eperson nor group is defined.')
                unimported += 1
                continue
        except Exception as e:
            logging.error('POST request ' + res_policy_url + ' for id: ' +
                          str(res_policy['policy_id']) + ' '
                                                         'failed. Exception: ' + str(e))
            unimported += 1

    # write statistic
    statistics_dict['resourcepolicy'] = {'expected: ': len(res_policy_json_list),
                                         'imported': imported,
                                         'duplicated': def_read,
                                         'unimported': unimported}


def delete_all_resource_policy():
    # create database connection
    conn = psycopg2.connect(database=CLARIN_DSPACE_7_NAME,
                            host=CLARIN_DSPACE_7_HOST,
                            user=CLARIN_DSPACE_7_USER,
                            password=CLARIN_DSPACE_7_PASSWORD)
    logging.info("Connection to database " + CLARIN_DSPACE_7_NAME + " was successful!")
    # get count of resourcepolicy
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) from public.resourcepolicy"
    )
    # access to 0. position, because the fetchone returns tuple
    expected = cursor.fetchone()[0]
    # delete all data
    cursor.execute(
        "DELETE FROM public.resourcepolicy")
    deleted = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    # control, if we deleted all data
    assert expected == deleted
