import logging

from data_pump.utils import read_json, do_api_post


def import_tasklistitem(workflowitem_id_dict,
                        eperson_id_dict,
                        statistics_dict):
    """
     Import data into database.
     Mapped table: tasklistitem
     """
    tasklistitem_json_name = "tasklistitem.json"
    tasklistitem_url = 'clarin/eperson/groups/tasklistitem'
    imported_tasklistitem = 0
    tasklistitem_json_list = read_json(tasklistitem_json_name)
    if not tasklistitem_json_list:
        logging.info("Tasklistitem JSON is empty.")
        return
    for tasklistitem in tasklistitem_json_list:
        try:
            params = {
                'epersonUUID': eperson_id_dict[tasklistitem['eperson_id']],
                'workflowitem_id': workflowitem_id_dict[tasklistitem['workflow_id']]
            }
            response = do_api_post(tasklistitem_url, params, None)
            if response.ok:
                imported_tasklistitem += 1
            else:
                raise Exception(response)
        except Exception as e:
            logging.error('POST request ' + tasklistitem_url + ' failed. Exception: ' +
                          str(e))

    statistics_val = (len(tasklistitem_json_list), imported_tasklistitem)
    statistics_dict['tasklistitem'] = statistics_val
    logging.info("Tasklistitem was sucessfully imported!")
