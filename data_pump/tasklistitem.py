import logging

from utils import read_json, do_api_post


def import_tasklistitem(workflowitem_id, eperson_id, statistics):
    """
     Import data into database.
     Mapped table: tasklistitem
     """
    json_name = "tasklistitem.json"
    url = 'clarin/eperson/groups/tasklistitem'
    imported = 0
    json_a = read_json(json_name)
    if not json_a:
        logging.info("Tasklistitem JSON is empty.")
        return
    for i in json_a:
        try:
            params = {'epersonUUID': eperson_id[i['eperson_id']],
                      'workflowitem_id': workflowitem_id[i['workflow_id']]}
            response = do_api_post(url, params, None)
            imported += 1
        except Exception as e:
            logging.error('POST request clarin/eperson/groups/tasklistitem failed.')
    statistics['tasklistitem'] = (len(json_a), imported)
    logging.info("Tasklistitem was sucessfully imported!")
