import logging
from ._utils import read_json, time_method, serialize, deserialize, progress_bar, log_before_import, log_after_import

_logger = logging.getLogger("pump.tasklistitem")


class tasklistitems:
    """
         Mapped table: tasklistitem

        SQL:
    """

    def __init__(self, tl_file_str: str):
        self._tasks = read_json(tl_file_str) or {}
        self._imported = {
            "tasks": 0,
        }

        if len(self._tasks) == 0:
            _logger.info(f"Empty input: [{tl_file_str}].")
            return

    def __len__(self):
        return len(self._tasks)

    @property
    def imported(self):
        return self._imported['tasks']

    @time_method
    def import_to(self, dspace, epersons, items):
        expected = len(self)
        log_key = "tasks"
        log_before_import(log_key, expected)

        for task in progress_bar(self._tasks):
            try:
                params = {
                    'epersonUUID': epersons.uuid(task['eperson_id']),
                    'workflowitem_id': items.wf_id(task['workflow_id'])
                }
                resp = dspace.put_tasklistitem(params)
                self._imported["task"] += 1
            except Exception as e:
                _logger.error(f'put_tasklistitem: [{task}] failed [{str(e)}]')

        log_after_import(log_key, expected, self.imported)

    # =============

    def serialize(self, file_str: str):
        data = {
            "tasks": self._tasks,
            "imported": self._imported,
        }
        serialize(file_str, data)

    def deserialize(self, file_str: str):
        data = deserialize(file_str)
        self._tasks = data["tasks"]
        self._imported = data["imported"]
