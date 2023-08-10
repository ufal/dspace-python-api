import logging

from data_pump.utils import do_api_post, read_json


class Handle:
    def __init__(self):
        self.handle_dict = {}
        self.imported_handle = 0
        self.read_handle()
        self.import_handle_with_url()
        self.import_handle_without_object()

    def read_handle(self):
        """
        Read handle as json and convert it to dictionary wth tuple key:
        resource_type_id and resource_type,
        where value is list of jsons.
        """
        handle_json_name = 'handle.json'
        handle_json_list = read_json(handle_json_name)
        if not handle_json_list:
            logging.info('Handle JSON is empty.')
            return
        for handle in handle_json_list:
            key = (handle['resource_type_id'], handle['resource_id'])
            if key in self.handle_dict.keys():
                self.handle_dict[key].append(handle)
            else:
                self.handle_dict[key] = [handle]

    def import_handle_with_url(self):
        """
        Import handles into database with url.
        Other handles are imported by dspace objects.
        Mapped table: handles
        """
        handle_url = 'core/handles'
        # handle with defined url has key (None, None)
        if (None, None) not in self.handle_dict:
            logging.info("Handles with url don't exist.")
            return
        handles_a = self.handle_dict[(None, None)]
        for handle in handles_a:
            handle_json_p = {
                'handle': handle['handle'],
                'url': handle['url']
            }
            try:
                response = do_api_post(handle_url, {}, handle_json_p)
                if response.ok:
                    self.imported_handle += 1
                else:
                    raise Exception(response)
            except Exception as e:
                logging.error('POST response ' + handle_url + ' for handle: ' +
                              handle['handle'] + ' failed. Exception: ' + str(e))

        logging.info("Handles with url were successfully imported!")

    def import_handle_without_object(self):
        """
        Import handles which have not objects into database.
        Other handles are imported by dspace objects.
        Mapped table: handles
        """
        handle_url = 'clarin/import/handle'
        if (2, None) not in self.handle_dict:
            logging.info("Handles without objects don't exist.")
            return

        handles_a = self.handle_dict[(2, None)]
        for handle in handles_a:
            handle_json_p = {
                'handle': handle['handle'],
                'resourceTypeID': handle['resource_type_id']
            }
            try:
                do_api_post(handle_url, {}, handle_json_p)
                self.imported_handle += 1
            except Exception as e:
                logging.error(
                    'POST response ' + handle_url + ' failed. Exception: ' + str(e))

        logging.info("Handles without object were successfully imported!")

    def get_handle(self, obj_type_int, obj_id):
        """
        Get handle based on object type and its id.
        """
        if (obj_type_int, obj_id) in self.handle_dict:
            self.imported_handle += 1
            return self.handle_dict[(obj_type_int, obj_id)][0]['handle']
        else:
            return None

    def get_imported_handle(self):
        return self.imported_handle
