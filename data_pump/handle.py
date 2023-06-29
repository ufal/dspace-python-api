import logging

from utils import do_api_post, read_json


class Handle:
    def __init__(self, handle):
        """
        Read handle as json and convert it to dictionary wth tuple key: resource_type_id and resource_type,
        where value is list of jsons.
        """
        json_name = 'handle.json'
        handle_json = read_json(json_name)
        if not handle_json:
            logging.info('Handle JSON is empty.')
            return
        for i in handle_json:
            key = (i['resource_type_id'], i['resource_id'])
            if key in handle.keys():
                handle[key].append(i)
            else:
                handle[key] = [i]

    def import_handle_with_url(self, handle, imported_handle):
        """
        Import handles into database with url.
        Other handles are imported by dspace objects.
        Mapped table: handles
        """
        url = 'core/handles'
        # handle with defined url has key (None, None)
        if (None, None) not in handle:
            logging.info("Handles with url don't exist.")
            return
        handles_url = handle[(None, None)]
        for i in handles_url:
            json_p = {'handle': i['handle'], 'url': i['url']}
            try:
                response = do_api_post(url, None, json_p)
                imported_handle += 1
            except Exception as e:
                logging.error('POST response ' + response.url + ' failed. Status: ' + str(response.status_code))

        logging.info("Handles with url were successfully imported!")

    def import_handle_without_object(self, handle):
        """
        Import handles which have not objects into database.
        Other handles are imported by dspace objects.
        Mapped table: handles
        """
        url = 'clarin/import/handle'
        if (2, None) not in handle:
            logging.info("Handles without objects don't exist.")
            return

        handles = handle[(2, None)]
        for i in handles:
            json_p = {'handle': i['handle'], 'resourceTypeID': i['resource_type_id']}
            try:
                response = do_api_post(url, None, json_p)
            except Exception as e:
                logging.error('POST response clarin/import/handle failed. Status: ' + str(response.status_code))

        logging.info("Handles without object were successfully imported!")
