import logging
import json

from utils import read_json, do_api_post


def import_registrationdata(statistics):
    """
    Import data into database.
    Mapped tables: registrationdata
    """
    json_name = 'registrationdata.json'
    url = 'eperson/registrations'
    imported = 0
    json_a = read_json(json_name)
    if not json_a:
        logging.info("Registrationdata JSON is empty.")
        return
    for i in json_a:
        json_p = {'email': i['email']}
        try:
            do_api_post(url, None, json_p)
            imported += 1
        except Exception as e:
            json_e = json.loads(e.args[0])
            logging.error('POST request' + json_e['path'] + ' for email: ' + i['email'] + ' failed. Status: ' +
                          str(json_e['status']))
    statistics['registrationdata'] = (len(json_a), imported)
    logging.info("Registration data was successfully imported!")
