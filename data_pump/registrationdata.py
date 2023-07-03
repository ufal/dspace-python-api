import logging
import json

from utils import read_json, do_api_post


def import_registrationdata(statistics_dict):
    """
    Import data into database.
    Mapped tables: registrationdata
    """
    registrationdata_json_name = 'registrationdata.json'
    registrationdata_url = 'eperson/registrations'
    imported_registrationdata = 0
    registrationdata_json_a = read_json(registrationdata_json_name)
    if not registrationdata_json_a:
        logging.info("Registrationdata JSON is empty.")
        return
    for registrationdata in registrationdata_json_a:
        registrationdata_json_p = {'email': registrationdata['email']}
        try:
            do_api_post(registrationdata_url, None, registrationdata_json_p)
            imported_registrationdata += 1
        except Exception as e:
            json_e = json.loads(e.args[0])
            logging.error('POST request' + json_e['path'] + ' for email: ' +
                          registrationdata['email'] + ' failed. Status: ' +
                          str(json_e['status']))

    statistics_val = (len(registrationdata_json_a), imported_registrationdata)
    statistics_dict['registrationdata'] = statistics_val
    logging.info("Registration data was successfully imported!")
