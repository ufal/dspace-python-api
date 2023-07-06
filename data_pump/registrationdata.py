import logging

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
            response = do_api_post(registrationdata_url, {}, registrationdata_json_p)
            if response.ok:
                imported_registrationdata += 1
        except Exception as e:
            logging.error('POST request' + registrationdata_url + ' for email: ' +
                          registrationdata['email'] + ' failed. Exception: ' + str(e))

    statistics_val = (len(registrationdata_json_a), imported_registrationdata)
    statistics_dict['registrationdata'] = statistics_val
    logging.info("Registration data was successfully imported!")
