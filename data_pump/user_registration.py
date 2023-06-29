import logging

from utils import read_json, convert_response_to_json, do_api_post


def import_user_registration(email2epersonId, eperson_id, userRegistration_id, statistics):
    """
    Import data into database.
    Mapped tables: user_registration
    """
    json_name = "user_registration.json"
    url = 'clarin/import/userregistration'
    imported = 0
    # read user_registration
    json_a = read_json(json_name)
    if not json_a:
        logging.info("User_registration JSON is empty.")
        return
    for i in json_a:
        json_p = {'email': i['email'], 'organization': i['organization'],
                  'confirmation': i['confirmation']}
        if i['email'] in email2epersonId:
            json_p['ePersonID'] = eperson_id[email2epersonId[i['email']]]
        else:
            json_p['ePersonID'] = None
        try:
            response = do_api_post(url, None, json_p)
            userRegistration_id[i['eperson_id']] = convert_response_to_json(response)[
                'id']
            imported += 1
        except Exception:
            logging.error('POST request clarin/import/userregistration for id: ' + str(i['eperson_id']) +
                          ' failed. Status: ' + str(response.status_code))
    statistics['user_registration'] = (len(json_a), imported)
    logging.info("User registration was successfully imported!")
