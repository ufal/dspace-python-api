import logging

from data_pump.utils import read_json, convert_response_to_json, do_api_post, \
    save_dict_as_json


def import_user_registration(email2epersonId_dict,
                             eperson_id_dict,
                             userRegistration_id_dict,
                             statistics_dict,
                             save_dict):
    """
    Import data into database.
    Mapped tables: user_registration
    """
    user_reg_json_name = "user_registration.json"
    saved_user_reg_json_name = 'user_registration_dict.json'
    user_reg_url = 'clarin/import/userregistration'
    imported_user_reg = 0
    # read user_registration
    user_reg_json_list = read_json(user_reg_json_name)
    if not user_reg_json_list:
        logging.info("User_registration JSON is empty.")
        return
    for user_reg_json in user_reg_json_list:
        user_reg_json_p = {
            'email': user_reg_json['email'],
            'organization': user_reg_json['organization'],
            'confirmation': user_reg_json['confirmation']
        }
        if user_reg_json['email'] in email2epersonId_dict:
            user_reg_json_p['ePersonID'] = \
                eperson_id_dict[email2epersonId_dict[user_reg_json['email']]]
        else:
            user_reg_json_p['ePersonID'] = None
        try:
            response = do_api_post(user_reg_url, {}, user_reg_json_p)
            userRegistration_id_dict[user_reg_json['eperson_id']] = \
                convert_response_to_json(response)['id']
            imported_user_reg += 1
        except Exception as e:
            logging.error('POST request ' + user_reg_url + ' for id: ' +
                          str(user_reg_json['eperson_id']) +
                          ' failed. Exception: ' + str(e))

    # save user registration dict as json
    if save_dict:
        save_dict_as_json(saved_user_reg_json_name, userRegistration_id_dict)
    statistics_val = (len(user_reg_json_list), imported_user_reg)
    statistics_dict['user_registration'] = statistics_val
    logging.info("User registration was successfully imported!")
