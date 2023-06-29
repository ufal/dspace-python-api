import json

from support.dspace_proxy import rest_proxy
from migration_const import DATA_PATH
from const import API_URL


def read_json(file_name, path=DATA_PATH):
    """
    Read data from file as json.
    @param file_name: file name
    @return: data as json
    """
    with open(path + file_name) as f:
        json_p = json.load(f)
    return json_p


def convert_response_to_json(response):
    """
    Convert response to json.
    @param response: response from api call
    @return: json created from response
    """
    return json.loads(response.content.decode('utf-8'))


def do_api_post(url, param, json_p):
    """
    Insert data into database by api if they are not None.
    @param url: url for api post
    @param param: parameters for api post
    @param json_p: posted data
    @return: response from api post
    """
    url = API_URL + url
    response = rest_proxy.d.api_post(url, param, json_p)
    return response


def do_api_get_one(url, id):
    """
    Get data with id from table.
    @param url: url for api get
    @param id: id of object
    @return: response from api get
    """
    url = API_URL + url + '/' + str(id)
    response = rest_proxy.d.api_get(url, None, None)
    return response


def do_api_get_all(url):
    """
    Get all data from table.
    @param url: url for api get
    @return: response from api get
    """
    url = API_URL + url
    param = {'size': 1000}
    response = rest_proxy.d.api_get(url, param, None)
    return response
