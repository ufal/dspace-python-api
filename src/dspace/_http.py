import json
import requests

g_cnt = 0


def response_to_json(response: requests.models.Response):
    """
    Convert response to json.
    @param response: response from api call
    @return: json created from response
    """
    global g_cnt
    g_cnt += 1
    return json.loads(response.content.decode('utf-8'))
