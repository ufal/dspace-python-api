"""
Defines how dspace api (rest_proxy) behaves on responses.
Add specific reactions to response_map.
"""
from json import JSONDecodeError

from support.logs import log, Severity


def check_response(r, additional_message):
    if r is None:
        log("Failed to receive response. " + additional_message, Severity.ERROR)
        raise Exception("No response from server where one was expected")
    log(str(additional_message) + " Response " + str(r.status_code))
    if r.status_code not in response_map:
        log("Unexpected response while creating item: " + str(r.status_code) + "; " + r.url + "; " + r.text,
            Severity.WARN)
    else:
        response_map[r.status_code](r)


response_map = {
    201: lambda r: response_success(r),
    200: lambda r: response_success(r),
    500: lambda r: error(r),
    400: lambda r: error(r)
}


def error(r):
    raise ConnectionError(r.text)


def response_success(r):
    try:
        r = r.json()
        log(f'{r["type"]} {r["uuid"]} created successfully!')
    except JSONDecodeError:
        log("request successfully")
