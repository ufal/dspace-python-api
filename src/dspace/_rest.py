import logging
# from json import JSONDecodeError
from ._http import response_to_json
from .impl import client

_logger = logging.getLogger("dspace.rest")
ANONYM_EMAIL = True


def ascii(s, default="unknown"):
    try:
        return str(s).encode("ascii", "ignore").decode("ascii")
    except Exception as e:
        pass
    return default


def progress_bar(arr):
    if len(arr) < 2:
        return iter(arr)
    try:
        from tqdm import tqdm
    except Exception as e:
        return iter(arr)

    mininterval = 5 if len(arr) < 500 else 10
    return tqdm(arr, mininterval=mininterval, maxinterval=2 * mininterval)


class rest:
    """
        Serves as proxy to Dspace REST API.
        Mostly uses attribute d which represents (slightly modified) dspace_client from
        original python rest api by dspace developers
    """

    def __init__(self, endpoint: str, user: str, password: str, auth: bool = True):
        _logger.info(f"Initialise connection to DSpace REST backend [{endpoint}]")

        self._acceptable_resp = []
        self._get_cnt = 0
        self._post_cnt = 0

        client.check_response = lambda x, y: self._resp_check(x, y)
        self._response_map = {
            201: lambda r: self._resp_ok(r),
            200: lambda r: self._resp_ok(r),
            500: lambda r: self._resp_error(r),
            400: lambda r: self._resp_error(r)
        }

        self.client = client.DSpaceClient(
            api_endpoint=endpoint, username=user, password=password)
        if auth:
            if not self.client.authenticate():
                _logger.error(f'Error auth to dspace REST API at [{endpoint}]!')
                raise ConnectionError("Cannot connect to dspace!")
            _logger.debug(f"Successfully logged in to [{endpoint}]")
        _logger.info(f"DSpace REST backend is available at [{endpoint}]")
        self.endpoint = endpoint.rstrip("/")

    # =======

    @property
    def get_cnt(self):
        return self._get_cnt

    @property
    def post_cnt(self):
        return self._post_cnt

    # =======

    def push_acceptable(self, arr: list):
        self._acceptable_resp.append(arr)

    def pop_acceptable(self):
        self._acceptable_resp.pop()

    # =======

    def clarin_put_handles(self, handle_arr: list):
        """
            Import handles which have not objects into database.
            Other handles are imported by dspace objects.
            Mapped table: handles
        """
        url = 'clarin/import/handle'
        arr = [{'handle': h['handle'], 'resourceTypeID': h['resource_type_id']}
               for h in handle_arr]
        return self._put(url, arr)

    def put_handles(self, handle_arr: list):
        url = 'core/handles'
        arr = [{'handle': h['handle'], 'url': h['url']} for h in handle_arr]
        return self._put(url, arr)

    # =======

    def fetch_existing_epersongroups(self):
        """
            Get all existing eperson groups from database.
        """
        url = 'eperson/groups'
        resp = self._fetch(url, self.get_many, '_embedded')
        return resp["groups"]

    def fetch_metadata_schemas(self):
        """
            Gel all existing data from table metadataschemaregistry.
        """
        url = 'core/metadataschemas'
        arr = self._fetch(url, self.get_many, None)
        if arr is None or "_embedded" not in arr:
            return None
        return arr["_embedded"]['metadataschemas']

    def fetch_metadata_fields(self):
        """
        """
        url = 'core/metadatafields'
        arr = self._fetch(url, self.get_many, None)
        if arr is None or "_embedded" not in arr:
            return None
        return arr["_embedded"]['metadatafields']

    def fetch_metadata_field(self, object_id):
        """
        """
        url = 'core/metadatafields'
        return self._fetch(url, self.get_one, None, object_id=object_id)

    def fetch_schema(self, object_id):
        """
            Gel all existing data from table metadataschemaregistry.
        """
        url = 'core/metadataschemas'
        return self._fetch(url, self.get_one, None, object_id=object_id)

    def put_metadata_schema(self, data):
        url = 'core/metadataschemas'
        return list(self._iput(url, [data]))[0]

    def put_metadata_field(self, data: list, params: list):
        url = 'core/metadatafields'
        return list(self._iput(url, [data], [params]))[0]

    # =======

    def put_community(self, param: dict, data: dict):
        url = 'core/communities'
        _logger.debug(f"Importing [{data}] using [{url}]")
        arr = list(self._iput(url, [data], [param]))
        if len(arr) == 0:
            return None
        return arr[0]

    def put_community_admin_group(self, com_id: int):
        url = f'core/communities/{com_id}/adminGroup'
        _logger.debug(f"Adding admin group to [{com_id}] using [{url}]")
        return list(self._iput(url, [{}], [{}]))[0]

    # =======

    def put_collection(self, param: dict, data: dict):
        url = 'core/collections'
        _logger.debug(f"Importing [{data}] using [{url}]")
        arr = list(self._iput(url, [data], [param]))
        if len(arr) == 0:
            return None
        return arr[0]

    def put_collection_editor_group(self, col_id: int):
        url = f'core/collections/{col_id}/workflowGroups/editor'
        _logger.debug(f"Adding editor group to [{col_id}] using [{url}]")
        return list(self._iput(url, [{}], [{}]))[0]

    def put_collection_submitter(self, col_id: int):
        url = f'core/collections/{col_id}/submittersGroup'
        _logger.debug(f"Adding editor group to [{col_id}] using [{url}]")
        return list(self._iput(url, [{}], [{}]))[0]

    def put_collection_bitstream_read_group(self, col_id: int):
        url = f'core/collections/{col_id}/bitstreamReadGroup'
        _logger.debug(f"Adding bitstream read group to [{col_id}] using [{url}]")
        return list(self._iput(url, [{}], [{}]))[0]

    def put_collection_item_read_group(self, col_id: int):
        url = f'core/collections/{col_id}/itemReadGroup'
        _logger.debug(f"Adding item read group to [{col_id}] using [{url}]")
        return list(self._iput(url, [{}], [{}]))[0]

    # =======

    def put_registrationdata(self, param: dict, data: dict):
        url = 'eperson/registrations'
        _logger.debug(f"Importing [{data}] using [{url}]")
        return list(self._iput(url, [data], [param]))[0]

    # =======

    def put_eperson_group(self, param: dict, data: dict):
        url = 'eperson/groups'
        _logger.debug(f"Importing [{data}] using [{url}]")
        return list(self._iput(url, [data], [param]))[0]

    def put_group2group(self, parent, child):
        url = f'clarin/eperson/groups/{parent}/subgroups'
        child_url = f'{self.endpoint}/eperson/groups/{child}'
        _logger.debug(f"Importing [{parent}][{child}] using [{url}]")
        return list(self._iput(url, [child_url]))[0]

    def put_eperson(self, param: dict, data: dict):
        url = 'clarin/import/eperson'
        _logger.debug(f"Importing [{data}] using [{url}]")
        return list(self._iput(url, [data], [param]))[0]

    def put_userregistration(self, data: dict):
        url = 'clarin/import/userregistration'
        _logger.debug(f"Importing [{data}] using [{url}]")
        return list(self._iput(url, [data]))[0]

    def put_egroup(self, gid: int, eid: int):
        url = f'clarin/eperson/groups/{gid}/epersons'
        _logger.debug(f"Importing group[{gid}] e:[{eid}] using [{url}]")
        eperson_url = f'{self.endpoint}/eperson/groups/{eid}'
        return list(self._iput(url, [eperson_url]))[0]

    # =======

    def fetch_bitstreamregistry(self):
        url = 'core/bitstreamformats'
        arr = self._fetch(url, self.get_many, None)
        if arr is None or "_embedded" not in arr:
            return None
        return arr["_embedded"]["bitstreamformats"]

    def put_bitstreamregistry(self, data: dict):
        url = 'core/bitstreamformats'
        _logger.debug(f"Importing [{data}] using [{url}]")
        return list(self._iput(url, [data]))[0]

    # =======

    def put_license_label(self, data: dict):
        url = 'core/clarinlicenselabels'
        _logger.debug(f"Importing [{data}] using [{url}]")
        return list(self._iput(url, [data]))[0]

    def put_license(self, param: dict, data: dict):
        url = 'clarin/import/license'
        _logger.debug(f"Importing [{data}] using [{url}]")
        return list(self._iput(url, [data], [param]))[0]

    # =======

    def put_tasklistitem(self, param: dict):
        url = 'clarin/eperson/groups/tasklistitem'
        _logger.debug(f"Importing [][{param}] using [{url}]")
        return list(self._iput(url, None, [param]))[0]

    # =======

    def put_bundle(self, item_uuid: int, data: dict):
        url = f'core/items/{item_uuid}/bundles'
        _logger.debug(f"Importing [{data}] using [{url}]")
        return list(self._iput(url, [data],))[0]

    # =======

    def fetch_raw_item(self, uuid: str):
        url = f'core/items/{uuid}'
        _logger.debug(f"Fetching [{uuid}] using [{url}]")
        r = self.get(url)
        if not r.ok:
            raise Exception(r)
        return response_to_json(r)

    # =======

    def put_usermetadata(self, params: dict, data: dict):
        url = 'clarin/import/usermetadata'
        _logger.debug(f"Importing [{data}] using [{url}]")
        return list(self._iput(url, [data], [params]))[0]

    # =======

    def put_resourcepolicy(self, params: dict, data: dict):
        url = 'authz/resourcepolicies'
        _logger.debug(f"Importing [{data}] using [{url}]")
        return list(self._iput(url, [data], [params]))[0]

    # =======

    def add_checksums(self):
        """
            Fill the tables most_recent_checksum and checksum_result based
            on imported bitstreams that haven't already their checksum
            calculated.
        """
        url = 'clarin/import/core/bitstream/checksum'
        _logger.debug(f"Checksums using [{url}]")
        r = self.post(url)
        if not r.ok:
            raise Exception(r)

    def put_bitstream(self, param: dict, data: dict):
        url = 'clarin/import/core/bitstream'
        _logger.debug(f"Importing [][{param}] using [{url}]")
        return list(self._iput(url, [data], [param]))[0]

    def put_com_logo(self, param: dict):
        url = 'clarin/import/logo/community'
        _logger.debug(f"Importing [][{param}] using [{url}]")
        r = self.post(url, params=param, data=None)
        if not r.ok:
            raise Exception(r)
        return response_to_json(r)

    def put_col_logo(self, param: dict):
        url = 'clarin/import/logo/collection'
        _logger.debug(f"Importing [][{param}] using [{url}]")
        r = self.post(url, params=param, data=None)
        if not r.ok:
            raise Exception(r)
        return response_to_json(r)

    # =======

    def fetch_item(self, uuid: str):
        url = f'clarin/import/{uuid}/item'
        _logger.debug(f"Importing [] using [{url}]")
        return self._fetch(url, self.get, None)

    def put_ws_item(self, param: dict, data: dict):
        url = 'clarin/import/workspaceitem'
        _logger.debug(f"Importing [{data}] using [{url}]")
        return list(self._iput(url, [data], [param]))[0]

    def put_wf_item(self, param: dict):
        url = 'clarin/import/workflowitem'
        _logger.debug(f"Importing [][{param}] using [{url}]")
        r = self.post(url, params=param, data=None)
        if not r.ok:
            raise Exception(r)
        return r

    def put_item(self, param: dict, data: dict):
        url = 'clarin/import/item'
        _logger.debug(f"Importing [][{param}] using [{url}]")
        return list(self._iput(url, [data], [param]))[0]

    def put_item_to_col(self, item_uuid: str, data: list):
        url = f'clarin/import/item/{item_uuid}/mappedCollections'
        _logger.debug(f"Importing [{data}] using [{url}]")
        col_url = 'core/collections/'
        # Prepare request body which should looks like this:
        # `"https://localhost:8080/spring-rest/api/core/collections/{collection_uuid_1}" + \n
        # "https://localhost:8080/spring-rest/api/core/collections/{collection_uuid_2}"
        data = [f"{self.endpoint}/{col_url}/{x}" for x in data]
        return list(self._iput(url, [data]))[0]

    # =======

    def fetch_search_items(self, item_type: str = "ITEM", page: int = 0, size: int = 100):
        """
            TODO(jm): make generic
        """
        url = f'discover/search/objects?sort=score,DESC&size={size}&page={page}&configuration=default&dsoType={item_type}&embed=thumbnail&embed=item%2Fthumbnail'
        r = self.get(url)
        if not r.ok:
            raise Exception(r)
        return response_to_json(r)

    # =======

    def _fetch(self, url: str, method, key: str, **kwargs):
        try:
            r = method(url, **kwargs)
            js = response_to_json(r)
            if key is None:
                return js
            return js[key]
        except Exception as e:
            _logger.error(f'GET [{url}] failed. Exception: [{str(e)}]')
        return None

    def _put(self, url: str, arr: list, params: list = None):
        return len(list(self._iput(url, arr, params)))

    def _iput(self, url: str, arr: list, params=None):
        _logger.debug(f"Importing {len(arr)} using [{url}]")
        if params is not None:
            assert len(params) == len(arr)

        for i, data in enumerate(progress_bar(arr)):
            try:
                param = params[i] if params is not None else None
                r = self.post(url, params=param, data=data)
                if not r.ok:
                    raise Exception(r)
                try:
                    js = None
                    if len(r.content or '') > 0:
                        js = response_to_json(r)
                    yield js
                except Exception:
                    yield r
            except Exception as e:
                ascii_data = ascii(data)
                if ANONYM_EMAIL:
                    # poor man's anonymize
                    if "@" in ascii_data or "email" in ascii_data:
                        ascii_data = ascii_data[:5]
                if len(ascii_data) > 80:
                    ascii_data = f"{ascii_data[:70]}..."
                msg_r = ""
                try:
                    msg_r = str(r)
                except Exception:
                    pass

                msg = f'POST [{url}] for [{ascii_data}] failed. Exception: [{str(e)}][{msg_r}]'
                _logger.error(msg)
                yield None
        _logger.debug(f"Imported [{url}] successfully")

    # =======

    def get_many(self, command: str, size: int = 1000):
        params = {'size': size}
        return self.get(command, params)

    def get_one(self, command: str, object_id: int):
        url = command + '/' + str(object_id)
        return self.get(url, {})

    def get(self, command: str, params=None, data=None):
        url = self.endpoint + '/' + command
        self._get_cnt += 1
        return self.client.api_get(url, params, data)

    def post(self, command: str, params=None, data=None):
        url = self.endpoint + '/' + command
        self._post_cnt += 1
        return self.client.api_post(url, params or {}, data or {})

    # =======

    def _resp_check(self, r, msg):
        if r is None:
            _logger.error(f"Failed to receive response [{msg}] ")
            raise Exception("No response from server where one was expected")
        _logger.debug(f"{str(msg)}: {r.status_code}")

        # explicit accepted
        for ar in self._acceptable_resp:
            if r.status_code in ar:
                return

        if r.status_code not in self._response_map:
            _logger.warning(f"Unexpected response: {r.status_code}; [{r.url}]; {r.text}")
        else:
            self._response_map[r.status_code](r)

    def _resp_error(self, r):
        raise ConnectionError(r.text)

    def _resp_ok(self, r):
        return True
