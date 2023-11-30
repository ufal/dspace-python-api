import logging
from ._utils import read_json, time_method, serialize, deserialize, progress_bar, log_before_import, log_after_import

_logger = logging.getLogger("pump.usermetadata")


class usermetadatas:
    """
        SQL:
        Mapped tables: user_metadata, license_resource_user_allowance
    """

    def __init__(self, usermetadata_file_str: str, userallowance_file_str: str, resourcemapping_file_str: str):
        self._umeta = read_json(usermetadata_file_str)
        self._uallowance = read_json(userallowance_file_str)
        self._rmap = read_json(resourcemapping_file_str)
        self._id2uuid = {}
        self._imported = {
            "um": 0,
        }

        if len(self._umeta) == 0:
            _logger.info(f"Empty input: [{usermetadata_file_str}].")

        if len(self._uallowance) == 0:
            _logger.info(f"Empty input: [{userallowance_file_str}].")

        if len(self._rmap) == 0:
            _logger.info(f"Empty input: [{resourcemapping_file_str}].")

        # mapping transaction_id to mapping_id
        self._uallowance_transid2d = {ua['transaction_id']: ua for ua in self._uallowance}
        # mapping bitstream_id to mapping_id
        self._rmap_id2bsid = {m["mapping_id"]: m["bitstream_id"] for m in self._rmap}

        # Group user metadata by `transaction_id`. The endpoint must receive list of all metadata with the same
        # transaction_id` because if the endpoint will be called for every `user_metadata` there will be a huge amount
        # of `license_resource_user_allowance` records with not correct mapping with the `user_metadata` table.
        self._umeta_transid2ums = {}
        for um in self._umeta:
            t_id = um['transaction_id']
            if t_id not in self._uallowance_transid2d:
                continue
            self._umeta_transid2ums.setdefault(t_id, []).append(um)

    def __len__(self):
        return len(self._umeta)

    def uuid(self, b_id: int):
        assert isinstance(list(self._id2uuid.keys() or [""])[0], str)
        return self._id2uuid[str(b_id)]

    @property
    def imported(self):
        return self._imported['um']

    @time_method
    def import_to(self, dspace, bitstreams, userregistrations):
        expected = len(self._umeta_transid2ums)
        log_key = "usermetadata"
        log_before_import(log_key, expected)

        # Go through dict and import user_metadata
        for t_id, um_arr in progress_bar(self._umeta_transid2ums.items()):
            um0 = um_arr[0]
            # Get user_registration data for importing
            ua_d = self._uallowance_transid2d[um0['transaction_id']]
            # Get `eperson_id` for importing
            eperson_id = um0['eperson_id']
            map_id = ua_d['mapping_id']

            # Prepare user_metadata list for request
            data = [{'metadataKey': um['metadata_key'],
                     'metadataValue': um['metadata_value']
                     } for um in um_arr]

            try:
                bs_id = self._rmap_id2bsid[map_id]
                bs_uuid = bitstreams.uuid(bs_id)
                if bs_uuid is None:
                    _logger.info(
                        f"Cannot import user metadata for mapping_id->bsid: [{map_id}]->[{bs_id}] because the bitstream has probably already been deleted.")
                    continue
                userreg_id = userregistrations.uuid(eperson_id)

                # Prepare params for the import endpoint
                params = {
                    'bitstreamUUID': bs_uuid,
                    'createdOn': ua_d['created_on'],
                    'token': ua_d['token'],
                    'userRegistrationId': userreg_id
                }
                resp = dspace.put_usermetadata(params, data)
                self._imported['um'] += 1
            except Exception as e:
                _logger.error(f'put_usermetadata: [{t_id}] failed [{str(e)}]')

        log_after_import(log_key, expected, self.imported)

    # =============

    def serialize(self, file_str: str):
        data = {
            "umeta": self._umeta,
            "uallowance": self._uallowance,
            "rmap": self._rmap,
            "id2uuid": self._id2uuid,
            "imported": self._imported,
        }
        serialize(file_str, data)

    def deserialize(self, file_str: str):
        data = deserialize(file_str)
        self._umeta = data["umeta"]
        self._uallowance = data["uallowance"]
        self._rmap = data["rmap"]
        self._id2uuid = data["id2uuid"]
        self._imported = data["imported"]
