import logging
import os

from ._utils import time_method

from ._handle import handles
from ._metadata import metadatas

from ._group import groups
from ._community import communities
from ._collection import collections
from ._registrationdata import registrationdatas
from ._eperson import epersons
from ._eperson import groups as eperson_groups
from ._userregistration import userregistrations
from ._bitstreamformatregistry import bitstreamformatregistry
from ._license import licenses
from ._item import items
from ._tasklistitem import tasklistitems
from ._bundle import bundles
from ._bitstream import bitstreams
from ._resourcepolicy import resourcepolicies
from ._usermetadata import usermetadatas
from ._db import db
from ._sequences import sequences

_logger = logging.getLogger("pump.repo")


class repo:
    @time_method
    def __init__(self, env: dict, dspace):
        def _f(name): return os.path.join(env["input"]["datadir"], name)

        # load groups
        self.groups = groups(
            _f("epersongroup.json"),
            _f("group2group.json"),
        )
        self.groups.from_rest(dspace)

        # load handles
        self.handles = handles(_f("handle.json"))

        # load metadata
        self.metadatas = metadatas(
            env,
            dspace,
            _f("metadatavalue.json"),
            _f("metadatafieldregistry.json"),
            _f("metadataschemaregistry.json"),
        )

        # load community
        self.communities = communities(
            _f("community.json"),
            _f("community2community.json"),
        )

        self.collections = collections(
            _f("collection.json"),
            _f("community2collection.json"),
            _f("metadatavalue.json"),
        )

        self.registrationdatas = registrationdatas(
            _f("registrationdata.json")
        )

        self.epersons = epersons(
            _f("eperson.json")
        )

        self.egroups = eperson_groups(
            _f("epersongroup2eperson.json")
        )

        self.userregistrations = userregistrations(
            _f("user_registration.json")
        )

        self.bitstreamformatregistry = bitstreamformatregistry(
            _f("bitstreamformatregistry.json")
        )

        self.licenses = licenses(
            _f("license_label.json"),
            _f("license_definition.json"),
            _f("license_label_extended_mapping.json"),
        )

        self.items = items(
            _f("item.json"),
            _f("workspaceitem.json"),
            _f("workflowitem.json"),
            _f("collection2item.json"),
        )

        self.tasklistitems = tasklistitems(
            _f("tasklistitem.json")
        )

        self.bundles = bundles(
            _f("bundle.json"),
            _f("item2bundle.json"),
        )

        self.bitstreams = bitstreams(
            _f("bitstream.json"),
            _f("bundle2bitstream.json"),
        )

        self.usermetadatas = usermetadatas(
            _f("user_metadata.json"),
            _f("license_resource_user_allowance.json"),
            _f("license_resource_mapping.json")
        )

        self.resourcepolicies = resourcepolicies(
            _f("resourcepolicy.json")
        )

        self.raw_db_7 = db(env["db_dspace_7"])
        self.raw_db_dspace_5 = db(env["db_dspace_5"])
        self.raw_db_utilities_5 = db(env["db_utilities_5"])

        self.sequences = sequences()

    def _fetch_all_vals(self, db5, table_name: str, sql: str = None):
        sql = f"SELECT * FROM {table_name}"
        cols5 = []
        db5 = db5 or self.raw_db_dspace_5
        vals5 = db5.fetch_all(sql, col_names=cols5)
        cols7 = []
        vals7 = self.raw_db_7.fetch_all(sql, col_names=cols7)
        return cols5, vals5, cols7, vals7

    def _filter_vals(self, vals, col_names, only_names):
        idxs = [col_names.index(x) for x in only_names]
        filtered = []
        for row in vals:
            filtered.append([row[idx] for idx in idxs])
        return filtered

    def _cmp_values(self, table_name: str, vals5, only_in_5, vals7, only_in_7, do_not_show: bool):
        too_many_5 = ""
        too_many_7 = ""
        LIMIT = 5
        if len(only_in_5) > LIMIT:
            too_many_5 = f"!!! TOO MANY [{len(only_in_5)}] "
        if len(only_in_7) > LIMIT:
            too_many_7 = f"!!! TOO MANY [{len(only_in_7)}] "

        do_not_show = do_not_show or "CI" in os.environ or "GITHUB_ACTION" in os.environ
        # assume we do not have emails that we do not want to show in db7
        if do_not_show:
            only_in_5 = [x if "@" not in x else "....." for x in only_in_5]
            only_in_7 = [x if "@" not in x else "....." for x in only_in_7]

        _logger.info(f"Table [{table_name}]: v5:[{len(vals5)}], v7:[{len(vals7)}]\n"
                     f"  {too_many_5}only in v5:[{only_in_5[:LIMIT]}]\n"
                     f"  {too_many_7}only in v7:[{only_in_7[:LIMIT]}]")

    def diff_table_cmp_cols(self, db5, table_name: str, compare_arr: list, gdpr: bool = True):
        cols5, vals5, cols7, vals7 = self._fetch_all_vals(db5, table_name)
        do_not_show = gdpr and "email" in compare_arr

        filtered5 = self._filter_vals(vals5, cols5, compare_arr)
        vals5_cmp = ["|".join(str(x) for x in x) for x in filtered5]
        filtered7 = self._filter_vals(vals7, cols7, compare_arr)
        vals7_cmp = ["|".join(str(x) for x in x) for x in filtered7]

        only_in_5 = list(set(vals5_cmp).difference(vals7_cmp))
        only_in_7 = list(set(vals7_cmp).difference(vals5_cmp))
        if len(only_in_5) + len(only_in_7) == 0:
            _logger.info(f"Table [{table_name: >20}] is THE SAME in v5 and v7!")
            return
        self._cmp_values(table_name, vals5, only_in_5, vals7, only_in_7, do_not_show)

    def diff_table_cmp_len(self, db5, table_name: str, nonnull: list = None, gdpr: bool = True):
        nonnull = nonnull or []
        cols5, vals5, cols7, vals7 = self._fetch_all_vals(db5, table_name)
        do_not_show = gdpr and "email" in nonnull

        msg = " OK " if len(vals5) == len(vals7) else " !!! WARN !!! "
        _logger.info(
            f"Table [{table_name: >20}] {msg} compared by len only v5:[{len(vals5)}], v7:[{len(vals7)}]")

        for col_name in nonnull:
            vals5_cmp = [x for x in self._filter_vals(
                vals5, cols5, [col_name]) if x[0] is not None]
            vals7_cmp = [x for x in self._filter_vals(
                vals7, cols7, [col_name]) if x[0] is not None]

            msg = " OK " if len(vals5_cmp) == len(vals7_cmp) else " !!! WARN !!! "
            _logger.info(
                f"Table [{table_name: >20}] {msg}  NON NULL [{col_name:>15}] v5:[{len(vals5_cmp):3}], v7:[{len(vals7_cmp):3}]")

    def diff_table_sql(self, db5, table_name: str, sql5, sql7, compare, process_ftor):
        cols5 = []
        vals5 = db5.fetch_all(sql5, col_names=cols5)
        cols7 = []
        vals7 = self.raw_db_7.fetch_all(sql7, col_names=cols7)
        # special case where we have different names of columns but only one column to compare
        if compare == 0:
            vals5_cmp = [x[0] for x in vals5 if x[0] is not None]
            vals7_cmp = [x[0] for x in vals7 if x[0] is not None]
        elif compare is None:
            vals5_cmp = vals5
            vals7_cmp = vals7
        else:
            vals5_cmp = [x[0] for x in self._filter_vals(
                vals5, cols5, [compare]) if x[0] is not None]
            vals7_cmp = [x[0] for x in self._filter_vals(
                vals7, cols7, [compare]) if x[0] is not None]

        if process_ftor is not None:
            vals5_cmp, vals7_cmp = process_ftor(self, vals5_cmp, vals7_cmp)

        only_in_5 = list(set(vals5_cmp).difference(vals7_cmp))
        only_in_7 = list(set(vals7_cmp).difference(vals5_cmp))
        self._cmp_values(table_name, vals5, only_in_5, vals7, only_in_7, False)

    def diff(self, to_validate=None):
        if to_validate is None:
            to_validate = [
                getattr(getattr(self, x), "validate_table")
                for x in dir(self) if hasattr(getattr(self, x), "validate_table")
            ]
        else:
            if not hasattr(to_validate, "validate_table"):
                _logger.warning(f"Missing validate_table in {to_validate}")
                return
            to_validate = [to_validate.validate_table]

        for valid_defs in to_validate:
            for table_name, defin in valid_defs:
                _logger.info("=" * 10 + f" Validating {table_name} " + "=" * 10)
                db5_name = defin.get("db", "clarin-dspace")
                db5 = self.raw_db_dspace_5 if db5_name == "clarin-dspace" else self.raw_db_utilities_5

                cmp = defin.get("compare", None)
                if cmp is not None:
                    self.diff_table_cmp_cols(db5, table_name, cmp)

                cmp = defin.get("nonnull", None)
                if cmp is not None:
                    self.diff_table_cmp_len(db5, table_name, cmp)

                # compare only len
                if len(defin) == 0:
                    self.diff_table_cmp_len(db5, table_name)

                cmp = defin.get("sql", None)
                if cmp is not None:
                    self.diff_table_sql(
                        db5, table_name, cmp["5"], cmp["7"], cmp["compare"], cmp.get("process", None))

    # =====
    def uuid(self, res_type_id: int, res_id: int):
        # find object id based on its type
        try:
            if res_type_id == self.communities.TYPE:
                return self.communities.uuid(res_id)
            if res_type_id == self.collections.TYPE:
                return self.collections.uuid(res_id)
            if res_type_id == self.items.TYPE:
                return self.items.uuid(res_id)
            if res_type_id == self.bitstreams.TYPE:
                return self.bitstreams.uuid(res_id)
            if res_type_id == self.bundles.TYPE:
                return self.bundles.uuid(res_id)
            if res_type_id == self.epersons.TYPE:
                return self.epersons.uuid(res_id)
            if res_type_id == self.groups.TYPE:
                arr = self.groups.uuid(res_id)
                if len(arr or []) > 0:
                    return arr[0]
        except Exception as e:
            return None
        return None
