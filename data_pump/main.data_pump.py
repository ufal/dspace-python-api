import logging

import var_declarations as var
from data_pump.bitstream import import_bitstream
from data_pump.bitstreamformatregistry import import_bitstreamformatregistry
from data_pump.bundle import import_bundle
from data_pump.collection import import_collection
from data_pump.community import import_community
from data_pump.user_metadata import import_user_metadata
from data_pump.eperson import import_eperson, import_group2eperson
from data_pump.epersongroup import import_epersongroup, import_group2group
from data_pump.handle import Handle
from data_pump.item import import_item
from data_pump.license import import_license_label, import_license_definition
from data_pump.metadata import Metadata
from data_pump.metadatafieldregistry import import_metadatafieldregistry
from data_pump.metadataschemaregistry import import_metadataschemaregistry
from data_pump.registrationdata import import_registrationdata
from data_pump.tasklistitem import import_tasklistitem
from data_pump.user_registration import import_user_registration
from utils import read_json


def at_the_end_of_import(imported_handle, statistics):
    json_a = read_json("handle.json")
    statistics['handle'] = (len(json_a), imported_handle)
    # write statistic into log
    logging.info("Statistics:")
    for key, value in statistics.items():
        logging.info(key + ": " + str(value[0]) +
                     " expected and imported " + str(value[1]))


if __name__ == "__main__":
    handle_obj = Handle(var.handle)
    metadata = Metadata(var.metadatavalue)

    logging.info("Data migration started!")
    handle_obj.import_handle_without_object(var.handle)
    handle_obj.import_handle_with_url(var.handle, var.imported_handle)
    import_metadataschemaregistry(var.metadata_schema_id, var.statistics)
    import_metadatafieldregistry(var.metadata_schema_id,
                                 var.metadata_field_id, var.statistics)
    import_community(metadata, var.group_id, var.handle, var.community_id, var.community2logo,
                     var.imported_handle, var.metadatavalue, var.metadata_field_id, var.statistics)
    import_collection(metadata, var.group_id, var.handle, var.community_id, var.collection_id,
                      var.collection2logo, var.imported_handle, var.metadatavalue, var.metadata_field_id,
                      var.statistics)
    import_registrationdata(var.statistics)
    import_epersongroup(metadata, var.group_id, var.metadatavalue,
                        var.metadata_field_id, var.statistics)
    import_group2group(var.group_id, var.statistics)
    import_eperson(metadata, var.eperson_id, var.email2epersonId, var.metadatavalue, var.metadata_field_id,
                   var.statistics)
    import_user_registration(var.email2epersonId, var.eperson_id,
                             var.userRegistration_id, var.statistics)
    import_group2eperson(var.eperson_id, var.group_id, var.statistics)
    import_license_label(var.labels, var.statistics)
    import_license_definition(var.labels, var.eperson_id, var.statistics)
    import_item(metadata, var.workflowitem_id, var.workspaceitem_id, var.item_id, var.collection_id,
                var.eperson_id, var.imported_handle, var.handle, var.metadatavalue, var.metadata_field_id,
                var.statistics)
    import_tasklistitem(var.workflowitem_id, var.eperson_id, var.statistics)
    import_bitstreamformatregistry(
        var.bitstreamformat_id, var.unknown_format_id, var.statistics)
    import_bundle(metadata, var.item_id, var.bundle_id, var.primaryBitstream, var.metadatavalue, var.metadata_field_id,
                  var.statistics)
    import_bitstream(metadata, var.bitstreamformat_id, var.primaryBitstream, var.bitstream2bundle, var.bundle_id,
                     var.community2logo, var.collection2logo, var.bitstream_id, var.community_id, var.collection_id,
                     var.unknown_format_id, var.metadatavalue, var.metadata_field_id, var.statistics)
    import_user_metadata(var.bitstream_id, var.userRegistration_id, var.statistics)
    at_the_end_of_import(var.imported_handle, var.statistics)
    logging.info("Data migration is completed!")
