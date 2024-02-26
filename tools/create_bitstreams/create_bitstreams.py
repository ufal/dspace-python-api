import logging
import os
import zipfile
import sys
import tqdm
from urllib.parse import urljoin, urlparse

_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../src"))


import dspace  # noqa
import settings  # noqa
import project_settings  # noqa
from dspace.impl.models import Item  # noqa
from utils import init_logging, update_settings  # noqa

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])

MULTIPART_CONTENT_TYPE = 'multipart/form-data'
COPIES_COUNT = 20

TEMPLATE_FILE_PATH = 'template.png'
ZIP_FILE_PATH = 'zipfile.zip'
BIG_FILE_PATH = 'bigfile.txt'

COMMUNITY_2_CREATE = {
    "type": {
        "value": "community"
    },
    "metadata": {
        "dc.title": [
            {
                "language": None,
                "value": "Test Item Community"
            }
        ],
    }
}

COLLECTION_2_CREATE = {
    "type": {
        "value": "collection"
    },
    "metadata": {
        "dc.title": [
            {
                "language": None,
                "value": "Test Item Collection"
            }
        ]
    },
}

ITEM_2_CREATE = {
    "type": {
        "value": "item"
    },
    "metadata": {
        "dc.title": [
            {
                "language": None,
                "value": "Test Item"
            }
        ]
    },
    "inArchive": True,
    "discoverable": True,
    "withdrawn": False,
}


def remove_file(path):
    try:
        os.remove(path)
    except OSError as e:
        _logger.warning(f"Error: {e.filename} - {e.strerror}.")


def get_bundle(dspace_client, item):
    """
        Fetch a bundle of existing Item or create a new one
    """
    original_bundle = None
    item_bundles = dspace_client.client.get_bundles(item)
    for bundle in item_bundles:
        if bundle.name == 'ORIGINAL':
            return bundle
    if original_bundle is None:
        original_bundle = dspace_client.client.create_bundle(item)
    if not original_bundle:
        _logger.warning('The bundle was neither found nor created.')
        return None

    return original_bundle


def create_item_with_title(dspace_client, parent, title):
    """
    Create item with specific title.
    @param dspace_client: dspace client
    @param parent: collection where the item will be created
    @param title: title of the item
    @return: created item or None if item was not created
    """
    item2create = ITEM_2_CREATE
    item2create['metadata']['dc.title'][0]['value'] = title
    return dspace_client.client.create_item(parent.uuid, Item(item2create))


if __name__ == '__main__':
    dspace_be = dspace.rest(
        env["backend"]["endpoint"],
        env["backend"]["user"],
        env["backend"]["password"],
        env["backend"]["authentication"]
    )

    def _link(uuid):
        parsed_url = urlparse(env["backend"]["endpoint"])
        return urljoin(f"{parsed_url.scheme}://{parsed_url.netloc}", f'items/{uuid}')

    # Fetch all items from the server
    all_items = dspace_be.client.get_items()
    _logger.info(f"Found {len(all_items)} items.")

    # 3 Items are updated - if they don't exist create a community and collection where a new item will be created
    if len(all_items) < 3:
        # Create community
        community = dspace_be.client.create_community(None, COMMUNITY_2_CREATE)
        if not community:
            _logger.warning('Community was not created.')

        # Create collection
        collection = dspace_be.client.create_collection(
            community.uuid, COLLECTION_2_CREATE)
        if not collection:
            _logger.warning('Collection was not created.')

        item_hundred_files = create_item_with_title(
            dspace_be, collection, 'Hundred Files')
        item_zip_files = create_item_with_title(dspace_be, collection, 'Zip File')
        item_big_file = create_item_with_title(dspace_be, collection, 'Big File')

    else:
        import random
        for item in all_items:
            _logger.info(f"Item: {_link(item.uuid)}")

        random.shuffle(all_items)
        item_hundred_files, item_zip_files, item_big_file = all_items[:3]

    _logger.info(
        f"Using items:\n{_link(item_hundred_files.uuid)}\n{_link(item_zip_files.uuid)}\n{_link(item_big_file.uuid)}")

    # Item with 100 bitstreams
    b = get_bundle(dspace_be, item_hundred_files)
    _logger.info(
        f"Adding many files to [{item_hundred_files.handle}] [{item_hundred_files.uuid}]")
    for i in tqdm.tqdm(range(COPIES_COUNT)):
        dspace_be.client.create_bitstream(
            b, TEMPLATE_FILE_PATH, TEMPLATE_FILE_PATH, MULTIPART_CONTENT_TYPE)
    _logger.info(f"Created [{item_hundred_files.handle}] with many files")

    # Item with zip bitstream
    b = get_bundle(dspace_be, item_zip_files)
    zipfile.ZipFile(ZIP_FILE_PATH, mode='w').write(TEMPLATE_FILE_PATH)
    _logger.debug(f'Creating bitstream with file: {ZIP_FILE_PATH}')
    dspace_be.client.create_bitstream(
        b, ZIP_FILE_PATH, ZIP_FILE_PATH, MULTIPART_CONTENT_TYPE)
    _logger.info(
        f"Created [{item_zip_files.handle}] [{item_zip_files.uuid}] with ZIP file")
    remove_file(ZIP_FILE_PATH)

    # Item with big bitstream
    big_size = 3 * 1024 * 1024 * 1024
    _logger.debug(f'Creating [{big_size// (1024 * 1024)} GB] file: {BIG_FILE_PATH}')
    with open(BIG_FILE_PATH, 'wb') as f:
        f.seek(big_size)
        f.write(b'\0')
    dspace_be.client.create_bitstream(
        item_big_file, BIG_FILE_PATH, BIG_FILE_PATH, MULTIPART_CONTENT_TYPE)
    _logger.info(f"Created [{item_big_file.handle}] [{item_big_file.uuid}] with BIG file")
    remove_file(BIG_FILE_PATH)
