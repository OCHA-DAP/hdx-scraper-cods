#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from cods import COD
from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.facades.simple import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-cods"


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    with ErrorsOnExit() as errors:
        with Download() as downloader:
            cod = COD(downloader, errors)
            datasets_metadata = cod.get_datasets_metadata(configuration["url"])
            logger.info(f"Number of datasets to upload: {len(datasets_metadata)}")
            for metadata in datasets_metadata:
                dataset, batch = cod.generate_dataset(metadata)
                if dataset:
                    dataset.update_from_yaml()
                    try:
                        dataset.create_in_hdx(
                            hxl_update=False,
                            updated_by_script="HDX Scraper: CODS",
                            batch=batch,
                            ignore_fields=["num_of_rows", "resource:description"],
                        )
                    except HDXError as ex:
                        logger.exception(f"Dataset: {metadata['DatasetTitle']} could not be uploaded")
                        errors.add(f"Dataset: {metadata['DatasetTitle']}, error: {ex}")


if __name__ == "__main__":
    facade(
        main,
        hdx_site="feature",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
