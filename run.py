import argparse
import logging
from os.path import expanduser, join

from cods import COD

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.facades.keyword_arguments import facade
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-cods"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-co", "--countries_override", default=None, help="Countries to run"
    )
    args = parser.parse_args()
    return args


def main(countries_override, **ignore) -> None:
    """Generate datasets and create them in HDX"""

    with ErrorsOnExit() as errors:
        with temp_dir() as temp_folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, temp_folder, "saved_data", temp_folder, False, False
                )
                configuration = Configuration.read()
                cod = COD(retriever, errors)
                datasets_metadata = cod.get_datasets_metadata(
                    configuration["url"],
                    countries_override,
                    enhanced_only=True,
                    boundaries_only=True,
                )
                logger.info(f"Number of datasets to upload: {len(datasets_metadata)}")
                datasets_to_update = []
                for metadata in datasets_metadata:
                    dataset, batch = cod.generate_dataset(metadata, latest_only=True)
                    if dataset:
                        datasets_to_update.append([dataset, batch])

                if not countries_override:
                    countries_override = [c for c in Country.countriesdata()["countries"]]
                for country in countries_override:
                    dataset = Dataset.read_from_hdx(f"cod-ps-{country.lower()}")

                    if not dataset:
                        continue

                    dataset, batch = cod.add_population_services(dataset, country, configuration["ps_url"])
                    if dataset:
                        datasets_to_update.append([dataset, batch])

                for dataset, batch in datasets_to_update:
                    try:
                        dataset.create_in_hdx(
                            hxl_update=False,
                            remove_additional_resources=True,
                            updated_by_script="HDX Scraper: CODS",
                            batch=batch,
                            ignore_fields=["num_of_rows", "resource:description"],
                        )
                    except HDXError as ex:
                        logger.exception(f"Dataset: {metadata['DatasetTitle']} could not be uploaded")
                        errors.add(f"Dataset: {metadata['DatasetTitle']}, error: {ex}")


if __name__ == "__main__":
    args = parse_args()
    if args.countries_override:
        countries_override = args.countries_override.split(",")
    else:
        countries_override = None
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml"),
        countries_override=countries_override,
    )
