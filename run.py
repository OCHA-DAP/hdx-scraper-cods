import logging
from os.path import expanduser, join

from cods import COD

from hdx.api.configuration import Configuration
from hdx.location.country import Country
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-cods"


def main(**ignore):
    configuration = Configuration.read()
    with ErrorsOnExit() as errors:
        with Download() as downloader:
            cod = COD(
                downloader,
                configuration["ab_url"],
                configuration["em_url"],
                configuration["ps_url"],
                errors,
            )

            boundary_jsons = cod.get_boundary_jsons()
            if len(boundary_jsons) < 2:
                cod.errors.add("Could not get boundary service data")
                return

            countries = Country.countriesdata()["countries"]
            dataset_types = ["ab", "em", "ps"]

            for country in countries:
                for dataset_type in dataset_types:
                    try:
                        dataset = Dataset.read_from_hdx(f"cod-{dataset_type}-{country.lower()}")
                    except HDXError:
                        logger.warning(f"Could not read cod-{dataset_type}-{country.lower()}")
                        continue

                    if not dataset:
                        continue

                    dataset = cod.update_dataset(dataset, countries[country], dataset_type, boundary_jsons)

                    if not dataset:
                        continue

                    try:
                        dataset.create_in_hdx(
                            hxl_update=False,
                            batch_mode="KEEP_OLD",
                            updated_by_script="HDX Scraper: CODS",
                            remove_additional_resources=True,
                            ignore_fields=["num_of_rows", "resource:description"],
                        )
                    except HDXError as ex:
                        logger.exception(f"Dataset: {dataset['name']} could not be uploaded")
                        errors.add(f"Dataset: {dataset['name']}, error: {ex}")


if __name__ == "__main__":
    facade(
        main,
        hdx_site="prod",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
