import argparse
import logging
from os import getenv
from os.path import expanduser, join

from cods import COD

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-cods"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-gs",
        "--gsheet_auth",
        default=None,
        help="Credentials for accessing Google Sheets",
    )
    parser.add_argument(
        "-sv",
        "--save",
        default=False,
        action="store_true",
        help="Save downloaded data",
    )
    parser.add_argument(
        "-usv",
        "--use_saved",
        default=False,
        action="store_true",
        help="Use saved data",
    )
    args = parser.parse_args()
    return args


def main(
    # gsheet_auth,
    save,
    use_saved,
    **ignore,
):
    """Generate datasets and create them in HDX"""

    with ErrorsOnExit() as errors:
        with temp_dir() as temp_folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, temp_folder, "saved_data", temp_folder, save, use_saved
                )
                configuration = Configuration.read()
                cod = COD(retriever, errors)
                dataset_names = cod.get_metadata(configuration["url"])
                logger.info(f"Number of datasets to upload: {len(dataset_names)}")
                for dataset_name in dataset_names:
                    dataset, update = cod.generate_dataset(dataset_name)
                    if dataset and update:
                        try:
                            dataset.create_in_hdx(
                                hxl_update=False,
                                updated_by_script="HDX Scraper: CODS",
                                batch_mode="KEEP_OLD",
                                ignore_fields=["num_of_rows", "resource:description"],
                            )
                        except HDXError as ex:
                            errors.add(f"Dataset: {dataset_name}, error: {ex}")

                logger.info("Getting dataset info")
                cod.get_dataset_info()
                cod.write_to_gsheets(configuration["googlesheets"], gsheet_auth)


if __name__ == "__main__":
    args = parse_args()
    gsheet_auth = args.gsheet_auth
    if gsheet_auth is None:
        gsheet_auth = getenv("GSHEET_AUTH")
    facade(
        main,
        hdx_site="stage",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml"),
        gsheet_auth=gsheet_auth,
        save=args.save,
        use_saved=args.use_saved,
    )
