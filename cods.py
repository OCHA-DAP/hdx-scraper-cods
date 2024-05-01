import gspread
import json
import logging
from os.path import join

from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import dict_of_dicts_add, read_list_from_csv, write_list_to_csv

logger = logging.getLogger(__name__)


class COD:
    def __init__(self, temp_folder, errors, save, use_saved, gsheet_auth=None):
        self.save = save
        self.use_saved = use_saved
        self.gsheet_auth = gsheet_auth
        self.errors = errors
        if save:
            self.folder = "saved_data"
        else:
            self.folder = temp_folder
        self.metadata = {}
        self.dataset_info = [[
            "country",
            "theme",
            "dataset",
            "metadata item",
            "resource name",
            "metadata value",
            "updated metadata value",
        ]]

    def get_metadata(self, url, countries=None):
        if self.use_saved:
            rows = read_list_from_csv(join(self.folder, "download_cod_metadata.csv"))
        else:
            info = json.loads(self.gsheet_auth)
            scopes = ["https://www.googleapis.com/auth/spreadsheets"]
            gc = gspread.service_account_from_dict(info, scopes=scopes)
            spreadsheet = gc.open_by_url(url)
            tab = spreadsheet.worksheet("metadata")
            rows = tab.get_all_values()
        if self.save:
            write_list_to_csv(join(self.folder, "download_cod_metadata.csv"), rows[1:])

        headers = rows[1]
        for row in rows[2:]:
            dataset_name = row[headers.index("dataset")]
            country = row[headers.index("country")]
            if countries and country not in countries:
                continue
            if not row[headers.index("updated metadata value")] \
                    or row[headers.index("updated metadata value")] == "":
                continue
            if not row[headers.index("resource name")] or row[headers.index("resource name")] == "":
                dict_of_dicts_add(
                    self.metadata,
                    dataset_name,
                    row[headers.index("metadata item")],
                    row[headers.index("updated metadata value")],
                )
            else:
                dict_of_dicts_add(
                    self.metadata,
                    dataset_name,
                    row[headers.index("resource name")],
                    row[headers.index("updated metadata value")],
                )

        return [{"name": dataset_name} for dataset_name in sorted(self.metadata)]

    def generate_dataset(self, dataset_name):
        metadata = self.metadata[dataset_name]
        update = False

        dataset = Dataset.read_from_hdx(dataset_name)
        if not dataset:
            return None, update

        for metadata_item in metadata:
            if metadata_item not in dataset:
                continue
            hdx_metadata = dataset[metadata_item]
            new_metadata = metadata[metadata_item]
            if new_metadata != hdx_metadata:
                dataset[metadata_item] = new_metadata
                if metadata_item == "notes":
                    dataset["notes"] = dataset["notes"].replace(
                        "\n", "  \n"
                    )
                update = True

        resources = dataset.get_resources()
        for resource in resources:
            resource_name = resource["name"]
            hdx_description = resource["description"]
            if resource_name not in metadata:
                self.errors(f"{dataset_name}: resource {resource_name} not in metadata")
                return None, update
            new_description = metadata[resource_name]
            if new_description != hdx_description:
                resource["description"] = new_description
                update = True

        return dataset, update

    def get_dataset_info(self):
        datasets = Dataset.search_in_hdx(
            fq='cod_level:"cod-standard"'
        ) + Dataset.search_in_hdx(
            fq='cod_level:"cod-enhanced"'
        )

        for dataset in datasets:
            theme = None
            if dataset["name"][:6] in ["cod-ab", "cod-ps", "cod-hp", "cod-em"]:
                theme = dataset["name"][4:6].upper()

            if not theme:
                continue

            country = " | ".join(dataset.get_location_iso3s())
            dataset_name = dataset["name"]

            for metadata_item in ["notes", "cod_level"]:
                self.dataset_info.append(
                    [
                        country,
                        theme,
                        dataset_name,
                        metadata_item,
                        "",
                        dataset[metadata_item],
                        "",
                    ]
                )
            for resource in dataset.get_resources():
                self.dataset_info.append(
                    [
                        country,
                        theme,
                        dataset_name,
                        "resource_description",
                        resource["name"],
                        resource["description"],
                        "",
                    ]
                )

    def write_to_gsheets(self, url):
        info = json.loads(self.gsheet_auth)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        gc = gspread.service_account_from_dict(info, scopes=scopes)
        spreadsheet = gc.open_by_url(url)
        tab = spreadsheet.worksheet("metadata")
        tab.clear()
        tab.update("A1", [["Do Not Edit"]])
        tab.update("A2", self.dataset_info)
        return
