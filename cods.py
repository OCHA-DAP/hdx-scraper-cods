import gspread
import json
import logging

from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import dict_of_dicts_add

logger = logging.getLogger(__name__)


class COD:
    def __init__(self, retriever, errors):
        self.retriever = retriever
        self.metadata = {}
        self.errors = errors
        self.dataset_info = [[
            "country",
            "theme",
            "dataset",
            "metadata item",
            "resource name",
            "metadata value",
            "updated metadata value",
        ]]

    def get_dataset_names(self, url, countries=None):
        _, iterator = self.retriever.get_tabular_rows(url, format="csv", dict_form=True, headers=2)
        dataset_names = []
        for row in iterator:
            country = row["country"]
            if countries and country not in countries:
                continue
            dataset_name = row["dataset"]
            if dataset_name not in dataset_names:
                dataset_names.append(dataset_name)
        return dataset_names

    def get_metadata(self, url, countries=None):
        _, iterator = self.retriever.get_tabular_rows(url, format="csv", dict_form=True, headers=2)

        for row in iterator:
            dataset_name = row["dataset"]
            country = row["country"]
            if countries and country not in countries:
                continue
            if not row["updated metadata value"] or row["updated metadata value"] == "":
                continue
            if not row["resource name"] or row["resource name"] == "":
                dict_of_dicts_add(self.metadata, dataset_name, row["metadata item"], row["updated metadata value"])
            else:
                dict_of_dicts_add(self.metadata, dataset_name, row["resource name"], row["updated metadata value"])

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

    def write_to_gsheets(self, url, gsheet_auth):
        info = json.loads(gsheet_auth)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        gc = gspread.service_account_from_dict(info, scopes=scopes)
        spreadsheet = gc.open_by_url(url)
        tab = spreadsheet.worksheet("metadata")
        tab.clear()
        tab.update("A1", [["Do Not Edit"]])
        tab.update("A2", self.dataset_info)
        return
