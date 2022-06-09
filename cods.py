#!/usr/bin/python
"""
CODS:
-----

Generates urls from the COD website.

"""
import logging

from hdx.data.dataset import Dataset
from hdx.data.date_helper import DateHelper
from hdx.data.hdxobject import HDXError
from hdx.data.organization import Organization
from hdx.data.resource import Resource
from hdx.utilities.uuid import get_uuid
from slugify import slugify

logger = logging.getLogger(__name__)


class COD:
    def __init__(self, downloader, errors):
        self.downloader = downloader
        self.batches_by_org = dict()
        self.errors = errors

    def get_dataset_titles(self, url):
        headers, iterator = self.downloader.get_tabular_rows(url, dict_form=True)
        return [x["Dataset title"] for x in iterator]

    def get_datasets_metadata(self, url, dataset_titles=None):
        self.downloader.download(url)
        results = self.downloader.get_json()
        if dataset_titles is None:
            return results
        return [x for x in results if x["DatasetTitle"] in dataset_titles]

    def generate_dataset(self, metadata):
        error_count = len(self.errors.errors)
        title = metadata["DatasetTitle"]
        is_requestdata_type = metadata["is_requestdata_type"]
        if not is_requestdata_type:
            if metadata["Total"] == 0:
                self.errors.add(f"Ignoring dataset: {title} which has no resources!")
                return None, None
        if not metadata["Source"]:
            self.errors.add(f"Dataset: {title} has no source!")
        logger.info(f"Creating dataset: {title}")
        cod_level = "cod-standard"
        if metadata["is_enhanced_cod"]:
            cod_level = "cod-enhanced"
        theme = metadata["Theme"]
        if not theme:
            self.errors.add(f"Dataset: {title} has no theme!")
        location = metadata["Location"]
        if theme == "COD_AB" and (location == ["MMR"] or location == ["mmr"]):
            name = slugify(title)
        else:
            name = slugify(f"{theme} {' '.join(location)}")
        dataset = Dataset(
            {
                "name": name[:99],
                "title": title,
                "notes": metadata["DatasetDescription"],
                "dataset_source": metadata["Source"],
                "methodology": metadata["Methodology"],
                "methodology_other": metadata["Methodology_Other"],
                "license_id": metadata["License"],
                "license_other": metadata["License_Other"],
                "caveats": metadata["Caveats"],
                "data_update_frequency": metadata["FrequencyUpdates"],
                "cod_level": cod_level,
            }
        )
        licence = metadata["License"]
        if licence == "Other":
            dataset["license_id"] = "hdx-other"
            dataset["license_other"] = metadata["License_Other"]
        else:
            dataset["license_id"] = licence
        methodology = metadata["Methodology"]
        methodology_other = metadata["Methodology_Other"]
        if methodology == "" and methodology_other == "":
            self.errors.add(f"Dataset: {title} has no methodology!")
        if methodology == "Other":
            dataset["methodology"] = "Other"
            if not methodology_other or methodology_other == "":
                self.errors.add(f"Dataset: {title} has no other methodology!")
            if methodology_other:
                dataset["methodology_other"] = methodology_other
        else:
            dataset["methodology"] = methodology
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        organization = Organization.autocomplete(metadata["Contributor"])
        if len(organization) == 0:
            organization = Organization.autocomplete(metadata["Contributor"].replace(" ", "-"))
        organization_id = None
        batch = None
        try:
            organization_id = organization[0]["id"]
        except IndexError:
            self.errors.add(f"Dataset: {title} has an invalid organization {metadata['Contributor']}!")
        if organization_id:
            dataset.set_organization(organization_id)
            batch = self.batches_by_org.get(organization_id, get_uuid())
            self.batches_by_org[organization_id] = batch
        dataset.set_subnational(True)
        try:
            dataset.add_country_locations(location)
        except HDXError:
            self.errors.add(f"Dataset: {title} has an invalid location {location}!")
        dataset.add_tags(metadata["Tags"])
        if len(dataset.get_tags()) < len(metadata["Tags"]):
            self.errors.add(f"Dataset: {title} has invalid tags!")
        if "common operational dataset - cod" not in dataset.get_tags():
            dataset.add_tag("common operational dataset - cod")
        if is_requestdata_type:
            dataset["dataset_date"] = metadata["DatasetDate"]
            dataset["is_requestdata_type"] = True
            dataset["file_types"] = metadata["file_types"]
            dataset["field_names"] = metadata["field_names"]
            num_of_rows = metadata.get("num_of_rows")
            if num_of_rows:
                dataset["num_of_rows"] = num_of_rows
        else:
            startdate = None
            enddate = None
            ongoing = False
            resources = list()
            for resource_metadata in metadata["Resources"]:
                resource_daterange = resource_metadata["daterange_for_data"]
                format = resource_metadata["Format"]
                if format == "VectorTile":
                    format = "MBTiles"
                    logger.error(f"Dataset: {title} is using file type VectorTile instead of MBTiles")
                resourcedata = {
                    "name": resource_metadata["ResourceItemTitle"],
                    "description": resource_metadata["ResourceItemDescription"],
                    "url": resource_metadata["DownloadURL"],
                    "format": format,
                    "daterange_for_data": resource_daterange,
                    "grouping": resource_metadata["Version"],
                }
                date_info = DateHelper.get_date_info(resource_daterange)
                resource_startdate = date_info["startdate"]
                resource_enddate = date_info["enddate"]
                resource_ongoing = date_info["ongoing"]
                if startdate is None or resource_startdate < startdate:
                    startdate = resource_startdate
                if enddate is None or resource_enddate > enddate:
                    enddate = resource_enddate
                    ongoing = resource_ongoing
                resource = Resource(resourcedata)
                resources.append(resource)
            if ongoing:
                enddate = "*"
            try:
                dataset.add_update_resources(resources)
            except HDXError as ex:
                self.errors.add(f"Dataset: {title} resources could not be added. Error: {ex}")
            dataset.set_date_of_dataset(startdate, enddate)
        if len(self.errors.errors) > error_count:
            return None, None
        else:
            return dataset, batch
