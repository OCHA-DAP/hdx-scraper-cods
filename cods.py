import logging

from hdx.data.dataset import Dataset
from hdx.data.date_helper import DateHelper
from hdx.data.hdxobject import HDXError
from hdx.data.organization import Organization
from hdx.data.resource import Resource
from hdx.utilities.retriever import DownloadError
from hdx.location.country import Country
from hdx.utilities.uuid import get_uuid
from slugify import slugify
logger = logging.getLogger(__name__)


class COD:
    def __init__(self, retriever, errors):
        self.retriever = retriever
        self.batches_by_org = dict()
        self.errors = errors

    def get_dataset_titles(self, url, countries=None):
        results = self.retriever.download_json(url)
        if countries is None:
            return [x["DatasetTitle"] for x in results]
        return [x["DatasetTitle"] for x in results if len(x["Location"]) > 0 and x["Location"][0].upper() in countries]

    def get_datasets_metadata(self, url, countries=None, enhanced_only=True, boundaries_only=True):
        results = self.retriever.download_json(url)
        if enhanced_only:
            results = [x for x in results if x.get("is_enhanced_cod")]
        if boundaries_only:
            results = [x for x in results if x.get("Theme") in ["COD_AB", "COD_EM"]]
        if countries is None:
            return results
        return [x for x in results if len(x["Location"]) > 0 and x["Location"][0].upper() in countries]

    def generate_dataset(self, metadata, latest_only=True):
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
        try:
            hdx_dataset = Dataset.read_from_hdx(name)
        except HDXError:
            logger.error(f"Could not read dataset {name} from HDX")
        customviz = None
        if hdx_dataset:
            customviz = hdx_dataset.get("customviz")
        if customviz:
            dataset["customviz"] = customviz
        licence = metadata["License"]
        if licence == "Other":
            dataset["license_id"] = "hdx-other"
            dataset["license_other"] = metadata["License_Other"]
        else:
            dataset["license_id"] = licence
        methodology = metadata["Methodology"]
        methodology_other = metadata["Methodology_Other"]
        if methodology == "" and methodology_other == "":
            self.errors.add(f"Dataset: {dataset['name']} has no methodology!")
        if methodology == "Other":
            dataset["methodology"] = "Other"
            if not methodology_other or methodology_other == "":
                self.errors.add(f"Dataset: {dataset['name']} has no other methodology!")
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
            self.errors.add(f"Dataset: {dataset['name']} has an invalid organization {metadata['Contributor']}!")
        if organization_id:
            dataset.set_organization(organization_id)
            batch = self.batches_by_org.get(organization_id, get_uuid())
            self.batches_by_org[organization_id] = batch
        dataset.set_subnational(True)
        try:
            dataset.add_country_locations(location)
        except HDXError:
            self.errors.add(f"Dataset: {dataset['name']} has an invalid location {location}!")
        tags = [t for t in metadata["Tags"] if t.replace(" ", "") != "commonoperationaldataset-cod"]
        dataset.add_tags(tags)
        if len(dataset.get_tags()) < len(tags):
            self.errors.add(f"Dataset: {dataset['name']} has invalid tags!")
        if theme in ["COD_AB", "COD_EM"]:
            if "baseline population" in dataset.get_tags():
                dataset.remove_tag("baseline population")
            if "administrative boundaries-divisions" not in dataset.get_tags():
                dataset.add_tag("administrative boundaries-divisions")
        if theme == "COD_PS":
            if "baseline population" not in dataset.get_tags():
                dataset.add_tag("baseline population")
            if "administrative boundaries-divisions" in dataset.get_tags():
                dataset.remove_tag("administrative boundaries-divisions")
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
                version = resource_metadata["Version"]
                if latest_only and version.lower() != "latest":
                    continue
                resource_daterange = resource_metadata["daterange_for_data"]
                format = resource_metadata["Format"]
                if format == "VectorTile":
                    format = "MBTiles"
                    logger.error(f"Dataset: {dataset['name']} is using file type VectorTile instead of MBTiles")
                resourcedata = {
                    "name": resource_metadata["ResourceItemTitle"],
                    "description": resource_metadata["ResourceItemDescription"],
                    "url": resource_metadata["DownloadURL"],
                    "format": format,
                    "daterange_for_data": resource_daterange,
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
                self.errors.add(f"Dataset: {dataset['name']} resources could not be added. Error: {ex}")
            dataset.set_reference_period(startdate, enddate, ongoing)
        if len(self.errors.errors) > error_count:  # if more errors were generated, do not push dataset to HDX
            return None, None
        else:
            return dataset, batch

    def add_population_services(self, dataset, iso, url):
        country_name = Country.get_country_name_from_iso3(iso)

        resources = list()
        do_not_continue = False
        for adm in range(0, 5):
            resource = dict()
            if do_not_continue:
                continue

            resource["url"] = url.replace("/iso", f"/{iso}").replace("/adm/", f"/{adm}/")
            resource["name"] = f"{iso.upper()} admin {adm} population"
            resource["format"] = "JSON"
            try:
                year = self.retriever.download_json(resource["url"], file_prefix=str(adm))
            except (DownloadError, FileNotFoundError):
                do_not_continue = True
                continue

            if len(year) == 1 and year[0].get("status"):
                do_not_continue = True
                continue

            year = year.get("Year")
            if not year:
                continue

            resource["description"] = f"{country_name} administrative level {adm} {year} population statistics"
            resources.append(resource)

        for resource in reversed(dataset.get_resources()):
            if resource.get_file_type() not in ["geoservice", "json"]:
                continue
            if "itos.uga.edu" not in resource["url"]:
                self.errors.add(f"Dataset: {dataset['name']} has service resource {resource['url']}")
                continue

            try:
                dataset.delete_resource(resource, delete=False)
            except HDXError:
                self.errors.add(f"Dataset: {dataset['name']} service resources could not be deleted")
                continue

        try:
            dataset.add_update_resources(resources)
        except HDXError as ex:
            self.errors.add(f"Dataset: {dataset['name']} resources could not be added. Error: {ex}")
            return None, None

        organization_id = dataset.get_organization()["id"]
        batch = self.batches_by_org.get(organization_id, get_uuid())
        self.batches_by_org[organization_id] = batch

        return dataset, batch
