#!/usr/bin/python
"""
Unit tests for cods.

"""
from os.path import join

import hdx
import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.location.country import Country
from hdx.utilities.uuid import is_valid_uuid
from hdx.utilities.loader import load_json
from hdx.utilities.errors_onexit import ErrorsOnExit

from cods import COD

alljson = load_json(join("tests", "fixtures", "apiinput.json"))


class TestCods:
    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            user_agent="test",
            hdx_key="12345",
            project_config_yaml=join("tests", "config", "project_configuration.yml"),
        )
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
                {"name": "phl", "title": "Philippines"},
            ]
        )
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": "common operational dataset - cod"},
                {"name": "administrative divisions"},
                {"name": "geodata"},
                {"name": "gazetteer"},
            ],
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

    @pytest.fixture(scope="class")
    def downloader(self):
        class Download:
            @staticmethod
            def download(url):
                pass

            @staticmethod
            def get_json():
                return alljson

        return Download()

    @pytest.fixture(scope="class")
    def Dataset(self):
        class Dataset:
            @staticmethod
            def read_from_hdx():
                return None

    @pytest.fixture(scope="class")
    def cod(self, downloader):
        return COD(downloader, ErrorsOnExit())

    def test_get_dataset_titles(self, configuration):
        cod = COD(hdx.utilities.downloader.Download(user_agent="test"), ErrorsOnExit())
        dataset_titles = cod.get_dataset_titles(configuration["config_url"])
        assert dataset_titles == [
            "Colombia - Subnational Administrative Divisions",
            "Ethiopia - Subnational Administrative Divisions",
            "Iraq - Subnational Administrative Divisions",
            "Kyrgyzstan - Subnational Administrative Divisions",
            "Liberia - Subnational Administrative Divisions",
            "Mongolia - Subnational Administrative Divisions",
            "Somalia - Subnational Administrative Divisions",
            "Syrian Arab Republic - Subnational Administrative Divisions",
            "Ukraine - Subnational Administrative Divisions",
            "Yemen - Subnational Administrative Divisions",
        ]

    def test_get_datasets_metadata(self, cod):
        datasets_metadata = cod.get_datasets_metadata("http://lala")
        assert datasets_metadata == alljson
        datasets_metadata = cod.get_datasets_metadata(
            "http://lala",
            dataset_titles=[
                "Philippines - Subnational Administrative Boundaries",
            ],
        )
        assert datasets_metadata == [alljson[0]]

    def test_generate_dataset(self, cod, configuration):
        dataset, batch = cod.generate_dataset(alljson[0])
        assert is_valid_uuid(batch) is True
        assert dataset == {
            "name": "cod-ab-phl",
            "title": "Philippines - Subnational Administrative Boundaries",
            "notes": "Philippines administrative levels",
            "dataset_source": "National Mapping and Resource Information Authority (NAMRIA), Philippines Statistics Authority (PSA)",
            "methodology": "Census",
            "methodology_other": "",
            "license_id": "Creative Commons Attribution for Intergovernmental Organisations",
            "license_other": "",
            "caveats": "Prepared by OCHA",
            "data_update_frequency": "365",
            "cod_level": "cod-enhanced",
            "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
            "owner_org": "27fbd3ff-d0f4-4658-8a69-a07f49a7a853",
            "subnational": "1",
            "groups": [{"name": "phl"}],
            "tags": [
                {
                    "name": "administrative divisions",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
                {
                    "name": "common operational dataset - cod",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
                {
                    "name": "gazetteer",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
            ],
            "dataset_date": "[2020-05-29T00:00:00 TO 2021-05-29T00:00:00]",
        }
        resources = dataset.get_resources()
        assert len(resources) == alljson[0]["Total"]
        assert resources == [
            {
                "name": "Philippines - Subnational Administrative Boundaries",
                "description": "Philippines - Subnational Administrative Boundaries",
                "url": "https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/80e52e31-d06c-47ab-9bfa-cbb57dc17a1e/download/phl_adminboundaries_tabulardata.xlsx",
                "format": "xlsx",
                "daterange_for_data": "[2020-05-29T00:00:00 TO 2021-05-29T00:00:00]",
                "grouping": "Latest",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "Philippines - Subnational Administrative Boundaries",
                "description": "Philippines - Subnational Administrative Boundaries",
                "url": "https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/12457689-6a86-4474-8032-5ca9464d38a8/download/phl_adm_psa_namria_20200529_shp.zip",
                "format": "shp",
                "daterange_for_data": "[2020-05-29T00:00:00 TO 2021-05-29T00:00:00]",
                "grouping": "Latest",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "Philippines - Subnational Administrative Boundaries",
                "description": "Philippines - Subnational Administrative Boundaries",
                "url": "https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/82cda4bf-19f1-47bf-9469-f8bb8e5446d3/download/phl_adm_psa_namria_20200529_emf.zip",
                "format": "emf",
                "daterange_for_data": "[2020-05-29T00:00:00 TO 2021-05-29T00:00:00]",
                "grouping": "Latest",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "Philippines - Subnational Administrative Boundaries",
                "description": "Philippines - Subnational Administrative Boundaries",
                "url": "https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/c8fd86b6-eac2-4e7d-8ee3-6f0d01b991da/download/phl_adminboundaries_candidate_adm3.gdb.zip",
                "format": "geodatabase",
                "daterange_for_data": "[2020-05-29T00:00:00 TO 2021-05-29T00:00:00]",
                "grouping": "Latest",
                "resource_type": "api",
                "url_type": "api",
            },
        ]

        dataset, batch = cod.generate_dataset(alljson[1])
        assert is_valid_uuid(batch) is True
        assert dataset == {
            "name": "cod-ab-afg",
            "title": "Afghanistan - Subnational Administrative Boundaries",
            "notes": "Afghanistan administrative level 0 (country), 1 (province), and 2 (district)",
            "dataset_source": "Afghanistan Geodesy and Cartography Head Office (AGCHO)",
            "methodology": "Other",
            "methodology_other": "ITOS processing",
            "license_id": "",
            "license_other": "",
            "caveats": "In-country humanitarian responders",
            "data_update_frequency": "365",
            "cod_level": "cod-enhanced",
            "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
            "owner_org": "10e168ce-5b51-49ac-8616-a142d48618e5",
            "subnational": "1",
            "groups": [{"name": "afg"}],
            "tags": [
                {
                    "name": "administrative divisions",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
                {
                    "name": "common operational dataset - cod",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
                {
                    "name": "gazetteer",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
                {
                    "name": "geodata",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
            ],
            "dataset_date": "[2019-10-22T00:00:00 TO 2019-10-22T23:59:59]",
            "is_requestdata_type": True,
            "file_types": "shp,geodatabase",
            "field_names": "Afghanistan administrative level 0-2 and UNAMA region boundary polygons,lines,and points",
        }
        resources = dataset.get_resources()
        assert len(resources) == alljson[1]["Total"]
        assert resources == []
