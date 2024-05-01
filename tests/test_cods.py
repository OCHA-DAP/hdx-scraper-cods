from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from cods import COD


class TestCods:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join("config", "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def Dataset(self):
        class Dataset:
            @staticmethod
            def read_from_hdx(dataset_name):
                return Dataset.load_from_json(join("tests", "fixtures", f"dataset-{dataset_name}.json"))

    @pytest.fixture(scope="function")
    def fixtures_folder(self):
        return join("tests", "fixtures")

    def test_get_dataset_names(self, configuration, fixtures_folder):
        with temp_dir() as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, fixtures_folder, folder, False, True
                )
                cod = COD(retriever, ErrorsOnExit())
                dataset_names = cod.get_dataset_names(configuration["url"], countries=["COL", "ETH"])
                assert dataset_names == [
                    "cod-ps-eth",
                    "cod-ps-col",
                    "cod-ab-col",
                    "cod-ab-eth",
                ]

    def test_get_metadata(self, configuration, fixtures_folder):
        with temp_dir() as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, fixtures_folder, folder, False, True
                )
                cod = COD(retriever, ErrorsOnExit())
                dataset_names = cod.get_metadata(
                    configuration["url"],
                )
                assert dataset_names == [
                    {"name": "cod-ab-lca"},
                    {"name": "cod-ab-mdg"},
                    {"name": "cod-ps-dji"},
                ]
                assert cod.metadata == {
                    "cod-ps-dji": {
                        "cod_level": "cod-enhanced",
                        "notes": "Djibouti administrative level 0-1 2009 population statistics test description",
                        "dji_admpop_2009.xlsx": "Djibouti administrative level 0-1 2009 population statistics test",
                        "dji_admpop_adm0_2009.csv": "Djibouti administrative level 0 2009 population statistics test",
                        "dji_admpop_adm1_2009.csv": "Djibouti administrative level 1 2009 population statistics test",
                    },
                    "cod-ab-mdg": {
                        "cod_level": "cod-standard",
                        "notes": "Madagascar administrative level 0 (country), 1 (region), 2 (district), 3 (commune), and 4 (fokontany) boundary and line shapefiles, geodatabase, and gazetteer.",
                        "mdg_gazetteer_20181031.xls": "Madagascar administrative level 0 (country), 1 (region), 2 (district), 3 (commune), and 4 (fokontany) gazetteer",
                        "mdg_adm_BNGRC_OCHA_20181031_SHP.zip": "Madagascar administrative level 0 (country), 1 (region), 2 (district), 3 (commune), and 4 (fokontany) boundary shapefiles",
                        "mdg_admbnda_adm0_BNGRC_OCHA_20181031.emf": "Madagascar administrative level 0 (country) boundary polygon EMF file",
                        "mdg_admbnda_adm1_BNGRC_OCHA_20181031.emf": "Madagascar administrative level 1 (region) boundary polygon EMF file",
                        "mdg_admbnda_adm2_BNGRC_OCHA_20181031.emf": "Madagascar administrative level 2 (district) boundary polygon EMF file",
                        "mdg_admbnda_adm3_BNGRC_OCHA_20181031.emf": "Madagascar administrative level 3 (commune) boundary polygon EMF file",
                        "mdg_admbnda_adm4_BNGRC_OCHA_20181031.emf": "Madagascar administrative level 4 (fokontany) boundary polygon EMF file",
                        "mdg_admbndl_all_BNGRC_OCHA_20181031.zip": "Madagascar administrative level 0 (country), 1 (region), 2 (district), 3 (commune), and 4 (fokontany) boundary and line geodatabase",
                        "mdg_cod_metadata_20181031.pdf": "Metadata notes updated 2018 10 31",
                        "BNGRC_codes_LUT_pcodes_2018.xlsx": "Excel table of P-codes and related BNGRC (Disaster Management Agency) codes for Administrative Boundaries level 1-2."
                    },
                    "cod-ab-lca": {
                        "cod_level": "cod-standard",
                        "notes": "Saint Lucia administrative level 0 (nation), 1 (quarter) and 2 boundaries\n \n \n \n The administrative level 0 and 1 boundaries are suitable for database or GIS linkage to the [Saint Lucia administrative levels 0-1 sex and age disaggregated population statistics](https://data.humdata.org/dataset/cod-ps-lca) tables.",
                        "lca_adm_gazetteer.xlsx": "St. Lucia administrative level 0 (country) and 1 (quarter) gazetteer",
                        "lca_admbnda_gov_2019_SHP.zip": "St. Lucia administrative level 0 (country), 1 (quarter), and 2 boundary polygon and line shapefiles",
                        "lca_adm_gov_2019_EMF.zip": "St. Lucia administrative level 0 (country), 1 (quarter), and 2 boundary polygon EMF files",
                        "lca_adm_gov_2019.gdb.zip": "St. Lucia administrative level 0 (country), 1 (quarter), and 2 boundary polygon geodatabase",
                    },
                }

    def test_generate_dataset(self, configuration, fixtures_folder):
        with temp_dir() as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, fixtures_folder, folder, False, True
                )
                cod = COD(retriever, ErrorsOnExit())
                cod.get_metadata(configuration["url"])
                dataset, update = cod.generate_dataset("cod-ps-dji")
                assert update is True
                assert dataset["cod_level"] == "cod-enhanced"
                assert dataset["notes"] == "Djibouti administrative level 0-1 2009 population statistics test description"
                resources = dataset.get_resources()
                assert len(resources) == 3
                assert [r["description"] for r in resources] == [
                    "Djibouti administrative level 0-1 2009 population statistics test",
                    "Djibouti administrative level 0 2009 population statistics test",
                    "Djibouti administrative level 1 2009 population statistics test",
                ]
