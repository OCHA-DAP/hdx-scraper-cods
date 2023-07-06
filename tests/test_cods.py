from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
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
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def input_folder(self, fixtures):
        return join(fixtures, "input")

    def test_update_dataset(
        self, configuration, fixtures, input_folder
    ):
        with temp_dir() as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, input_folder, folder, False, True
                )
                cod = COD(
                    retriever,
                    configuration["ab_url"],
                    configuration["em_url"],
                    configuration["ps_url"],
                    ErrorsOnExit(),
                )
                boundary_jsons = cod.get_boundary_jsons()
                country = {
                    "#country+code+v_iso3": "POL",
                    "#country+name+preferred": "Poland",
                }

                dataset_em = Dataset.load_from_json(join(fixtures, "dataset-cod-em-pol.json"))
                dataset_em = cod.update_dataset(dataset_em, country, "em", boundary_jsons)
                outdataset_em = Dataset.load_from_json(join(fixtures, "dataset-cod-em-pol_updated.json"))
                assert dataset_em.get_resources() == outdataset_em.get_resources()

                dataset_ps = Dataset.load_from_json(join(fixtures, "dataset-cod-ps-pol.json"))
                dataset_ps = cod.update_dataset(dataset_ps, country, "ps", boundary_jsons)
                outdataset_ps = Dataset.load_from_json(join(fixtures, "dataset-cod-ps-pol_updated.json"))
                assert dataset_ps.get_resources() == outdataset_ps.get_resources()
