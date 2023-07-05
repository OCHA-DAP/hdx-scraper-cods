from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.loader import load_json
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.useragent import UserAgent

from cods import COD


class TestCods:
    country = {
        "#country+code+v_iso3": "POL",
        "#country+name+preferred": "Poland",
    }
    boundary_jsons = {
        "em": load_json(join("tests", "fixtures", "COD_External_Edgematch.json")),
    }

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
    def downloader(self):
        class Download:
            @staticmethod
            def download_json(url):
                if "COD_External" in url:
                    filename = "POL_PL_Edgematch.json"
                else:
                    adm = url.split("/")[-3]
                    if int(adm) > 1:
                        return dict()
                    filename = f"cod-ps-pol_adm{adm}.json"

                data = load_json(join("tests", "fixtures", filename))
                return data

        return Download()

    @pytest.fixture(scope="class")
    def cod(self, configuration):
        cod = COD(
            self.downloader,
            configuration["ab_url"],
            configuration["em_url"],
            configuration["ps_url"],
            ErrorsOnExit(),
        )
        return cod

    def test_em(self, downloader, configuration):
        cod = COD(downloader, configuration["ab_url"], configuration["em_url"], configuration["ps_url"], ErrorsOnExit())
        dataset = Dataset.load_from_json(join("tests", "fixtures", "cod-em-pol.json"))
        dataset = cod.update_dataset(dataset, self.country, "em", self.boundary_jsons)
        outdataset = Dataset.load_from_json(join("tests", "fixtures", "cod-em-pol_final.json"))
        assert dataset.get_resources() == outdataset.get_resources()

    def test_ps(self, downloader, configuration):
        cod = COD(downloader, configuration["ab_url"], configuration["em_url"], configuration["ps_url"], ErrorsOnExit())
        dataset = Dataset.load_from_json(join("tests", "fixtures", "cod-ps-pol.json"))
        dataset = cod.update_dataset(dataset, self.country, "ps", self.boundary_jsons)
        outdataset = Dataset.load_from_json(join("tests", "fixtures", "cod-ps-pol_final.json"))
        assert dataset.get_resources() == outdataset.get_resources()
