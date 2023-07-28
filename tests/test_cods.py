from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.dataset import Dataset
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent
from hdx.utilities.uuid import is_valid_uuid

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
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
            ]
        )
        Country.countriesdata(use_live=False)
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": "administrative boundaries-divisions"},
                {"name": "baseline population"},
                {"name": "geodata"},
                {"name": "gazetteer"},
            ],
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

    @pytest.fixture(scope="class")
    def Dataset(self):
        class Dataset:
            @staticmethod
            def read_from_hdx():
                return None

    @pytest.fixture(scope="function")
    def fixtures_folder(self):
        return join("tests", "fixtures")

    def test_get_dataset_titles(self, configuration, fixtures_folder):
        with temp_dir() as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, fixtures_folder, folder, False, True
                )
                cod = COD(retriever, ErrorsOnExit())
                dataset_titles = cod.get_dataset_titles(configuration["url"], countries=["COL", "ETH", "IRQ"])
                assert dataset_titles == [
                    "Colombia - Subnational Administrative Boundaries",
                    "Colombia - Subnational Population Statistics",
                    "Ethiopia - Subnational Administrative Boundaries",
                    "Iraq - Subnational Administrative Boundaries",
                ]

    def test_get_datasets_metadata(self, configuration, fixtures_folder):
        with temp_dir() as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, fixtures_folder, folder, False, True
                )
                cod = COD(retriever, ErrorsOnExit())
                datasets_metadata = cod.get_datasets_metadata(configuration["url"], countries=["AFG"])
                assert datasets_metadata[0] == {
                    'DatasetTitle': 'Afghanistan - Subnational Administrative Boundaries',
                    'DatasetDescription': 'Afghanistan administrative level 0-2 and UNAMA region gazetteer and P-code geoservices.\r\n\r\nThis gazetteer is compatible with the [Afghanistan - Subnational Population Statistics](https://data.humdata.org/dataset/cod-ps-afg) gazetteer.\r\n\r\nOnly the gazetteer the the P-code geoservices can be made generally available. Humanitarian responders who want the administrative boundary files should make a request by clicking the \'Contact the contributor\' button (below) and including:\r\n\r\na) the following declaration:  "I agree not to share the data with any third party or publish it online without prior permission from AGCHO.  As per the agreement, the datasets are for humanitarian use only."\r\n\r\nb) name\r\n\r\nc) organization or cluster\r\n\r\nd) email address\r\n\r\nThe user will then receive a link to the boundary files, which may only be used according to the above restriction.',
                    'FrequencyUpdates': '365',
                    'DatasetDate': '[2019-10-22T00:00:00 TO *]',
                    'Resources': [
                        {'Format': 'XLSX',
                         'ResourceItemTitle': 'AFG_AdminBoundaries_TabularData.xlsx',
                         'ResourceItemDescription': 'Afghanistan administrative level 0-2 and UNAMA region gazetteer',
                         'DownloadURL': 'https://data.humdata.org/dataset/4c303d7b-8eae-4a5a-a3aa-b2331fa39d74/resource/0238eb07-4f98-4f71-9a03-905c4414f476/download/afg_adminboundaries_tabulardata.xlsx',
                         'Version': 'Latest',
                         'daterange_for_data': '[2021-11-17T00:00:00 TO 2022-11-17T00:00:00]'},
                        {'Format': 'Geoservice',
                         'ResourceItemTitle': 'COD_External/AFG_DA (MapServer)',
                         'ResourceItemDescription': 'This map service contains OCHA Common Operational Datasets for Afghanistan, in Dari: Administrative Boundaries and Regions. The service is available as ESRI Map, ESRI Feature, WMS, and KML Services. See the OCHA COD/FOD terms of use for access and use constraints.',
                         'DownloadURL': 'https://codgis.itos.uga.edu/arcgis/rest/services/COD_External/AFG_DA/MapServer',
                         'Version': 'Latest',
                         'daterange_for_data': '[2021-11-17T00:00:00 TO 2022-11-17T00:00:00]'},
                        {'Format': 'Geoservice',
                         'ResourceItemTitle': 'COD_External/AFG_EN (MapServer)',
                         'ResourceItemDescription': 'This map service contains OCHA Common Operational Datasets for Afghanistan, in English: Administrative Boundaries and Regions. The service is available as ESRI Map, ESRI Feature, WMS, and KML Services. See the OCHA COD/FOD terms of use for access and use constraints.',
                         'DownloadURL': 'https://codgis.itos.uga.edu/arcgis/rest/services/COD_External/AFG_EN/MapServer',
                         'Version': 'Latest',
                         'daterange_for_data': '[2021-11-17T00:00:00 TO 2022-11-17T00:00:00]'},
                        {'Format': 'Geoservice',
                         'ResourceItemTitle': 'COD_External/AFG_pcode (MapServer)',
                         'ResourceItemDescription': "This service is intended as a labelling layer for PCODES from OCHA's Common Operational Datasets for Afghanistan. As a map service it is intended to be used in conjunction with the basemap located at http://gistmaps.itos.uga.edu/arcgis/rest/services/COD_External/AFG_EN/MapServer. The service is available as ESRI Map, WMS, WFS and KML Services.",
                         'DownloadURL': 'https://codgis.itos.uga.edu/arcgis/rest/services/COD_External/AFG_pcode/MapServer',
                         'Version': 'Latest',
                         'daterange_for_data': '[2021-11-17T00:00:00 TO 2022-11-17T00:00:00]'}
                    ],
                    'Source': 'Afghanistan Geodesy and Cartography Head Office (AGCHO)',
                    'Contributor': 'OCHA Field Information Services Section (FISS)',
                    'Location': ['afg'],
                    'Theme': 'COD_AB',
                    'Visibility': 'True',
                    'License': 'Creative Commons Attribution for Intergovernmental Organisations',
                    'License_Other': '',
                    'Methodology': 'Other',
                    'Methodology_Other': 'ITOS processing',
                    'Caveats': 'In-country humanitarian responders in Afghanistan can collect a copy of latest available datasets from OCHA Afghanistan as a member of the Information Management Working Group (IMWG).\r\n\r\nThese datasets are available for purchase from the [National Statistic and Information Authority]( https://www.nsia.gov.af/home) (NSIA)  in Afghanistan.',
                    'is_requestdata_type': False,
                    'is_enhanced_cod': True,
                    'file_types': '',
                    'field_names': '',
                    'Tags': ['administrative divisions', 'common operational dataset - cod', 'geodata', 'gazetteer'],
                    'Total': 4
                }

    def test_generate_dataset(self, configuration, fixtures_folder):
        with temp_dir() as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, fixtures_folder, folder, False, True
                )
                cod = COD(retriever, ErrorsOnExit())
                datasets_metadata = cod.get_datasets_metadata(configuration["url"], countries=["AFG"])
                dataset, batch = cod.generate_dataset(datasets_metadata[0])
                assert is_valid_uuid(batch) is True
                assert dataset == {
                    'name': 'cod-ab-afg',
                    'title': 'Afghanistan - Subnational Administrative Boundaries',
                    'notes': 'Afghanistan administrative level 0-2 and UNAMA region gazetteer and P-code geoservices.\r\n\r\nThis gazetteer is compatible with the [Afghanistan - Subnational Population Statistics](https://data.humdata.org/dataset/cod-ps-afg) gazetteer.\r\n\r\nOnly the gazetteer the the P-code geoservices can be made generally available. Humanitarian responders who want the administrative boundary files should make a request by clicking the \'Contact the contributor\' button (below) and including:\r\n\r\na) the following declaration:  "I agree not to share the data with any third party or publish it online without prior permission from AGCHO.  As per the agreement, the datasets are for humanitarian use only."\r\n\r\nb) name\r\n\r\nc) organization or cluster\r\n\r\nd) email address\r\n\r\nThe user will then receive a link to the boundary files, which may only be used according to the above restriction.',
                    'dataset_source': 'Afghanistan Geodesy and Cartography Head Office (AGCHO)',
                    'methodology': 'Other',
                    'methodology_other': 'ITOS processing',
                    'license_id': 'Creative Commons Attribution for Intergovernmental Organisations',
                    'license_other': '',
                    'caveats': 'In-country humanitarian responders in Afghanistan can collect a copy of latest available datasets from OCHA Afghanistan as a member of the Information Management Working Group (IMWG).\r\n\r\nThese datasets are available for purchase from the [National Statistic and Information Authority]( https://www.nsia.gov.af/home) (NSIA)  in Afghanistan.',
                    'data_update_frequency': '365',
                    'cod_level': 'cod-enhanced',
                    'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                    'owner_org': 'b3a25ac4-ac05-4991-923c-d25f47bef1ec',
                    'subnational': '1',
                    'groups': [{'name': 'afg'}],
                    'tags': [
                        {'name': 'administrative boundaries-divisions', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'geodata', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'gazetteer', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}
                    ],
                    'dataset_date': '[2021-11-17T00:00:00 TO 2022-11-17T23:59:59]'
                }
                resources = dataset.get_resources()
                assert len(resources) == 4
                assert resources == [
                    {
                        'name': 'AFG_AdminBoundaries_TabularData.xlsx',
                        'description': 'Afghanistan administrative level 0-2 and UNAMA region gazetteer',
                        'url': 'https://data.humdata.org/dataset/4c303d7b-8eae-4a5a-a3aa-b2331fa39d74/resource/0238eb07-4f98-4f71-9a03-905c4414f476/download/afg_adminboundaries_tabulardata.xlsx',
                        'format': 'xlsx',
                        'daterange_for_data': '[2021-11-17T00:00:00 TO 2022-11-17T00:00:00]',
                        'resource_type': 'api', 'url_type': 'api'
                    },
                    {
                        'name': 'COD_External/AFG_DA (MapServer)',
                        'description': 'This map service contains OCHA Common Operational Datasets for Afghanistan, in Dari: Administrative Boundaries and Regions. The service is available as ESRI Map, ESRI Feature, WMS, and KML Services. See the OCHA COD/FOD terms of use for access and use constraints.',
                        'url': 'https://codgis.itos.uga.edu/arcgis/rest/services/COD_External/AFG_DA/MapServer',
                        'format': 'geoservice',
                        'daterange_for_data': '[2021-11-17T00:00:00 TO 2022-11-17T00:00:00]',
                        'resource_type': 'api', 'url_type': 'api'
                    },
                    {
                        'name': 'COD_External/AFG_EN (MapServer)',
                        'description': 'This map service contains OCHA Common Operational Datasets for Afghanistan, in English: Administrative Boundaries and Regions. The service is available as ESRI Map, ESRI Feature, WMS, and KML Services. See the OCHA COD/FOD terms of use for access and use constraints.',
                        'url': 'https://codgis.itos.uga.edu/arcgis/rest/services/COD_External/AFG_EN/MapServer',
                        'format': 'geoservice',
                        'daterange_for_data': '[2021-11-17T00:00:00 TO 2022-11-17T00:00:00]',
                        'resource_type': 'api', 'url_type': 'api'
                    },
                    {
                        'name': 'COD_External/AFG_pcode (MapServer)',
                        'description': "This service is intended as a labelling layer for PCODES from OCHA's Common Operational Datasets for Afghanistan. As a map service it is intended to be used in conjunction with the basemap located at http://gistmaps.itos.uga.edu/arcgis/rest/services/COD_External/AFG_EN/MapServer. The service is available as ESRI Map, WMS, WFS and KML Services.",
                        'url': 'https://codgis.itos.uga.edu/arcgis/rest/services/COD_External/AFG_pcode/MapServer',
                        'format': 'geoservice',
                        'daterange_for_data': '[2021-11-17T00:00:00 TO 2022-11-17T00:00:00]',
                        'resource_type': 'api', 'url_type': 'api'
                    }
                ]

    def test_add_population_services(self, configuration, fixtures_folder):
        with temp_dir() as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, fixtures_folder, folder, False, True
                )
                cod = COD(retriever, ErrorsOnExit())
                dataset = Dataset.load_from_json(join(fixtures_folder, "dataset-cod-ps-afg.json"))
                dataset, batch = cod.add_population_services(dataset, "AFG", configuration["ps_url"])
                assert is_valid_uuid(batch) is True
                resources = dataset.get_resources()
                assert len(resources) == 6
                assert dataset.get_resources()[-2:] == [
                    {
                        'url': 'https://apps.itos.uga.edu/CODV2API/api/v1/themes/cod-ps/lookup/Get/0/do/AFG',
                        'name': 'AFG admin 0 population',
                        'format': 'json',
                        'description': 'Afghanistan administrative level 0 2021 population statistics',
                        'resource_type': 'api', 'url_type': 'api'
                    },
                    {
                        'url': 'https://apps.itos.uga.edu/CODV2API/api/v1/themes/cod-ps/lookup/Get/1/do/AFG',
                        'name': 'AFG admin 1 population',
                        'format': 'json',
                        'description': 'Afghanistan administrative level 1 2021 population statistics',
                        'resource_type': 'api', 'url_type': 'api'
                    }
                ]
