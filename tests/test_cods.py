#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for cods.

"""
from os.path import join

import hdx
import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities import is_valid_uuid

from cods import COD

alljson = [{'DatasetTitle': 'Philippines - Subnational Administrative Boundaries', 'DatasetDescription': 'Philippines administrative levels', 'FrequencyUpdates': '365', 'Resources': [{'Format':'XLSX','ResourceItemTitle':'Philippines - Subnational Administrative Boundaries','ResourceItemDescription':'Philippines - Subnational Administrative Boundaries','DownloadURL':'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/80e52e31-d06c-47ab-9bfa-cbb57dc17a1e/download/phl_adminboundaries_tabulardata.xlsx','Version':'Latest','daterange_for_data':'[2020-07-30T09:59:36 TO *]'},{'Format':'SHP','ResourceItemTitle':'Philippines - Subnational Administrative Boundaries','ResourceItemDescription':'Philippines - Subnational Administrative Boundaries','DownloadURL':'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/12457689-6a86-4474-8032-5ca9464d38a8/download/phl_adm_psa_namria_20200529_shp.zip','Version':'Latest','daterange_for_data':'[2020-07-30T09:59:36 TO *]'},{'Format':'EMF','ResourceItemTitle':'Philippines - Subnational Administrative Boundaries','ResourceItemDescription':'Philippines - Subnational Administrative Boundaries','DownloadURL':'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/82cda4bf-19f1-47bf-9469-f8bb8e5446d3/download/phl_adm_psa_namria_20200529_emf.zip','Version':'Latest','daterange_for_data':'[2020-07-30T09:59:36 TO *]'},{'Format':'KMZ','ResourceItemTitle':'Philippines - Subnational Administrative Boundaries','ResourceItemDescription':'Philippines - Subnational Administrative Boundaries','DownloadURL':'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/fab322bb-07f1-4f45-9980-795d9294386b/download/phl_adm_psa_namria_20200529_kmz.zip','Version':'Latest','daterange_for_data':'[2020-07-30T09:59:36 TO *]'},{'Format':'Geodatabase','ResourceItemTitle':'Philippines - Subnational Administrative Boundaries','ResourceItemDescription':'Philippines - Subnational Administrative Boundaries','DownloadURL':'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/c8fd86b6-eac2-4e7d-8ee3-6f0d01b991da/download/phl_adminboundaries_candidate_adm3.gdb.zip','Version':'Latest','daterange_for_data':'[2020-07-30T09:59:36 TO *]'},{'Format':'API','ResourceItemTitle':'Philippines - Subnational Administrative Boundaries','ResourceItemDescription':'Philippines - Subnational Administrative Boundaries','DownloadURL':'https://gistmaps.itos.uga.edu/arcgis/rest/services/V00_0/PHL_pcode/MapServer','Version':'1','daterange_for_data':'[2020-07-30T09:59:36 TO 2020-05-29T00:00:00]'},{'Format':'PDF','ResourceItemTitle':'Philippines - Subnational Administrative Boundaries','ResourceItemDescription':'Philippines - Subnational Administrative Boundaries','DownloadURL':'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/7c56c053-00f9-4eb7-ad61-6d07a954fc2b/download/phl-cod-ab-2020_06_05.pdf','Version':'Latest','daterange_for_data':'[2020-07-30T09:59:36 TO *]'}], 'Source': 'National Mapping and Resource Information Authority (NAMRIA), Philippines Statistics Authority (PSA)', 'Contributor': 'OCHA Philippines', 'Location':['phl'], 'Visibility': 'True', 'License': 'Other', 'License_Other': 'Humanitarian use only', 'Methodology': 'Census', 'Methodology_Other': None, 'Caveats': 'Prepared by OCHA', 'Tags': ['administrative divisions', 'common operational dataset - cod'], 'Total': 7}]


class TestCods():
    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(user_agent='test', hdx_key='12345',
                                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'phl', 'title': 'Philippines'}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'common operational dataset - cod'}, {'name': 'administrative divisions'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}
        return Configuration.read()

    @pytest.fixture(scope='class')
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()

                def fn():
                    return alljson
                response.json = fn
                return response
        return Download()

    @pytest.fixture(scope='class')
    def cod(self, downloader):
        return COD(downloader)

    def test_get_dataset_titles(self, configuration):
        cod = COD(hdx.utilities.downloader.Download(user_agent='test'))
        dataset_titles = cod.get_dataset_titles(configuration['config_url'])
        assert dataset_titles == ['Colombia - Subnational Administrative Divisions',
                                  'Ethiopia - Subnational Administrative Divisions',
                                  'Iraq - Subnational Administrative Divisions',
                                  'Kyrgyzstan - Subnational Administrative Divisions',
                                  'Liberia - Subnational Administrative Divisions',
                                  'Mongolia - Subnational Administrative Divisions',
                                  'Somalia - Subnational Administrative Divisions',
                                  'Syrian Arab Republic - Subnational Administrative Divisions',
                                  'Ukraine - Subnational Administrative Divisions',
                                  'Yemen - Subnational Administrative Divisions']

    def test_get_datasets_metadata(self, cod):
        datasets_metadata = cod.get_datasets_metadata('http://lala')
        assert datasets_metadata == alljson
        datasets_metadata = cod.get_datasets_metadata('http://lala', dataset_titles=['Philippines - Subnational Administrative Boundaries', ])
        assert datasets_metadata == alljson

    def test_generate_dataset(self, cod, configuration):
        dataset, batch = cod.generate_dataset(alljson[0])
        assert is_valid_uuid(batch) is True
        assert dataset == {'name': 'philippines-subnational-administrative-boundaries', 'title': 'Philippines - Subnational Administrative Boundaries',
                           'notes': 'Philippines administrative levels',
                           'dataset_source': 'National Mapping and Resource Information Authority (NAMRIA), Philippines Statistics Authority (PSA)',
                           'methodology': 'Census', 'methodology_other': None, 'license_id': 'hdx-other',
                           'license_other': 'Humanitarian use only',
                           'caveats': 'Prepared by OCHA',
                           'data_update_frequency': '365', 'cod_level': 'cod-enhanced', 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                           'owner_org': '27fbd3ff-d0f4-4658-8a69-a07f49a7a853', 'subnational': '1', 'groups': [{'name': 'phl'}],
                           'tags': [{'name': 'administrative divisions', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'common operational dataset - cod', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                           'dataset_date': '[2020-07-30T09:59:36 TO *]'}
        resources = dataset.get_resources()
        assert len(resources) == alljson[0]['Total']
        assert resources == [{'name': 'Philippines - Subnational Administrative Boundaries', 'description': 'Philippines - Subnational Administrative Boundaries', 'url': 'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/80e52e31-d06c-47ab-9bfa-cbb57dc17a1e/download/phl_adminboundaries_tabulardata.xlsx', 'format': 'xlsx', 'daterange_for_data': '[2020-07-30T09:59:36 TO *]', 'grouping': 'Latest', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'Philippines - Subnational Administrative Boundaries', 'description': 'Philippines - Subnational Administrative Boundaries', 'url': 'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/12457689-6a86-4474-8032-5ca9464d38a8/download/phl_adm_psa_namria_20200529_shp.zip', 'format': 'shp', 'daterange_for_data': '[2020-07-30T09:59:36 TO *]', 'grouping': 'Latest', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'Philippines - Subnational Administrative Boundaries', 'description': 'Philippines - Subnational Administrative Boundaries', 'url': 'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/82cda4bf-19f1-47bf-9469-f8bb8e5446d3/download/phl_adm_psa_namria_20200529_emf.zip', 'format': 'emf', 'daterange_for_data': '[2020-07-30T09:59:36 TO *]', 'grouping': 'Latest', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'Philippines - Subnational Administrative Boundaries', 'description': 'Philippines - Subnational Administrative Boundaries', 'url': 'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/fab322bb-07f1-4f45-9980-795d9294386b/download/phl_adm_psa_namria_20200529_kmz.zip', 'format': 'kmz', 'daterange_for_data': '[2020-07-30T09:59:36 TO *]', 'grouping': 'Latest', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'Philippines - Subnational Administrative Boundaries', 'description': 'Philippines - Subnational Administrative Boundaries', 'url': 'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/c8fd86b6-eac2-4e7d-8ee3-6f0d01b991da/download/phl_adminboundaries_candidate_adm3.gdb.zip', 'format': 'geodatabase', 'daterange_for_data': '[2020-07-30T09:59:36 TO *]', 'grouping': 'Latest', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'Philippines - Subnational Administrative Boundaries', 'description': 'Philippines - Subnational Administrative Boundaries', 'url': 'https://gistmaps.itos.uga.edu/arcgis/rest/services/V00_0/PHL_pcode/MapServer', 'format': 'api', 'daterange_for_data': '[2020-07-30T09:59:36 TO 2020-05-29T00:00:00]', 'grouping': '1', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'Philippines - Subnational Administrative Boundaries', 'description': 'Philippines - Subnational Administrative Boundaries', 'url': 'https://data.humdata.org/dataset/caf116df-f984-4deb-85ca-41b349d3f313/resource/7c56c053-00f9-4eb7-ad61-6d07a954fc2b/download/phl-cod-ab-2020_06_05.pdf', 'format': 'pdf', 'daterange_for_data': '[2020-07-30T09:59:36 TO *]', 'grouping': 'Latest', 'resource_type': 'api', 'url_type': 'api'}]