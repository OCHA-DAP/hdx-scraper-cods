#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for cods.

"""
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country

from cods import get_datasets_metadata, generate_dataset


alljson = [{'DatasetTitle':'Nepal municipalities - administrative layer', 'DatasetDescription':'Polygon and Point datasets for the Municipalities of Nepal.   Municipalities are not treated as part of the administrative hierarchy but are useful in some contexts.\r\n', 'FrequencyUpdates':'365', 'Resources':[{'Format':'zipped shapefile', 'ResourceItemTitle':'Nepal municipalities - administrative layer', 'ResourceItemDescription':'Nepal municipalities - administrative layer', 'DownloadURL':'https://data.humdata.org/dataset/757e66f0-692d-4837-9af8-0ecb74b15c10/resource/01748066-bc08-4df2-82ec-2d6095ddb6c5/download/npl-pplp-munhq-25k-50k-sdn-wgs84-shp.zip', 'Version':'Latest'}], 'Source':'Survey Department,  Nepal', 'Contributor':'OCHA Nepal (closed)', 'DatasetDate':'2001-01-01T00:00:00', 'Location':['npl'], 'Visibility':'True', 'License':'Other: See these [Terms of Use](http://www.humanitarianresponse.info/en/applications/data/page/terms-use). This does not replace any terms of use information provided with the dataset.', 'Methodology':'Other: This data has been prepared by merging sheet-wise individual layers available from Survey Department,  Nepal. The topographic maps used to create this dataset are of 1:25, 000 scale at Terai and Mid-Hills,  and 1:50, 000 scale at Upper Mountains and Himalayan range.', 'Caveats':'', 'Tags':['common operational dataset - cod', 'earthquakes', 'geodata', 'populated places - settlements'], 'Total':1}]


class TestCods():
    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(user_agent='test', hdx_key='12345',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'npl', 'title': 'Nepal'}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'common operational dataset - cod'}, {'name': 'earthquakes'}, {'name': 'geodata'}, {'name': 'populated places - settlements'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}

    @pytest.fixture(scope='function')
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

    def test_get_datasets_metadata(self, downloader):
        datasets_metadata = get_datasets_metadata('http://lala', downloader)
        assert datasets_metadata == alljson

    def test_generate_dataset(self, configuration):
        dataset = generate_dataset(alljson[0])
        assert dataset == {'name': 'nepal-municipalities-administrative-layer', 'title': 'Nepal municipalities - administrative layer',
                           'notes': 'Nepal municipalities - administrative layer  \nPolygon and Point datasets for the Municipalities of Nepal.   Municipalities are not treated as part of the administrative hierarchy but are useful in some contexts.\r\n',
                           'dataset_source': 'Survey Department,  Nepal', 'methodology': 'Other',
                           'methodology_other': 'Other: This data has been prepared by merging sheet-wise individual layers available from Survey Department,  Nepal. The topographic maps used to create this dataset are of 1:25, 000 scale at Terai and Mid-Hills,  and 1:50, 000 scale at Upper Mountains and Himalayan range.',
                           'license_id': 'hdx-other', 'license_other': 'Licence: Other: See these [Terms of Use](http://www.humanitarianresponse.info/en/applications/data/page/terms-use). This does not replace any terms of use information provided with the dataset.',
                           'caveats': '', 'data_update_frequency': '365', 'cod_level': 'cod-enhanced', 'dataset_date': '01/01/2001', 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                           'owner_org': 'd985bc52-7a43-4caa-9fc9-0d0ff3ba371b', 'subnational': '1', 'groups': [{'name': 'npl'}],
                           'tags': [{'name': 'common operational dataset - cod', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'earthquakes', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'geodata', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'populated places - settlements', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}