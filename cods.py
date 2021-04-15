#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
CODS:
-----

Generates urls from the COD website.

"""
import logging
from datetime import date

from hdx.data.dataset import Dataset
from hdx.data.date_helper import DateHelper
from hdx.data.hdxobject import HDXError
from hdx.data.organization import Organization
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities import get_uuid
from hdx.utilities.dateparse import parse_date
from slugify import slugify

logger = logging.getLogger(__name__)


class COD:
    def __init__(self, downloader):
        self.downloader = downloader
        self.batches_by_org = dict()

    def get_dataset_titles(self, url):
        headers, iterator = self.downloader.get_tabular_rows(url, dict_form=True)
        return [x['Dataset title'] for x in iterator]

    def get_datasets_metadata(self, url, dataset_titles=None):
        response = self.downloader.download(url)
        results = response.json()
        if dataset_titles is None:
            return results
        return [x for x in results if x['DatasetTitle'] in dataset_titles]

    def generate_dataset(self, metadata):
        title = metadata['DatasetTitle']
        is_requestdata_type = metadata['is_requestdata_type']
        if not is_requestdata_type:
            if metadata['Total'] == 0:
                logger.error(f'Ignoring dataset: {title} which has no resources!')
                return None, None
        if not metadata['Source']:
            logger.error(f'Ignoring dataset: {title} which has no source!')
            return None, None
        logger.info(f'Creating dataset: {title}')
        dataset = Dataset({
            'name': slugify(title[:99]),
            'title': title,
            'notes': metadata['DatasetDescription'],
            'dataset_source': metadata['Source'],
            'methodology': metadata['Methodology'],
            'methodology_other': metadata['Methodology_Other'],
            'license_id': metadata['License'],
            'license_other': metadata['License_Other'],
            'caveats': metadata['Caveats'],
            'data_update_frequency': metadata['FrequencyUpdates'],
            'cod_level': 'cod-enhanced'
        })
        licence = metadata['License']
        if licence == 'Other':
            dataset['license_id'] = 'hdx-other'
            dataset['license_other'] = metadata['License_Other']
        else:
            dataset['license_id'] = licence
        methodology = metadata['Methodology']
        if methodology == 'Other':
            dataset['methodology'] = 'Other'
            methodology_other = metadata['Methodology_Other']
            if not methodology_other:
                logger.error(f'Ignoring dataset: {title} which has no methodology!')
                return None, None
            dataset['methodology_other'] = methodology_other
        else:
            dataset['methodology'] = methodology
        dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
        organization = Organization.autocomplete(metadata['Contributor'])
        organization_id = organization[0]['id']
        dataset.set_organization(organization_id)
        batch = self.batches_by_org.get(organization_id, get_uuid())
        self.batches_by_org[organization_id] = batch
        dataset.set_subnational(True)
        location = metadata['Location']
        try:
            dataset.add_country_locations(location)
        except HDXError:
            logger.exception(f'Ignoring dataset: {title} which has an invalid location {location}!')
            return None, None
        dataset.add_tags(metadata['Tags'])

        if is_requestdata_type:
            dataset['dataset_date'] = metadata['DatasetDate']
            dataset['is_requestdata_type'] = True
            dataset['file_types'] = metadata['file_types']
            dataset['field_names'] = metadata['field_names']
            num_of_rows = metadata.get('num_of_rows')
            if num_of_rows:
                dataset['num_of_rows'] = num_of_rows
        else:
            startdate = None
            enddate = None
            ongoing = False
            resources = list()
            for resource_metadata in metadata['Resources']:
                resource_daterange = resource_metadata['daterange_for_data']
                resourcedata = {
                    'name': resource_metadata['ResourceItemTitle'],
                    'description': resource_metadata['ResourceItemDescription'],
                    'url': resource_metadata['DownloadURL'],
                    'format': resource_metadata['Format'],
                    'daterange_for_data': resource_daterange,
                    'grouping': resource_metadata['Version']
                }
                date_info = DateHelper.get_date_info(resource_daterange)
                resource_startdate = date_info['startdate']
                resource_enddate = date_info['enddate']
                resource_ongoing = date_info['ongoing']
                if startdate is None or resource_startdate < startdate:
                    startdate = resource_startdate
                if enddate is None or resource_enddate > enddate:
                    enddate = resource_enddate
                    ongoing = resource_ongoing
                resource = Resource(resourcedata)
                resources.append(resource)
            if ongoing:
                enddate = '*'
            dataset.add_update_resources(resources)
            dataset.set_date_of_dataset(startdate, enddate)
        return dataset, batch
