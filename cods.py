#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
CODS:
-----

Generates urls from the COD website.

"""
import logging

from hdx.data.dataset import Dataset
from hdx.data.organization import Organization
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from slugify import slugify

logger = logging.getLogger(__name__)


def get_datasets_metadata(url, downloader):
    response = downloader.download(url)
    return response.json()


def generate_dataset(metadata):
    title = metadata['DatasetTitle']
    if metadata['Total'] == 0:
        logger.warning(f'Ignoring dataset: {title} which has no resources!')
        return None
    logger.info(f'Creating dataset: {title}')
    dataset = Dataset({
        'name': slugify(title[:99]),
        'title': title,
        'notes': f'{title}  \n{metadata["DatasetDescription"]}',
        'dataset_source': metadata['Source'],
        'methodology': 'Other',
        'methodology_other': metadata['Methodology'],
        'license_id': 'hdx-other',
        'license_other': f'Licence: {metadata["License"]}',
        'caveats': metadata['Caveats'],
        'data_update_frequency': metadata['FrequencyUpdates'],
        'cod_level': 'cod-enhanced'
    })
    dataset.set_dataset_date(metadata['DatasetDate'])
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    organization = Organization.autocomplete(metadata['Contributor'])
    dataset.set_organization(organization[0]['id'])
    dataset.set_subnational(True)
    dataset.add_country_locations(metadata['Location'])
    dataset.add_tags(metadata['Tags'])

    for resource_metadata in metadata['Resources']:
        resourcedata = {
            'name': resource_metadata['ResourceItemTitle'],
            'description': resource_metadata['ResourceItemDescription'],
            'url': resource_metadata['DownloadURL'],
            'format': resource_metadata['Format'],
            'daterange_for_data': '[2020-03-11T21:16:48.838 TO *]',
            'grouping': resource_metadata['Version']
        }
        dataset.add_update_resource(resourcedata)
    return dataset
