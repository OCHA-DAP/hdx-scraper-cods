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
    downloader.download(url)
    return downloader.get_json()


def generate_dataset(metadata):
    title = metadata['DatasetTitle']
    logger.info(f'Creating dataset: {title}')
    dataset = Dataset({
        'name': slugify(title),
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
    for location in metadata['Location']:
        countryiso, _ = Country.get_iso3_country_code_fuzzy(location.strip())
        dataset.add_country_location(countryiso)
    for tag in metadata['Tags']:
        dataset.add_tag(tag.strip())

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
