#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir

from hdx.facades.simple import facade

from cods import get_datasets_metadata, generate_dataset

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-cods'


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    url = configuration['url']
    with Download() as downloader:
        datasets_metadata = get_datasets_metadata(url, downloader)
        logger.info('Number of datasets to upload: %d' % len(datasets_metadata))
        for info, metadata in progress_storing_tempdir('CODS', datasets_metadata, 'DatasetTitle'):
            dataset = generate_dataset(metadata)
            if dataset:
                dataset.update_from_yaml()
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: CODS', batch=info['batch'])


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
