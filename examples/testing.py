import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

import bqemulatormanager


class TestBigquery(unittest.TestCase):

    def test_query(self):
        # test data
        service_df = pd.DataFrame([
            {
                'id': 0,
                'name': 'service-A'
            },
            {
                'id': 1,
                'name': 'service-B',
            },
            {
                'id': 2,
                'name': 'service-C',
            },
        ])

        use_df = pd.DataFrame([
            {
                'id': '11',
                'service_id': 0
            },
            {
                'id': '11',
                'service_id': 1
            },
            {
                'id': '22',
                'service_id': 0
            },
        ])

        package_df = pd.DataFrame([
            {
                'SERVICE_ID': 0,
                'ID': 11,
            },
            {
                'SERVICE_ID': 2,
                'ID': 33,
            },
        ])

        user_df = pd.DataFrame([
            {
                'ID': '11',
                'system_cd': '0111'
            },
            {
                'ID': '22',
                'system_cd': '0222'
            },
            {
                'ID': '33',
                'system_cd': '0333'
            },
            {
                'ID': '44',
                'system_cd': '0444'
            },
            {
                'ID': '55',
                'system_cd': '0555'
            },
        ])

        expected_df = pd.DataFrame([
            {
                'id': '11',
                'service_name': 'service-A'
            },
            {
                'id': '11',
                'service_name': 'service-B'
            },
            {
                'id': '22',
                'service_name': 'service-A'
            },
            {
                'id': '33',
                'service_name': 'service-C'
            },
        ])

        manager = bqemulatormanager.Manager(project='test', schema_path='resources/schema_testing.yaml')
        with manager:
            manager.load(use_df, 'dataset1.user_use_list')
            manager.load(service_df, 'dataset1.service')
            manager.load(package_df, 'dataset1.package_use_list')
            manager.load(user_df, 'dataset1.users')

            with open('./query.sql') as f:
                sql = f.read()

            result = manager.query(sql)

        assert_frame_equal(expected_df, result)
