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
                'code': '0111',
                'service_id': 0
            },
            {
                'code': '0111',
                'service_id': 1
            },
            {
                'code': '0222',
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
                'code': '0111'
            },
            {
                'ID': '22',
                'code': '0222'
            },
            {
                'ID': '33',
                'code': '0333'
            },
            {
                'ID': '44',
                'code': '0444'
            },
            {
                'ID': '55',
                'code': '0555'
            },
        ])

        expected_df = pd.DataFrame([
            {
                'code': '0111',
                'service_name': 'service-A'
            },
            {
                'code': '0111',
                'service_name': 'service-B'
            },
            {
                'code': '0222',
                'service_name': 'service-A'
            },
            {
                'code': '0333',
                'service_name': 'service-C'
            },
        ])

        manager = bqemulatormanager.Manager(project='test', schema_path='examples/resources/schema_testing.yaml')
        with manager:
            manager.load(use_df, 'dataset1.user_use_list')
            manager.load(service_df, 'dataset1.service')
            manager.load(package_df, 'dataset1.package_use_list')
            manager.load(user_df, 'dataset1.users')

            with open('examples/query.sql') as f:
                sql = f.read()

            result = manager.query(sql)

        assert_frame_equal(expected_df, result)

if __name__ == '__main__':
    unittest.main()