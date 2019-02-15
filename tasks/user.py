import os
import luigi
import numpy as np
import pandas as pd
from wikitools import wiki, api
from tasks.revision import CollectRevisions
from utils.num_utils import parse_int
from utils.str_utils import parse_string
from utils.date_utils import parse_timestamp


class CalculateUserEdits(luigi.Task):
    file_name = 'user_edits.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def run(self):
        revs_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(revs_df, pd.DataFrame):
            data = []
            unregistered_df = revs_df[revs_df['user_id'] == 0]
            for user_name, group in unregistered_df.groupby(by='user_name'):
                data.append([0, user_name, len(group)])

            registered_df = revs_df[revs_df['user_id'] != 0]
            for (user_id, user_name), group in registered_df.groupby(by=['user_id', 'user_name']):
                data.append([user_id, user_name, len(group)])

            df = pd.DataFrame(data=data, columns=['user_id', 'user_name', 'total_edits'])
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class CollectUnregisteredUsers(luigi.Task):
    file_name = 'unregistered_users.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')
    site = wiki.Wiki("https://en.wikipedia.org/w/api.php")

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name))

    def requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def get_contributions(self, users):
        params = \
            {
                'action': 'query',
                'format': 'json',
                'list': 'usercontribs',
                'ucprop': 'ids|timestamp',
                'uclimit': 1,
                'ucdir': 'newer',
                'ucuser': '|'.join(users)
            }

        return api.APIRequest(self.site, params).query(querycontinue=False)

    def run(self):
        revs_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(revs_df, pd.DataFrame):
            user_id = 0
            gender = 'unknown'
            min_index = 0
            max_index = batch_size = 1
            data = []
            user_names = revs_df[revs_df['user_id'] == 0]['user_name'].unique()
            while len(user_names[min_index:max_index]) > 0:
                json_data = self.get_contributions(user_names[min_index:max_index])
                contributions = json_data['query']['usercontribs']

                for contribution in contributions:
                    user_name = parse_string(contribution['user'])
                    timestamp = parse_timestamp(contribution['timestamp'])
                    data.append([user_id, user_name, gender, timestamp])

                    if len(data) % 1000 == 0:
                        print (len(data))

                min_index = max_index
                max_index = max_index + batch_size

            df = pd.DataFrame(data=data, columns=['user_id', 'user_name', 'gender', 'registration'])
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class CollectRegisteredUsers(luigi.Task):
    file_name = 'registered_users.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')
    site = wiki.Wiki("https://en.wikipedia.org/w/api.php")

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name))

    def requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def get_user(self, user_ids):
        params = \
            {
                'action': 'query',
                'list': 'users',
                'ususerids': '|'.join([str(user_id) for user_id in user_ids]),
                'usprop': 'blockinfo|groups|editcount|registration|gender'
            }

        return api.APIRequest(self.site, params).query(querycontinue=False)

    def run(self):
        revs_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(revs_df, pd.DataFrame):
            min_index = 0
            max_index = batch_size = 50
            data = []
            user_ids = revs_df[revs_df['user_id'] != 0]['user_id'].unique()
            while len(user_ids[min_index:max_index]) > 0:
                batch_user_ids = user_ids[min_index:max_index]
                user_json = self.get_user(user_ids=batch_user_ids)
                users = user_json['query']['users']
                for user in users:
                    if 'invalid' in user or 'missing' in user:
                        continue

                    user_id = parse_int(user['userid']) if 'userid' in user else None
                    user_name = parse_int(user['name']) if 'name' in user else None
                    gender = parse_string(user['gender']) if 'gender' in user else None
                    registration = parse_timestamp(user['registration']) \
                        if 'registration' in user and user['registration'] is not None else None

                    data.append([user_id, user_name, gender, registration])

                min_index = max_index
                max_index = max_index + batch_size

            df = pd.DataFrame(data=data, columns=['user_id', 'user_name', 'gender', 'registration'])
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')

