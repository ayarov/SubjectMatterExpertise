import pytz
import pymongo
import logging
import mwclient
import pandas as pd
from datetime import datetime
from config import Configuration
from utils.bot_utils import is_bot
from tasks.collections.revision import CollectRevisions
from tasks.features.base import FeatureTask


config = Configuration()
site = mwclient.Site('en.wikipedia.org')
logging.basicConfig(filename='sme.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(levelname)s:%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


class TenureFeature(FeatureTask):
    def cache_name(self):
        return 'tenure'

    def on_requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    @staticmethod
    def aggregate(collection, user_name):
        first_edit = None
        last_edit = None
        agg_result = collection.aggregate([
            {
                '$match': {'user_name': user_name}
            },
            {
                '$group': {'_id': "timestamp",
                           'first_edit': {'$min': "$timestamp"},
                           'last_edit': {'$max': "$timestamp"}}
            },
            {
                '$project': {'_id': 0}
            }])

        if agg_result is not None:
            for dic in agg_result:
                first_edit = dic['first_edit']
                last_edit = dic['last_edit']
                break

        return first_edit, last_edit

    def on_process(self, data_frames):
        wiki_launch_date = pytz.utc.localize(datetime(year=2001, month=1, day=15))
        current_date = pytz.utc.localize(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0))
        normalization_factor = (current_date - wiki_launch_date).total_seconds()

        revs_df = data_frames[0]

        data = []
        columns = ['user_name', 'tenure']
        if isinstance(revs_df, pd.DataFrame):
            user_names = revs_df['user_name'].unique()

            with pymongo.MongoClient(host=config.get('MONGO', 'host'),
                                     port=config.get_int('MONGO', 'port')) as client:
                db = client.get_database(config.get('MONGO', 'database'))
                collection = db.get_collection(config.get('MONGO', 'collection'))

                for user_name in user_names:
                    if is_bot(user_name):
                        continue

                    first_edit, last_edit = self.aggregate(collection=collection, user_name=user_name)

                    if first_edit is None or last_edit is None:
                        continue

                    tenure = (last_edit - first_edit).total_seconds() / normalization_factor
                    data.append([user_name, tenure])
                    logging.debug('Username: {}\tTenure: {}'.format(user_name, tenure))

        tenure_df = pd.DataFrame(data=data, columns=columns)
        data = []
        cols = ['page_id', 'user_name', 'tenure']
        df = revs_df.merge(tenure_df, how='left', on='user_name')[cols]
        for (page_id, user_name), group in df.groupby(by=['page_id', 'user_name']):
            data.append([page_id, user_name, group.iloc[0]['tenure']])

        return pd.DataFrame(data=data, columns=cols)


class GenderFeature(FeatureTask):
    def cache_name(self):
        return 'gender'

    def on_requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        revs_df = data_frames[0]

        data = []
        columns = ['user_name', 'gender']
        if isinstance(revs_df, pd.DataFrame):
            user_names = list(revs_df['user_name'].unique())

            user_names = list(filter(lambda x: not is_bot(x), user_names))

            # non_bot_users = []
            # for user_name in user_names:
            #     if is_bot(user_name):
            #         continue
            #     else:
            #         non_bot_users.append(user_name)

            for user in user_names:
                resp_users = site.users(users=[str(user)], prop=['gender'])
                for resp_user in resp_users:
                    if 'missing' in resp_user or 'gender' not in resp_user:
                        gender = -1.
                        data.append([user, gender])
                    else:
                        gender = 1. if resp_user['gender'] == 'male' else (-1. if resp_user['gender'] is None else 0.)
                        data.append([user, gender])
                    logging.debug('Username: {}\tGender: {}'.format(user, gender))
                    break

        tenure_df = pd.DataFrame(data=data, columns=columns)
        data = []
        cols = ['page_id', 'user_name', 'gender']
        df = revs_df.merge(tenure_df, how='left', on='user_name')[cols]
        for (page_id, user_name), group in df.groupby(by=['page_id', 'user_name']):
            data.append([page_id, user_name, group.iloc[0]['gender']])

        return pd.DataFrame(data=data, columns=cols)
