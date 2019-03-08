import pymongo
import logging
import numpy as np
import pandas as pd
from scipy.stats import entropy
from config import Configuration
from utils.bot_utils import is_bot
from tasks.collectors.edit_type import CollectEditTypes
from utils.date_utils import parse_timestamp
from tasks.collectors.revision import CollectRevisions
from tasks.features.base import FeatureTask
from tasks.calculators.page import CalculatePageFirstEditDate, CalculatePageLastEditDate


config = Configuration()
logging.basicConfig(filename='sme.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(levelname)s:%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


class EditPeriodsFeature(FeatureTask):
    @staticmethod
    def calculate_quarter(row):
        first_edit_date = parse_timestamp(row['first_edit_date'])
        last_edit_date = parse_timestamp(row['last_edit_date'])
        timestamp = parse_timestamp(row['timestamp'])

        edit_delta = last_edit_date.toordinal() - first_edit_date.toordinal()
        ordinal_period_1 = first_edit_date.toordinal() + (edit_delta / 4)
        ordinal_period_2 = first_edit_date.toordinal() + (edit_delta / 2)
        ordinal_period_3 = last_edit_date.toordinal() - (edit_delta / 4)
        ordinal_timestamp = timestamp.toordinal()

        if ordinal_timestamp < ordinal_period_1:
            return 'q1'
        elif ordinal_timestamp < ordinal_period_2:
            return 'q2'
        elif ordinal_timestamp < ordinal_period_3:
            return 'q3'
        else:
            return 'q4'

    @staticmethod
    def init_user_data(page_id, user_name):
        return {'page_id': page_id,
                'user_name': user_name,
                'edit_period_q1': 0,
                'edit_period_q2': 0,
                'edit_period_q3': 0,
                'edit_period_q4': 0}

    @staticmethod
    def get_user_row(user_data):
        total_edits = user_data['edit_period_q1'] + \
                      user_data['edit_period_q2'] + \
                      user_data['edit_period_q3'] + \
                      user_data['edit_period_q4']
        return [user_data['page_id'],
                user_data['user_name'],
                user_data['edit_period_q1']/float(total_edits),
                user_data['edit_period_q2']/float(total_edits),
                user_data['edit_period_q3']/float(total_edits),
                user_data['edit_period_q4']/float(total_edits)]

    def cache_name(self):
        return 'edit_periods'

    def on_requires(self):
        return [CalculatePageFirstEditDate(data_dir=self.data_dir),
                CalculatePageLastEditDate(data_dir=self.data_dir),
                CollectRevisions(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        data = []
        columns = ['page_id',
                   'user_name',
                   'edit_period_q1',
                   'edit_period_q2',
                   'edit_period_q3',
                   'edit_period_q4']
        fe_df = data_frames[0]
        le_df = data_frames[1]
        revs_df = data_frames[2]

        if isinstance(revs_df, pd.DataFrame) and isinstance(fe_df, pd.DataFrame) and isinstance(le_df, pd.DataFrame):
            for page_id, page_df in revs_df.groupby(by='page_id'):
                first_edit_date = parse_timestamp(fe_df[fe_df['page_id'] == page_id].iloc[0]['first_edit_date'])
                last_edit_date = parse_timestamp(le_df[le_df['page_id'] == page_id].iloc[0]['last_edit_date'])

                edit_delta = last_edit_date.toordinal() - first_edit_date.toordinal()
                ordinal_period_1 = first_edit_date.toordinal() + (edit_delta / 4)
                ordinal_period_2 = first_edit_date.toordinal() + (edit_delta / 2)
                ordinal_period_3 = last_edit_date.toordinal() - (edit_delta / 4)

                if isinstance(page_df, pd.DataFrame):
                    for user_name, user_df in page_df.groupby(by='user_name'):
                        user_data = self.init_user_data(page_id=page_id, user_name=user_name)
                        if isinstance(user_df, pd.DataFrame):
                            for index, row in user_df.iterrows():
                                timestamp = parse_timestamp(row['timestamp'])
                                ordinal_timestamp = timestamp.toordinal()

                                if ordinal_timestamp < ordinal_period_1:
                                    user_data['edit_period_q1'] = user_data['edit_period_q1'] + 1
                                elif ordinal_timestamp < ordinal_period_2:
                                    user_data['edit_period_q2'] = user_data['edit_period_q2'] + 1
                                elif ordinal_timestamp < ordinal_period_3:
                                    user_data['edit_period_q3'] = user_data['edit_period_q3'] + 1
                                else:
                                    user_data['edit_period_q4'] = user_data['edit_period_q4'] + 1

                            data.append(self.get_user_row(user_data))

        return pd.DataFrame(data=data, columns=columns)


class EditFrequencyFeature(FeatureTask):
    @staticmethod
    def calculate_edit_frequency(group, norm_factor):
        if isinstance(group, pd.DataFrame):
            timestamps = [parse_timestamp(timestamp) for timestamp in group['timestamp']]
            intervals = []
            index = 0
            for timestamp in timestamps:
                interval = timestamp - timestamp if (index - 1 < 0) else timestamp - timestamps[index - 1]
                norm_interval = (float(interval.total_seconds()) / norm_factor) if norm_factor > 0 else 0
                intervals.append(norm_interval)
                index += 1

            return np.mean(intervals), np.median(intervals)

    def cache_name(self):
        return 'edit_frequency'

    def on_requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        data = []
        columns = ['page_id',
                   'user_name',
                   'mean_edit_frequency',
                   'median_edit_frequency']
        revs_df = data_frames[0]
        if isinstance(revs_df, pd.DataFrame):
            norm_factor = (parse_timestamp('2050-01-01') - parse_timestamp('2001-01-01')).total_seconds()

            for (page_id, user_name), group in revs_df.groupby(by=['page_id', 'user_name']):
                logging.debug('Page ID: {}\tUser Name: {}'.format(page_id, user_name))
                mean_edit_interval, median_edit_interval = self.calculate_edit_frequency(group, norm_factor)
                data.append([page_id, user_name, mean_edit_interval, median_edit_interval])

        return pd.DataFrame(data=data, columns=columns)


class EditSizeFeature(FeatureTask):
    def cache_name(self):
        return 'edit_size'

    def on_requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        normalization_scale = 1000000
        data = []
        columns = ['page_id',
                   'user_name',
                   'mean_edit_size',
                   'median_edit_size']
        revs_df = data_frames[0]
        if isinstance(revs_df, pd.DataFrame):
            for (page_id, user_name), group in revs_df.groupby(by=['page_id', 'user_name']):
                logging.debug('Page ID: {}\tUser Name: {}'.format(page_id, user_name))
                data.append([page_id,
                             user_name,
                             np.mean(group['size'])/normalization_scale,
                             np.median(group['size'])/normalization_scale])

        return pd.DataFrame(data=data, columns=columns)


class EditTypesFeature(FeatureTask):
    def cache_name(self):
        return 'edit_types'

    def on_requires(self):
        return [CollectEditTypes(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        edit_type_columns = ['edit_type_a',
                             'edit_type_b',
                             'edit_type_c',
                             'edit_type_d',
                             'edit_type_e',
                             'edit_type_f',
                             'edit_type_g',
                             'edit_type_h',
                             'edit_type_i',
                             'edit_type_j',
                             'edit_type_k',
                             'edit_type_l',
                             'edit_type_m']

        df = data_frames[0]

        counter = 0
        data = []
        for (page_id, user_name), group in df.groupby(by=['page_id', 'user_name']):
            row = [page_id, user_name] + [np.sum(group[et_col]) / len(group) for et_col in edit_type_columns]
            data.append(row)

            if counter % 50000 == 0 and counter > 0:
                print(counter)
            counter += 1

        return pd.DataFrame(data=data, columns=['page_id', 'user_name'] + edit_type_columns)


class PageEditsEntropyFeature(FeatureTask):
    def cache_name(self):
        return 'page_edit_entropy'

    def on_requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    @staticmethod
    def aggregate(collection, user_name):
        agg_result = collection.aggregate([
            {
                '$match': {'user_name': user_name, 'page_ns': 0}},
            {
                '$group': {'_id': "$page_id", 'count': {'$sum': 1}}
            }
        ], allowDiskUse=True)

        if agg_result is not None:
            counts = []
            for dic in agg_result:
                counts.append(dic['count'])
            return entropy(counts)
        else:
            return None

    def on_process(self, data_frames):
        host = config.get('MONGO', 'host')
        port = config.get_int('MONGO', 'port')
        database = config.get('MONGO', 'database')
        collection = config.get('MONGO', 'collection')

        revs_df = data_frames[0]

        data = []
        columns = ['user_name', 'page_edit_dist']
        if isinstance(revs_df, pd.DataFrame):
            user_names = revs_df['user_name'].unique()

            with pymongo.MongoClient(host=host, port=port) as client:
                db = client.get_database(database)
                collection = db.get_collection(collection)

                for user_name in user_names:
                    if is_bot(user_name):
                        continue

                    page_edit_dist = self.aggregate(collection=collection, user_name=user_name)

                    if page_edit_dist is None:
                        continue

                    data.append([user_name, page_edit_dist])
                    logging.debug('Username: {}\tTotal edited pages: {}'.format(user_name, page_edit_dist))

        df = pd.DataFrame(data=data, columns=columns)

        data = []
        cols = ['page_id', 'user_name', 'page_edit_dist']
        df = revs_df.merge(df, how='left', on='user_name')[cols]
        for (page_id, user_name), group in df.groupby(by=['page_id', 'user_name']):
            data.append([page_id, user_name, group.iloc[0]['page_edit_dist']])

        return pd.DataFrame(data=data, columns=cols)
