import os
import pytz
import luigi
import logging
import numpy as np
import pandas as pd
import pymongo
from datetime import datetime
from config import Configuration
from functools import reduce
from utils.bot_utils import is_bot
from utils.date_utils import parse_timestamp
from tasks.revision import CollectRevisions, CollectTalkRevisions
from tasks.edit_type import CollectEditTypes

config = Configuration()
logging.basicConfig(filename='sme.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(levelname)s:%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


class CalculatePageTotalEdits(luigi.Task):
    file_name = 'page_total_edits.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def run(self):
        revs_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(revs_df, pd.DataFrame):
            grouped = revs_df.groupby(by='page_id')
            data = []
            for page_id, group in grouped:
                data.append([page_id, len(group)])
            df = pd.DataFrame(data=data, columns=['page_id', 'total_edits'])
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class CalculatePageFirstEditDate(luigi.Task):
    file_name = 'page_first_edit_date.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def run(self):
        revs_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(revs_df, pd.DataFrame):
            grouped = revs_df.groupby(by='page_id')
            data = []
            for page_id, group in grouped:
                data.append([page_id, group['timestamp'].min()])
            df = pd.DataFrame(data=data, columns=['page_id', 'first_edit_date'])
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class CalculatePageLastEditDate(luigi.Task):
    file_name = 'page_last_edit_date.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def run(self):
        revs_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(revs_df, pd.DataFrame):
            grouped = revs_df.groupby(by='page_id')
            data = []
            for page_id, group in grouped:
                data.append([page_id, group['timestamp'].max()])
            df = pd.DataFrame(data=data, columns=['page_id', 'last_edit_date'])
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class CalculateTalkPageTotalEdits(luigi.Task):
    file_name = 'talk_page_total_edits.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CollectTalkRevisions(data_dir=self.data_dir)]

    def run(self):
        revs_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(revs_df, pd.DataFrame):
            grouped = revs_df.groupby(by=['page_id', 'talk_page_id'])
            data = []
            for (page_id, talk_page_id), group in grouped:
                data.append([page_id, talk_page_id, len(group)])
            df = pd.DataFrame(data=data, columns=['page_id', 'talk_page_id', 'total_edits'])
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


# region Features
class FeatureTask(luigi.Task):
    __cache_format = 'h5'
    __cache_name = 'feature.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def cache_name(self):
        return self.__cache_name

    def cache_dir(self):
        return self.data_dir

    def cache_format(self):
        return self.__cache_format

    def output(self):
        cache_path = os.path.join(self.cache_dir(), '{}.{}'.format(self.cache_name(), self.cache_format()))
        return luigi.LocalTarget(path=cache_path, format=self.cache_format())

    def requires(self):
        return self.on_requires()

    def on_requires(self):
        return []

    def on_read_input(self):
        return [pd.read_hdf(input_target.path, mode='r') for input_target in self.input()]

    def on_process(self, data_frames):
        return data_frames[0]

    def on_save(self, df):
        cache_path = os.path.join(self.cache_dir(), '{}.{}'.format(self.cache_name(), self.cache_format()))
        df.to_hdf(path_or_buf=cache_path, key='df', mode='w')

    def run(self):
        self.on_save(self.on_process(self.on_read_input()))


class UserPageEditsFeature(FeatureTask):
    def cache_name(self):
        return 'user_page_edits'

    def on_requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        data = []
        columns = ['page_id', 'user_name', 'page_edits']
        revs_df = data_frames[0]
        if isinstance(revs_df, pd.DataFrame):
            grouped = revs_df.groupby(by=['page_id', 'user_name'])
            for (page_id, user_name), group in grouped:
                data.append([page_id, user_name, len(group)])

        return pd.DataFrame(data=data, columns=columns)


class UserPageEditsRatioFeature(FeatureTask):
    def cache_name(self):
        return 'user_page_edits_ratio'

    def on_requires(self):
        return [CalculatePageTotalEdits(data_dir=self.data_dir),
                UserPageEditsFeature(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        pte_df = data_frames[0]
        pef_df = data_frames[1]

        if isinstance(pef_df, pd.DataFrame):
            df = pd.merge(left=pef_df, right=pte_df, on='page_id', how='left')
            assert (len(df) == len(pef_df))
            df['page_edits_ratio'] = df.apply(lambda row: row['page_edits']/float(row['total_edits']), axis=1)
            return df[['page_id', 'user_name', 'page_edits_ratio']]


class UserTalkPageEditsFeature(FeatureTask):
    def cache_name(self):
        return 'user_talk_page_edits'

    def on_requires(self):
        return [CollectTalkRevisions(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        data = []
        columns = ['page_id', 'user_name', 'talk_page_edits']
        revs_df = data_frames[0]
        if isinstance(revs_df, pd.DataFrame):
            # for page_id in revs_df['page_id'].unique():
            grouped = revs_df.groupby(by=['page_id', 'user_name'])
            for (page_id, user_name), group in grouped:
                data.append([page_id, user_name, len(group)])

        return pd.DataFrame(data=data, columns=columns)


class UserTalkPageEditsRatioFeature(FeatureTask):
    def cache_name(self):
        return 'user_talk_page_edits_ratio'

    def on_requires(self):
        return [CalculateTalkPageTotalEdits(data_dir=self.data_dir),
                UserTalkPageEditsFeature(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        pte_df = data_frames[0]
        pef_df = data_frames[1]

        if isinstance(pef_df, pd.DataFrame):
            df = pd.merge(left=pef_df, right=pte_df, on=['page_id'], how='left')
            assert(len(df) == len(pef_df))
            df['talk_page_edits_ratio'] = df.apply(lambda row: row['talk_page_edits']/float(row['total_edits']), axis=1)
            return df[['page_id', 'user_name', 'talk_page_edits_ratio']]


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
        df = revs_df.merge(tenure_df, how='left', on='user_name')
        return df[['page_id', 'user_name', 'tenure']]


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






# endregion


class MergeFeatures(FeatureTask):
    def cache_name(self):
        return 'features'

    def on_requires(self):
        return [
                # CollectTalkPages(data_dir=self.data_dir),
                # CollectTalkRevisions(data_dir=self.data_dir),
                # CollectEditTypes2012(data_dir=self.data_dir),
                # CollectEditTypes2018(data_dir=self.data_dir),

                UserPageEditsFeature(data_dir=self.data_dir),
                UserPageEditsRatioFeature(data_dir=self.data_dir),
                UserTalkPageEditsFeature(data_dir=self.data_dir),
                UserTalkPageEditsRatioFeature(data_dir=self.data_dir),
                EditPeriodsFeature(data_dir=self.data_dir),
                EditFrequencyFeature(data_dir=self.data_dir),
                EditSizeFeature(data_dir=self.data_dir),
                EditTypesFeature(data_dir=self.data_dir),
                TenureFeature(data_dir=self.data_dir)
                ]

    def on_process(self, data_frames):
        logging.info('Merging features...')
        df = reduce(lambda x, y: pd.merge(x, y, on=['page_id', 'user_name'], how='left'), data_frames)
        return df

    # def requires(self):
    #     return [CollectEditTypes2012(data_dir=self.data_dir),
    #             CollectEditTypes2018(data_dir=self.data_dir),
    #             # Features
    #             UserPageEditsFeature(data_dir=self.data_dir),
    #             UserPageEditsRatioFeature(data_dir=self.data_dir),
    #             UserTalkPageEditsFeature(data_dir=self.data_dir),
    #             UserTalkPageEditsRatioFeature(data_dir=self.data_dir),
    #             EditPeriodsFeature(data_dir=self.data_dir),
    #             EditFrequencyFeature(data_dir=self.data_dir),
    #             EditSizeFeature(data_dir=self.data_dir),
    #             CalculateUserEdits(data_dir=self.data_dir)]
    #             # TenureFeature(data_dir=self.data_dir)]

    # def run(self):
    #     logging.info('Merging features...')
