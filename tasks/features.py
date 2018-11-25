import os
import luigi
import logging
import numpy as np
import pandas as pd
from utils.date_utils import parse_timestamp
from tasks.revision import CollectRevisions, CollectTalkRevisions
from tasks.edit_type import CollectEditTypes2012, CollectEditTypes2018


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
    format = 'h5'
    file_name = 'feature.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def get_file_name(self):
        return self.file_name

    def get_data_dir(self):
        return self.data_dir

    def get_format(self):
        return self.format

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.get_data_dir(), self.get_file_name()), format=self.get_format())

    def requires(self):
        return self.on_requires()

    def on_requires(self):
        return []

    def on_read_input(self):
        return [pd.read_hdf(input_target.path, mode='r') for input_target in self.input()]

    def on_process(self, data_frames):
        return data_frames[0]

    def on_save(self, df):
        df.to_hdf(os.path.join(self.get_data_dir(), self.get_file_name()), key='df', mode='w')

    def run(self):
        self.on_save(self.on_process(self.on_read_input()))


class UserPageEditsFeature(luigi.Task):
    file_name = 'user_page_edits.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def run(self):
        revs_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(revs_df, pd.DataFrame):
            data = []
            page_ids = revs_df['page_id'].unique()
            for page_id in page_ids:
                # process unregistered users
                df = revs_df[(revs_df['page_id'] == page_id) & (revs_df['user_id'] == 0)]
                grouped = df.groupby(by='user_name')
                for user_name, group in grouped:
                    data.append([0, user_name, page_id, len(group)])
                # process registered users
                df = revs_df[(revs_df['page_id'] == page_id) & (revs_df['user_id'] != 0)]
                grouped = df.groupby(by='user_id')
                for user_id, group in grouped:
                    data.append([user_id, group['user_name'].iloc[0], page_id, len(group)])

            df = pd.DataFrame(data=data, columns=['user_id', 'user_name', 'page_id', 'page_edits'])
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class UserPageEditsRatioFeature(luigi.Task):
    file_name = 'user_page_edits_ratio.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CalculatePageTotalEdits(data_dir=self.data_dir), UserPageEditsFeature(data_dir=self.data_dir)]

    def run(self):
        pte_df = pd.read_hdf(self.input()[0].path, mode='r')
        pef_df = pd.read_hdf(self.input()[1].path, mode='r')

        if isinstance(pef_df, pd.DataFrame):
            df = pd.merge(left=pef_df, right=pte_df, on='page_id', how='left')
            df['page_edits_ratio'] = df.apply(lambda row: row['page_edits']/float(row['total_edits']), axis=1)
            df = df[['user_id', 'user_name', 'page_id', 'page_edits_ratio']]
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class UserTalkPageEditsFeature(luigi.Task):
    file_name = 'user_talk_page_edits.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CollectTalkRevisions(data_dir=self.data_dir)]

    def run(self):
        revs_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(revs_df, pd.DataFrame):
            data = []
            page_ids = revs_df['page_id'].unique()
            for page_id in page_ids:
                talk_page_id = revs_df[revs_df['page_id'] == page_id]['talk_page_id'].iloc[0]

                # process unregistered users
                df = revs_df[(revs_df['page_id'] == page_id) & (revs_df['user_id'] == 0)]
                grouped = df.groupby(by='user_name')
                for user_name, group in grouped:
                    data.append([0, user_name, page_id, talk_page_id, len(group)])
                # process registered users
                df = revs_df[(revs_df['page_id'] == page_id) & (revs_df['user_id'] != 0)]
                grouped = df.groupby(by='user_id')
                for user_id, group in grouped:
                    data.append([user_id, group['user_name'].iloc[0], page_id, talk_page_id, len(group)])

            df = pd.DataFrame(data=data, columns=['user_id', 'user_name', 'page_id', 'talk_page_id', 'talk_page_edits'])
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class UserTalkPageEditsRatioFeature(luigi.Task):
    file_name = 'user_talk_page_edits_ratio.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CalculateTalkPageTotalEdits(data_dir=self.data_dir), UserTalkPageEditsFeature(data_dir=self.data_dir)]

    def run(self):
        pte_df = pd.read_hdf(self.input()[0].path, mode='r')
        pef_df = pd.read_hdf(self.input()[1].path, mode='r')

        if isinstance(pef_df, pd.DataFrame):
            df = pd.merge(left=pef_df, right=pte_df, on=['page_id', 'talk_page_id'], how='left')
            df['talk_page_edits_ratio'] = df.apply(lambda row: row['talk_page_edits']/float(row['total_edits']), axis=1)
            df = df[['user_id', 'user_name', 'page_id', 'talk_page_edits_ratio']]
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class EditPeriodsFeature(luigi.Task):
    file_name = 'edit_periods.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CalculatePageFirstEditDate(data_dir=self.data_dir),
                CalculatePageLastEditDate(data_dir=self.data_dir),
                CollectRevisions(data_dir=self.data_dir)]

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
    def init_user_data(page_id, user_name, user_id=0):
        return {'page_id': page_id,
                'user_id': user_id,
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
                user_data['user_id'],
                user_data['user_name'],
                user_data['edit_period_q1']/float(total_edits),
                user_data['edit_period_q2']/float(total_edits),
                user_data['edit_period_q3']/float(total_edits),
                user_data['edit_period_q4']/float(total_edits)]

    def run(self):
        fe_df = pd.read_hdf(self.input()[0].path, mode='r')
        le_df = pd.read_hdf(self.input()[1].path, mode='r')
        revs_df = pd.read_hdf(self.input()[2].path, mode='r')

        if isinstance(revs_df, pd.DataFrame) and isinstance(fe_df, pd.DataFrame) and isinstance(le_df, pd.DataFrame):
            data = []
            columns = ['page_id',
                       'user_id',
                       'user_name',
                       'edit_period_q1',
                       'edit_period_q2',
                       'edit_period_q3',
                       'edit_period_q4']
            for page_id, page_df in revs_df.groupby(by='page_id'):
                first_edit_date = parse_timestamp(fe_df[fe_df['page_id'] == page_id].iloc[0]['first_edit_date'])
                last_edit_date = parse_timestamp(le_df[le_df['page_id'] == page_id].iloc[0]['last_edit_date'])

                edit_delta = last_edit_date.toordinal() - first_edit_date.toordinal()
                ordinal_period_1 = first_edit_date.toordinal() + (edit_delta / 4)
                ordinal_period_2 = first_edit_date.toordinal() + (edit_delta / 2)
                ordinal_period_3 = last_edit_date.toordinal() - (edit_delta / 4)

                if isinstance(page_df, pd.DataFrame):
                    for user_name, user_df in page_df.groupby(by='user_name'):
                        user_id = user_df['user_id'].iloc[0]
                        if user_id == 0:
                            user_data = self.init_user_data(page_id=page_id, user_name=user_name)
                        else:
                            user_data = self.init_user_data(page_id=page_id, user_id=user_id, user_name=user_name)
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

            df = pd.DataFrame(data=data, columns=columns)
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class EditFrequencyFeature(luigi.Task):
    file_name = 'edit_frequency.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

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

    def run(self):
        revs_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(revs_df, pd.DataFrame):
            norm_factor = (parse_timestamp('2050-01-01') - parse_timestamp('2001-01-01')).total_seconds()
            data = []
            columns = ['user_id', 'user_name', 'page_id', 'mean_edit_frequency', 'median_edit_frequency']
            registered = revs_df[revs_df['user_id'] != 0]
            for (page_id, user_id, user_name), group in registered.groupby(by=['page_id', 'user_id', 'user_name']):
                logging.debug('Page ID: {}\tUser ID: {}\tUser Name: {}'.format(page_id, user_id, user_name))
                mean_edit_interval, median_edit_interval = self.calculate_edit_frequency(group, norm_factor)
                data.append([page_id, user_id, user_name, mean_edit_interval, median_edit_interval])

            unregistered = revs_df[revs_df['user_id'] == 0]
            for (page_id, user_name), group in unregistered.groupby(by=['page_id', 'user_name']):
                logging.debug('Page ID: {}\tUser Name: {}'.format(page_id, user_name))
                mean_edit_interval, median_edit_interval = self.calculate_edit_frequency(group, norm_factor)
                data.append([page_id, 0, user_name, mean_edit_interval, median_edit_interval])

            df = pd.DataFrame(data=data, columns=columns)
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class EditSizeFeature(FeatureTask):

    def get_file_name(self):
        return 'edit_size.h5'

    def on_requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        revs_df = data_frames[0]
        if isinstance(revs_df, pd.DataFrame):
            data = []
            columns = ['user_id', 'user_name', 'page_id', 'mean_edit_size', 'median_edit_size']
            registered = revs_df[revs_df['user_id'] != 0]
            for (page_id, user_id, user_name), group in registered.groupby(by=['page_id', 'user_id', 'user_name']):
                logging.debug('Page ID: {}\tUser ID: {}\tUser Name: {}'.format(page_id, user_id, user_name))
                data.append([page_id, user_id, user_name, np.mean(group['size']), np.median(group['size'])])

            unregistered = revs_df[revs_df['user_id'] == 0]
            for (page_id, user_name), group in unregistered.groupby(by=['page_id', 'user_name']):
                logging.debug('Page ID: {}\tUser Name: {}'.format(page_id, user_name))
                data.append([page_id, 0, user_name, np.mean(group['size']), np.median(group['size'])])

            return pd.DataFrame(data=data, columns=columns)
# endregion


class MergeFeatures(luigi.Task):
    file_name = 'features.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name))

    def requires(self):
        return [CollectEditTypes2012(data_dir=self.data_dir),
                CollectEditTypes2018(data_dir=self.data_dir),
                # Features
                UserPageEditsFeature(data_dir=self.data_dir),
                UserPageEditsRatioFeature(data_dir=self.data_dir),
                UserTalkPageEditsFeature(data_dir=self.data_dir),
                UserTalkPageEditsRatioFeature(data_dir=self.data_dir),
                EditPeriodsFeature(data_dir=self.data_dir),
                EditFrequencyFeature(data_dir=self.data_dir),
                EditSizeFeature(data_dir=self.data_dir)]

    def run(self):
        logging.info('Merging features...')
