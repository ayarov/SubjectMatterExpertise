import os
import luigi
import pandas as pd
from tasks.collectors.revision import CollectRevisions


class CalculatePageFirstEditDate(luigi.Task):
    file_name = 'page_first_edit_date.h5'
    data_dir = luigi.Parameter(default=r'../../data/sme')

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
