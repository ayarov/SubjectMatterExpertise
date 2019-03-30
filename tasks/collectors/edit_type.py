import os
import luigi
import pandas as pd


class CollectEditTypes(luigi.Task):
    file_name = 'revisions_with_edit_types.h5'
    data_dir = luigi.Parameter(default=r'../../data/sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name))

    def run(self):
        df = pd.read_csv(os.path.join(self.data_dir, 'revisions_with_edit_types_final.csv'))
        if isinstance(df, pd.DataFrame):
            df.drop_duplicates(inplace=True)
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class CollectEditTypes2012(luigi.Task):
    file_name = 'edit_types_2012.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name))

    def run(self):
        df = pd.read_hdf(os.path.join(self.data_dir, 'edit_types.h5'), key='df_2012', mode='r')
        if isinstance(df, pd.DataFrame):
            df.drop_duplicates(inplace=True)
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class CollectEditTypes2018(luigi.Task):
    file_name = 'edit_types_2018.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name))

    def run(self):
        df = pd.read_hdf(os.path.join(self.data_dir, 'edit_types.h5'), key='df_2018', mode='r')
        if isinstance(df, pd.DataFrame):
            df.drop_duplicates(inplace=True)
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')
