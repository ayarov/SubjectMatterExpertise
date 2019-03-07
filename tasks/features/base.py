import os
import luigi
import pandas as pd


class FeatureTask(luigi.Task):
    __cache_format = 'h5'
    __cache_name = 'feature.h5'
    data_dir = luigi.Parameter(default=r'../../data/sme')

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
