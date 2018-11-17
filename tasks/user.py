import os
import luigi
import pandas as pd
from tasks.revision import CollectRevisions


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
            revs_df['is_anonymous'] = revs_df.apply(lambda row: True if row.user_id == 0 else False, axis=1)
            revs_df.drop(columns=['page_id', 'revision_id'], inplace=True)
            revs_df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')

