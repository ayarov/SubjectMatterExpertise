import os
import luigi
import pandas as pd
from tasks.revision import CollectTalkRevisions


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
