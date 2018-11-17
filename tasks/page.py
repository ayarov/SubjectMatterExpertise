import os
import luigi
import pandas as pd


class CollectPages(luigi.Task):
    file_name = 'pages.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def run(self):
        df = pd.read_csv(os.path.join(self.data_dir, '1000.csv'))
        pages_df = pd.DataFrame(data=df['Article_ID'])
        pages_df.rename(index=str, columns={"Article_ID": "page_id"}, inplace=True)
        pages_df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')




