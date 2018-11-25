import os
import luigi
import pandas as pd
from wikitools import wiki, api
from utils.str_utils import normalize_string


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


class CollectTalkPages(luigi.Task):
    file_name = 'talk_pages.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')
    site = wiki.Wiki("https://en.wikipedia.org/w/api.php")

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name))

    def requires(self):
        return [CollectPages(data_dir=self.data_dir)]

    def run(self):
        pages_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(pages_df, pd.DataFrame):
            data = []
            page_ids = pages_df['page_id'].unique()
            for page_id in page_ids:
                params = {'action': 'query', 'prop': 'info', 'inprop': 'talkid', 'pageids': page_id}
                info_response = api.APIRequest(self.site, params).query(querycontinue=False)
                page_info = info_response['query']['pages']['{}'.format(page_id)]
                if 'missing' in page_info or 'talkid' not in page_info:
                    continue
                else:
                    talk_page_id = int(normalize_string(page_info['talkid']))
                    data.append([page_id, talk_page_id])

            df = pd.DataFrame(data=data, columns=['page_id', 'talk_page_id'])
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')

