import os
import luigi
import pymongo
import pandas as pd
# from wikitools import wiki, api
from utils.num_utils import parse_int
from utils.str_utils import parse_string


class CollectPages(luigi.Task):
    file_name = 'pages.h5'
    data_dir = luigi.Parameter(default=r'../../data/sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def run(self):
        with pymongo.MongoClient(host="localhost", port=27017) as client:
            db = client.get_database(name='enwiki')
            revs = db.get_collection(name='revs')
            pages = db.get_collection(name='pages')
            data = []
            columns = ['page_id', 'page_title']
            for rev in revs.find({}, {'_id': 1}):
                page = pages.find_one({'_id': rev['_id']}, {'_id': 1, 'title': 1})
                if page is None:
                    page_id = parse_int(rev['_id'])
                    page_title = ''
                else:
                    page_id = parse_int(page['_id'])
                    page_title = parse_string(page['title'])
                data.append([page_id, page_title])

            pages_df = pd.DataFrame(data=data, columns=columns)
            pages_df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class CollectTalkPages(luigi.Task):
    file_name = 'talk_pages.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name))

    def requires(self):
        return [CollectPages(data_dir=self.data_dir)]

    def run(self):
        site = wiki.Wiki("https://en.wikipedia.org/w/api.php")
        pages_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(pages_df, pd.DataFrame):
            data = []
            page_ids = pages_df['page_id'].unique()
            for page_id in page_ids:
                params = {'action': 'query', 'prop': 'info', 'inprop': 'talkid', 'pageids': page_id}
                info_response = api.APIRequest(site, params).query(querycontinue=False)
                page_info = info_response['query']['pages']['{}'.format(page_id)]
                if 'missing' in page_info or 'talkid' not in page_info:
                    continue
                else:
                    talk_page_id = parse_int(page_info['talkid'])
                    data.append([page_id, talk_page_id])

            df = pd.DataFrame(data=data, columns=['page_id', 'talk_page_id'])
            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')
