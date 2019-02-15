import os
import luigi
import pymongo
import pandas as pd
from wikitools import wiki, api
from utils.bot_utils import is_bot
from utils.num_utils import parse_int
from utils.str_utils import parse_string
from utils.date_utils import parse_timestamp
from tasks.page import CollectPages, CollectTalkPages


class CollectRevisions(luigi.Task):
    file_name = 'revisions.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name))

    def requires(self):
        return [CollectPages(data_dir=self.data_dir)]

    def run(self):
        pages_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(pages_df, pd.DataFrame):
            with pymongo.MongoClient(host="localhost", port=27017) as client:
                print('Client created')
                db = client.get_database(name='enwiki')
                revs_collection = db.get_collection(name='revs')
                page_ids = pages_df['page_id'].unique()

                data = []
                columns = ['page_id', 'revision_id', 'user_id', 'user_name', 'size', 'timestamp']
                for page_id in page_ids:
                    page_id = parse_int(page_id)
                    result = revs_collection.find_one({'_id': page_id})
                    if 'revs' in result and result['_id'] == page_id:
                        for rev in result['revs']:
                            revision_id = int(rev['rev_id'])
                            user_id = int(rev['user_id'])
                            user_name = str(rev['user_name'])
                            size = int(rev['size'])
                            timestamp = str(rev['timestamp'])

                            data.append([page_id, revision_id, user_id, user_name, size, timestamp])

                df = pd.DataFrame(data=data, columns=columns)
                df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')


class CollectTalkRevisions(luigi.Task):
    file_name = 'talk_revisions.h5'
    data_dir = luigi.Parameter(default=r'D:\data\sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name))

    def requires(self):
        return [CollectTalkPages(data_dir=self.data_dir)]

    def run(self):
        site = wiki.Wiki("https://en.wikipedia.org/w/api.php")
        df = None
        bot_users = self.load_bots()
        pages_df = pd.read_hdf(self.input()[0].path, mode='r')
        if isinstance(pages_df, pd.DataFrame):
            for index, row in pages_df.iterrows():
                page_id = int(row['page_id'])
                talk_page_id = int(row['talk_page_id'])

                print('Page ID: {}\tTalk Page ID: {}'.format(page_id, talk_page_id))

                talk_revs_df = self.get_revisions(site=site, page_id=page_id, talk_page_id=talk_page_id, bots=bot_users)
                if df is None:
                    df = talk_revs_df
                else:
                    df = df.append(talk_revs_df)

            df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')

    def get_revisions(self, site, page_id, talk_page_id, bots):
        data = []
        limit = 50000
        rv_continue = None
        total_revisions = 0

        while True:
            json_data = self.__get_revisions(site, talk_page_id, rv_continue)
            pages = json_data['query']['pages']
            cont_dictionary = json_data.get('continue')

            if str(talk_page_id) in pages:
                page = pages[str(talk_page_id)]
                if 'revisions' not in page:
                    continue

                revisions = page['revisions']

                for rev in revisions:
                    total_revisions += 1
                    if 'suppressed' in rev.keys() or 'userhidden' in rev.keys():
                        continue

                    user_id = parse_string(rev['userid'])
                    user_name = parse_string(rev['user'])
                    size = parse_int(rev['size'])
                    rev_id = parse_int(rev['revid'])
                    timestamp = None
                    if 'timestamp' in rev:
                        timestamp = parse_timestamp(rev['timestamp'])

                    if is_bot(user_name):
                        continue

                    data.append([page_id, talk_page_id, rev_id, user_id, user_name, size, timestamp])

            if cont_dictionary and total_revisions < limit:
                rv_continue = str(cont_dictionary['rvcontinue'])
            else:
                break

        return pd.DataFrame(data=data, columns=['page_id',
                                                'talk_page_id',
                                                'revision_id',
                                                'user_id',
                                                'user_name',
                                                'size',
                                                'timestamp'])

    def __get_revisions(self, site, page_id, rv_continue=None):
        params = \
            {
                'action': 'query',
                'pageids': page_id,
                'prop': 'revisions',
                'rvprop': 'ids|flags|timestamp|userid|user|size|comment|tags',
                'rvlimit': 500,
                'rvdir': 'newer'
            }

        if rv_continue:
            params['rvcontinue'] = rv_continue

        return api.APIRequest(site, params).query(querycontinue=False)

