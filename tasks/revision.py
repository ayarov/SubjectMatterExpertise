import os
import luigi
import pymongo
import pandas as pd
from tasks.page import CollectPages


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
                page_ids = pages_df['page_id']

                data = []
                columns = ['page_id', 'revision_id', 'user_id', 'user_name', 'size', 'timestamp']
                for page_id in page_ids:
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
