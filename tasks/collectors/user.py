import os
import luigi
import pymongo
import logging
import pandas as pd
from config import Configuration
from utils.bot_utils import is_bot
from .revision import CollectRevisions


config = Configuration()
logging.basicConfig(filename='sme.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(levelname)s:%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


class CollectTop10UserPages(luigi.Task):
    file_name = 'top10_user_pages.h5'
    data_dir = luigi.Parameter(default=r'../../data/sme')

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.data_dir, self.file_name), format='h5')

    def requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    @staticmethod
    def aggregate(collection, user_name, limit=10):
        agg_result = collection.aggregate([
            {
                '$match': {'user_name': user_name, 'page_ns': 0},
            },
            {
                '$group': {'_id': "$page_id", 'count': {'$sum': 1}}
            },
            {
                '$sort': {'count': -1}
            },
            {
                '$limit': limit
            }
        ], allowDiskUse=True)

        page_ids = []
        if agg_result is not None:
            for dic in agg_result:
                page_ids.append(dic['_id'])

        return page_ids

    def run(self):
        revs_df = pd.read_hdf(self.input()[0].path, mode='r')
        assert isinstance(revs_df, pd.DataFrame)
        user_names = list(revs_df['user_name'].unique())
        user_names = list(filter(lambda x: not is_bot(x), user_names))

        with pymongo.MongoClient(host=config.get('MONGO', 'host'), port=config.get_int('MONGO', 'port')) as client:
            db = client.get_database(config.get('MONGO', 'database'))
            collection = db.get_collection(config.get('MONGO', 'collection'))

            data = []
            columns = ['page_id', 'user_name']
            for user_name in user_names:
                page_ids = self.aggregate(collection, user_name)
                for page_id in page_ids:
                    data.append([page_id, user_name])

            pages_df = pd.DataFrame(data=data, columns=columns)
            pages_df.to_hdf(os.path.join(self.data_dir, self.file_name), key='df', mode='w')
