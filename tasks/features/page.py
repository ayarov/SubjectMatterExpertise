import pymongo
import logging
import numpy as np
import pandas as pd
from .base import FeatureTask
from config import Configuration
from utils.bot_utils import is_bot
from tasks.collectors.user import CollectTop10UserPages
from tasks.collectors.revision import CollectRevisions
from tasks.calculators.page import CalculatePageTotalEdits


config = Configuration()
logging.basicConfig(filename='sme.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(levelname)s:%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


class UserPageEditsFeature(FeatureTask):
    def cache_name(self):
        return 'user_page_edits'

    def on_requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        data = []
        columns = ['page_id', 'user_name', 'page_edits']
        revs_df = data_frames[0]
        if isinstance(revs_df, pd.DataFrame):
            grouped = revs_df.groupby(by=['page_id', 'user_name'])
            for (page_id, user_name), group in grouped:
                data.append([page_id, user_name, len(group)])

        return pd.DataFrame(data=data, columns=columns)


class UserPageEditsRatioFeature(FeatureTask):
    def cache_name(self):
        return 'user_page_edits_ratio'

    def on_requires(self):
        return [CalculatePageTotalEdits(data_dir=self.data_dir),
                UserPageEditsFeature(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        pte_df = data_frames[0]
        pef_df = data_frames[1]

        if isinstance(pef_df, pd.DataFrame):
            df = pd.merge(left=pef_df, right=pte_df, on='page_id', how='left')
            assert (len(df) == len(pef_df))
            df['page_edits_ratio'] = df.apply(lambda row: row['page_edits']/float(row['total_edits']), axis=1)
            return df[['page_id', 'user_name', 'page_edits_ratio']]


class NameSpacesFeature(FeatureTask):
    def cache_name(self):
        return 'name_spaces'

    def on_requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    @staticmethod
    def aggregate(collection, user_name):
        agg_result = collection.aggregate([
                     {'$match': {'user_name': user_name}},
                     {'$group': {'_id': "$page_ns",
                                 'count': {'$sum': 1}}},
                     {'$sort': {'count': -1}}
                   ])

        namespaces = np.zeros(16)

        if agg_result is not None:
            for dic in agg_result:
                ns = dic['_id']
                count = dic['count']
                if ns < len(namespaces):
                    namespaces[ns] = count

        ns_total_count = np.sum(namespaces)
        ns_edit_dist = [float(ns) / ns_total_count for ns in namespaces] if ns_total_count > 0 else list(np.zeros(16))

        return ns_edit_dist

    def on_process(self, data_frames):
        revs_df = data_frames[0]

        data = []
        ns_columns = ['ns{}_edit_dist'.format(ns) for ns in range(0, 16, 1)]
        columns = ['user_name'] + ns_columns
        if isinstance(revs_df, pd.DataFrame):
            user_names = revs_df['user_name'].unique()

            with pymongo.MongoClient(host=config.get('MONGO', 'host'),
                                     port=config.get_int('MONGO', 'port')) as client:
                db = client.get_database(config.get('MONGO', 'database'))
                collection = db.get_collection(config.get('MONGO', 'collection'))

                for user_name in user_names:
                    if is_bot(user_name):
                        continue

                    namespaces = self.aggregate(collection=collection, user_name=user_name)
                    data.append([user_name] + namespaces)
                    logging.debug('Username: {}\tNamespaces: {}'.format(user_name, namespaces))

        ns_df = pd.DataFrame(data=data, columns=columns)
        data = []
        cols = ['page_id', 'user_name'] + ns_columns
        df = revs_df.merge(ns_df, how='left', on='user_name')[cols]

        for (page_id, user_name), group in df.groupby(by=['page_id', 'user_name']):
            data.append([page_id, user_name] + list(group.iloc[0][ns_columns]))

        return pd.DataFrame(data=data, columns=cols)


class TotalEditedPagesFeature(FeatureTask):
    def cache_name(self):
        return 'total_edited_pages'

    def on_requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    @staticmethod
    def aggregate(collection, user_name):
        total_edits = None
        agg_result = collection.aggregate([
            {
                '$match': {'user_name': user_name, 'page_ns': 0}
            },
            {
                '$group': {'_id': 'null',
                           'total_edits': {'$sum': 1}}
            },
            {
                '$project': {'_id': 0}
            }])

        if agg_result is not None:
            for dic in agg_result:
                total_edits = dic['total_edits']
                break

        return total_edits

    def on_process(self, data_frames):
        host = config.get('MONGO', 'host')
        port = config.get_int('MONGO', 'port')
        database = config.get('MONGO', 'database')
        collection = config.get('MONGO', 'collection')

        revs_df = data_frames[0]

        data = []
        columns = ['user_name', 'total_edited_pages']
        if isinstance(revs_df, pd.DataFrame):
            user_names = revs_df['user_name'].unique()

            with pymongo.MongoClient(host=host, port=port) as client:
                db = client.get_database(database)
                collection = db.get_collection(collection)

                for user_name in user_names:
                    if is_bot(user_name):
                        continue

                    total_edited_pages = self.aggregate(collection=collection, user_name=user_name)

                    if total_edited_pages is None:
                        continue

                    data.append([user_name, total_edited_pages])
                    logging.debug('Username: {}\tTotal edited pages: {}'.format(user_name, total_edited_pages))

        df = pd.DataFrame(data=data, columns=columns)
        # normalization_factor = df['total_edited_pages'].max()
        # df['total_edited_pages'] = df['total_edited_pages'].apply(lambda x: float(x) / normalization_factor)

        data = []
        cols = ['page_id', 'user_name', 'total_edited_pages']
        df = revs_df.merge(df, how='left', on='user_name')[cols]
        for (page_id, user_name), group in df.groupby(by=['page_id', 'user_name']):
            data.append([page_id, user_name, group.iloc[0]['total_edited_pages']])

        return pd.DataFrame(data=data, columns=cols)


class PageCategoriesOverlap(FeatureTask):
    def cache_name(self):
        return 'page_category_overlap'

    def on_requires(self):
        return [CollectRevisions(self.data_dir),
                CollectTop10UserPages(self.data_dir)]

    @staticmethod
    def aggregate(collection, user_name):
        pass

    def on_process(self, data_frames):
        revs_df = data_frames[0]
        assert isinstance(revs_df, pd.DataFrame)
        top10_pages_df = data_frames[1]
        assert isinstance(top10_pages_df, pd.DataFrame)

        data = []
        columns = ['page_id', 'user_name', 'categories_overlap']
        with pymongo.MongoClient(host=config.get('MONGO', 'host'), port=config.get_int('MONGO', 'port')) as client:
            db = client.get_database(config.get('MONGO', 'database'))
            collection = db.get_collection('page_categories')

            grouped = revs_df.groupby(by=['page_id', 'user_name'])
            for (page_id, user_name), group in grouped:
                if is_bot(user_name):
                    continue

                cursor = collection.find(filter={'page_id': int(page_id)})
                if cursor is None:
                    continue

                categories = [entry['category'] for entry in cursor]

                top10_page_ids = list(top10_pages_df[top10_pages_df['user_name'] == user_name]['page_id'])
                top10_page_ids = list(filter(lambda x: x != page_id, top10_page_ids))

                categories_overlaps = []
                for k_page_id in top10_page_ids:
                    cursor = collection.find(filter={'page_id': int(k_page_id)})
                    if cursor is None:
                        continue

                    k_categories = [entry['category'] for entry in cursor]

                    categories_overlap = (float(len(set(categories).intersection(k_categories))) / len(set(categories).union(k_categories))) \
                        if (len(categories) > 0 or len(k_categories)) > 0 else 0.0
                    categories_overlaps.append(categories_overlap)

                mean_categories_overlap = np.mean(categories_overlaps) if len(categories_overlaps) > 0 else 0.0
                data.append([page_id, user_name, mean_categories_overlap])

        return pd.DataFrame(data=data, columns=columns)


class PageLinksOverlap(FeatureTask):
    def cache_name(self):
        return 'page_link_overlap'

    def on_requires(self):
        return [CollectRevisions(self.data_dir),
                CollectTop10UserPages(self.data_dir)]

    @staticmethod
    def aggregate(collection, user_name):
        pass

    def on_process(self, data_frames):
        revs_df = data_frames[0]
        assert isinstance(revs_df, pd.DataFrame)
        top10_pages_df = data_frames[1]
        assert isinstance(top10_pages_df, pd.DataFrame)

        data = []
        columns = ['page_id', 'user_name', 'links_overlap']
        with pymongo.MongoClient(host=config.get('MONGO', 'host'), port=config.get_int('MONGO', 'port')) as client:
            db = client.get_database(config.get('MONGO', 'database'))
            collection = db.get_collection('page_links')

            grouped = revs_df.groupby(by=['page_id', 'user_name'])
            for (page_id, user_name), group in grouped:
                if is_bot(user_name):
                    continue

                cursor = collection.find(filter={'pl_from': int(page_id)})
                if cursor is None:
                    continue

                links = [entry['pl_title'] for entry in cursor]

                top10_page_ids = list(top10_pages_df[top10_pages_df['user_name'] == user_name]['page_id'])
                top10_page_ids = list(filter(lambda x: x != page_id, top10_page_ids))

                links_overlaps = []
                for k_page_id in top10_page_ids:
                    cursor = collection.find(filter={'pl_from': int(k_page_id)})
                    if cursor is None:
                        continue

                    k_links = [entry['pl_title'] for entry in cursor]

                    links_overlap = (float(len(set(links).intersection(k_links))) / len(
                        set(links).union(k_links))) \
                        if (len(links) > 0 or len(k_links)) > 0 else 0.0
                    links_overlaps.append(links_overlap)

                mean_links_overlap = np.mean(links_overlaps) if len(links_overlaps) > 0 else 0.0
                data.append([page_id, user_name, mean_links_overlap])

        return pd.DataFrame(data=data, columns=columns)
