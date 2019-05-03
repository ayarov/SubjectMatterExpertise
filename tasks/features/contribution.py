import pymongo
import logging
import numpy as np
import pandas as pd
from config import Configuration
from tasks.features.base import FeatureTask
from tasks.collectors.revision import CollectRevisions
from tasks.collectors.user import CollectTop10UserPages
from tasks.features.persistence import RevisionPersistence, PersistenceFeature
from utils.nlp_utils import EsaProvider
from utils.bot_utils import is_bot
from utils.nlp_utils import stop_words


config = Configuration()
logging.basicConfig(filename='sme.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(levelname)s:%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


class SubjectSimilarityFeature(FeatureTask):
    def cache_name(self):
        return 'subject_similarity'

    def on_requires(self):
        return [CollectRevisions(self.data_dir),
                CollectTop10UserPages(self.data_dir)]

    def on_process(self, data_frames):
        data = []
        columns = ['page_id', 'user_name', 'title_similarity', 'summary_similarity']

        esa = EsaProvider(self.data_dir)

        revs_df = data_frames[0]
        assert isinstance(revs_df, pd.DataFrame)
        top10_pages_df = data_frames[1]
        assert isinstance(top10_pages_df, pd.DataFrame)

        with pymongo.MongoClient(host=config.get('MONGO', 'host'), port=config.get_int('MONGO', 'port')) as client:
            db = client.get_database(config.get('MONGO', 'database'))
            collection = db.get_collection('pages')

            grouped = revs_df.groupby(by=['page_id', 'user_name'])
            for (page_id, user_name), group in grouped:
                if is_bot(user_name):
                    continue

                # logging.info(f'Page ID: {page_id}\tUsername: {user_name}')
                top10_page_ids = list(top10_pages_df[top10_pages_df['user_name'] == user_name]['page_id'])
                top10_page_ids = list(filter(lambda x: x != page_id, top10_page_ids))

                page_json = collection.find_one(filter={'_id': int(page_id)})
                if page_json is None:
                    continue

                title = str(page_json['title'])
                text = str(page_json['text'][:1000])

                title_similarity_scores = []
                summary_similarity_scores = []
                for k_page_id in top10_page_ids:
                    k_page_json = collection.find_one(filter={'_id': int(k_page_id)})

                    if k_page_json is None:
                        continue

                    if 'title' in k_page_json:
                        k_title = str(k_page_json['title'])
                        title_sim = esa.get_semantic_similarity(title, k_title)
                        title_similarity_scores.append(0.0 if title_sim is None else title_sim)

                    if 'text' in k_page_json:
                        k_text = str(k_page_json['text'][:1000])
                        text_sim = esa.get_semantic_similarity(text, k_text, long_text=True)
                        summary_similarity_scores.append(0.0 if text_sim is None else text_sim)

                f_title_similarity = np.mean(title_similarity_scores) if len(title_similarity_scores) > 0 else 0.0
                f_summary_similarity = np.mean(summary_similarity_scores) if len(summary_similarity_scores) > 0 else 0.0

                data.append([page_id, user_name, f_title_similarity, f_summary_similarity])

        return pd.DataFrame(data=data, columns=columns)


class ContributionSimilarity(PersistenceFeature):
    def cache_name(self):
        return 'contribution_similarity'

    def on_requires(self):
        return [CollectRevisions(self.data_dir)]

    def on_process(self, data_frames):
        data = []
        columns = ['page_id', 'user_name', 'contribution_similarity']

        esa = EsaProvider(self.data_dir)

        revs_df = data_frames[0]
        assert isinstance(revs_df, pd.DataFrame)

        with pymongo.MongoClient(host=config.get('MONGO', 'host'), port=config.get_int('MONGO', 'port')) as client:
            db = client.get_database(config.get('MONGO', 'database'))
            collection = db.get_collection('pages')

            persistence_db = client.get_database(config.get('MONGO', 'persistence_database'))
            persistence_collection = persistence_db.get_collection(config.get('MONGO', 'persistence_collection'))

            grouped = revs_df.groupby(by=['page_id', 'user_name'])
            for (page_id, user_name), group in grouped:
                if is_bot(user_name):
                    continue

                user_persists = {}
                for index, row in group.iterrows():
                    rev_id = row['revision_id']

                    rev_pers = self.get_persistence(rev_id=rev_id, collection=persistence_collection)
                    if isinstance(rev_pers, RevisionPersistence):
                        if user_name not in user_persists:
                            user_persists[user_name] = []
                        user_persists[user_name].append(rev_pers)

                if user_name not in user_persists:
                    contribution_similarity = .0
                else:
                    all_content_tokens = []
                    for rev_pers in user_persists[user_name]:
                        if isinstance(rev_pers, RevisionPersistence):
                            all_content_tokens.extend(rev_pers.content_tokens)

                    if len(all_content_tokens) == 0:
                        contribution_similarity = .0
                    else:
                        page_json = collection.find_one(filter={'_id': int(page_id)})
                        if page_json is None:
                            continue

                        summary = str(page_json['text'][:300])
                        contribution_similarity = esa.get_semantic_similarity(summary, ' '.join(all_content_tokens))

                data.append([page_id, user_name, contribution_similarity])

        return pd.DataFrame(data=data, columns=columns)
