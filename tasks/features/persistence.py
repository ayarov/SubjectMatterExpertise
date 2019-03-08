import json
import pymongo
import logging
import numpy as np
import pandas as pd
from utils.nlp_utils import stop_words
from config import Configuration
from .base import FeatureTask
from utils.nlp_utils import WordParser
from tasks.collectors.revision import CollectRevisions


config = Configuration()
word_parser = WordParser()
logging.basicConfig(filename='sme.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(levelname)s:%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


class RevisionPersistence(object):

    def __init__(self, rev_id):
        self.rev_id = rev_id
        self.punctuations = []
        self.content_tokens = []
        self.tokens = []
        self.token_persists = []


class PersistenceFeature(FeatureTask):

    punctuations = {}

    def cache_name(self):
        return 'persistence'

    def on_requires(self):
        return [CollectRevisions(data_dir=self.data_dir)]

    def get_persistence(self, rev_id, collection):
        result = RevisionPersistence(rev_id=rev_id)
        mongo_document = collection.find_one(filter={'_id': rev_id})

        if mongo_document:
            mongo_revision = mongo_document["revision"]
            try:
                mongo_revision = json.loads(mongo_revision)
                token_elements = mongo_revision["persistence"]["tokens"]

                for token_element in token_elements:

                    token = token_element["text"]
                    result.tokens.append(token)

                    token_persist = token_element["seconds_visible"]
                    result.token_persists.append(token_persist)

                    if token in self.punctuations:
                        words = token
                        is_punctuation = self.punctuations[token]
                    else:
                        words = word_parser.parse(token)
                        is_punctuation = len(words) == 0

                    if is_punctuation:
                        if token not in result.punctuations:
                            result.punctuations.append(token)
                        self.punctuations[token] = True
                    else:
                        if token not in result.content_tokens:
                            result.content_tokens.extend(words)
                        self.punctuations[token] = False
            except json.decoder.JSONDecodeError as ex:
                logging.error(ex.msg)

        return result

    def on_process(self, data_frames):
        data = []
        columns = ['page_id',
                   'user_name',
                   'avg_persistence',
                   'content_token_count',
                   'content_token_edit_count_avg',
                   'content_token_vs_stop_words',
                   'content_token_vs_token',
                   'contribution_similarity',
                   'persistence_exists']
        revs_df = data_frames[0]

        if isinstance(revs_df, pd.DataFrame):
            with pymongo.MongoClient(host=config.get('MONGO', 'host'), port=config.get_int('MONGO', 'port')) as client:
                db = client.get_database(config.get('MONGO', 'persistence_database'))
                collection = db.get_collection(config.get('MONGO', 'persistence_collection'))

                for (page_id, user_name), group in revs_df.groupby(by=['page_id', 'user_name']):

                    user_persists = {}
                    for index, row in group.iterrows():
                        rev_id = row['revision_id']

                        rev_pers = self.get_persistence(rev_id=rev_id, collection=collection)
                        if isinstance(rev_pers, RevisionPersistence):
                            if user_name not in user_persists:
                                user_persists[user_name] = []
                            user_persists[user_name].append(rev_pers)

                    f_avg_token_persistence = 0.0
                    f_total_content_tokens = 0.0
                    f_avg_content_tokens_per_edit = 0.0
                    f_content_token_vs_stop_words = 0.0
                    f_content_token_vs_token = 0.0
                    f_contribution_similarity = 0.0
                    f_persistence_exists = True

                    if user_name not in user_persists:
                        f_persistence_exists = False
                    else:
                        total_tokens = []
                        all_stop_word_tokens = []
                        all_token_persistences = []
                        all_content_tokens = []
                        all_content_tokens_per_edit = {}
                        for rev_pers in user_persists[user_name]:
                            if isinstance(rev_pers, RevisionPersistence):
                                # aggregating all persistences to a single collection
                                all_token_persistences.extend(rev_pers.token_persists)
                                # aggregating all content tokens
                                all_content_tokens.extend(rev_pers.content_tokens)
                                # aggregating content tokens by edit
                                all_content_tokens_per_edit[rev_pers.rev_id] = len(rev_pers.content_tokens)
                                # aggregating all tokens
                                total_tokens.extend(rev_pers.tokens)
                                # aggregate all stop words
                                for token in rev_pers.tokens:
                                    if token in stop_words():
                                        all_stop_word_tokens.append(token)

                        f_avg_token_persistence = np.mean(all_token_persistences) if len(all_token_persistences) > 0 else 0.0

                        f_total_content_tokens = len(all_content_tokens) if len(all_content_tokens) else 0.0

                        f_avg_content_tokens_per_edit = np.mean(list(all_content_tokens_per_edit.values())) \
                            if np.sum(list(all_content_tokens_per_edit.values())) > 0 else 0.0

                        a = len(all_content_tokens)
                        b = len(all_stop_word_tokens)
                        f_content_token_vs_stop_words = a/b if b > 0 else b/a if a > 0 else 0.0

                        a = len(all_content_tokens)
                        b = len(total_tokens)
                        f_content_token_vs_token = a/b if b > 0 else b/a if a > 0 else 0.0

                    data.append([page_id,
                                 user_name,
                                 f_avg_token_persistence,
                                 f_total_content_tokens,
                                 f_avg_content_tokens_per_edit,
                                 f_content_token_vs_stop_words,
                                 f_content_token_vs_token,
                                 f_contribution_similarity,
                                 1 if f_persistence_exists else 0])

        return pd.DataFrame(data=data, columns=columns)
