import os
import re
import string
import base64
import logging
import hashlib
import requests
from config import Configuration
from nltk import word_tokenize
from nltk.corpus import wordnet
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer


config = Configuration()
logging.basicConfig(filename='sme.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(levelname)s:%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


def stop_words():
    return stopwords.words('english')


class WordParser:

    def __init__(self, stemmer=SnowballStemmer(language='english')):
        self.stemmer = stemmer

    def parse(self, word):
        words = []
        tokenized_words = word_tokenize(word)
        for tokenized_word in tokenized_words:
            stemmed_word = self.stemmer.stem(tokenized_word)
            if stemmed_word in stop_words():
                continue
            if not wordnet.synsets(stemmed_word):
                continue
            words.append(stemmed_word)
        return words


class EsaProvider:

    def __init__(self, cache_path, scheme='http', host='localhost', port=8080):
        self.scheme = scheme
        self.host = host
        self.port = port
        self.file_name = 'esa.csv'
        self.cache_path = os.path.join(cache_path, self._get_cache_ext())
        if not os.path.exists(self.cache_path):
            os.mkdir(self.cache_path)
        self.cache_file_path = os.path.join(self.cache_path, self.file_name)
        self.scores = self.__load_scores()

    def __load_scores(self):
        if os.path.exists(self.cache_file_path):
            with open(self.cache_file_path) as cache_file:
                lines = cache_file.readlines()

            scores_dictionary = {}
            for line in lines:
                arr = line.split(',')
                scores_dictionary[arr[0]] = float(arr[1])
            return scores_dictionary

        return {}

    def __save_score(self, key, value):
        self.scores[key] = value
        with open(self.cache_file_path, 'a') as cache_file:
            cache_file.write('{},{}\n'.format(key, value))

    def _get_cache_ext(self):
        return self.__class__.__name__

    def get_semantic_similarity(self, text_a, text_b, long_text=False, encoded=False):
        text_a = EsaProvider.__escape_punctuations(text_a)
        text_b = EsaProvider.__escape_punctuations(text_b)

        hash_object = hashlib.md5('{}_{}'.format(text_a, text_b).encode('utf-8'))
        key = hash_object.hexdigest()

        if key in self.scores:
            return self.scores[key]

        try:
            text_a = text_a.encode('utf-8')
            text_b = text_b.encode('utf-8')

            if encoded:
                text_a = base64.b64encode(text_a)
                text_b = base64.b64encode(text_b)

            if long_text:
                headers = {"Content-Type": "application/json"}
                data = {"textA": str(text_a), "textB": str(text_b), "encoded": encoded}
                url = '{}://{}:{}/esa'.format(self.scheme, self.host, self.port)
                resp = requests.post(url, headers=headers, json=data)
            else:
                is_encoded = 'true' if encoded else 'false'
                url = f'{self.scheme}://{self.host}:{self.port}/esa?textA={text_a}&textB={text_b}&encoded={is_encoded}'
                resp = requests.get(url)

            if resp.status_code == 200:
                response = resp.json()
                value = response['score']
                self.__save_score(key, value)
                return value
            else:
                logging.warning(resp.reason)
                return None

        except Exception as ex:
            logging.error(ex)
            return None

    @staticmethod
    def __escape_punctuations(text):
        chars = re.escape(string.punctuation)
        return re.sub(r'[' + chars + ']', '', text)

