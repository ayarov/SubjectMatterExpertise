from nltk import word_tokenize
from nltk.corpus import wordnet
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer


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

