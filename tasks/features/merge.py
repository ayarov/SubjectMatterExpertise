import logging
import pandas as pd
from config import Configuration
from functools import reduce

from tasks.features.base import FeatureTask

from tasks.features.page import NameSpacesFeature
from tasks.features.page import UserPageEditsFeature
from tasks.features.page import TotalEditedPagesFeature
from tasks.features.page import UserPageEditsRatioFeature
from tasks.features.page import PageCategoriesOverlap
from tasks.features.page import PageLinksOverlap

from tasks.features.talk_page import UserTalkPageEditsFeature
from tasks.features.talk_page import UserTalkPageEditsRatioFeature

from tasks.features.edit import EditSizeFeature
from tasks.features.edit import EditTypesFeature
from tasks.features.edit import EditPeriodsFeature
from tasks.features.edit import EditFrequencyFeature
from tasks.features.edit import PageEditsEntropyFeature

from tasks.features.user import TenureFeature
from tasks.features.user import GenderFeature

from tasks.features.persistence import PersistenceFeature

from tasks.features.contribution import SubjectSimilarityFeature
from tasks.features.contribution import ContributionSimilarity

config = Configuration()
logging.basicConfig(filename='sme.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(levelname)s:%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


class MergeFeatures(FeatureTask):
    def cache_name(self):
        return 'features'

    def on_requires(self):
        return [UserPageEditsFeature(data_dir=self.data_dir),
                UserPageEditsRatioFeature(data_dir=self.data_dir),
                UserTalkPageEditsFeature(data_dir=self.data_dir),
                UserTalkPageEditsRatioFeature(data_dir=self.data_dir),
                EditPeriodsFeature(data_dir=self.data_dir),
                EditFrequencyFeature(data_dir=self.data_dir),
                EditSizeFeature(data_dir=self.data_dir),
                EditTypesFeature(data_dir=self.data_dir),
                TenureFeature(data_dir=self.data_dir),
                GenderFeature(data_dir=self.data_dir),
                NameSpacesFeature(data_dir=self.data_dir),
                TotalEditedPagesFeature(data_dir=self.data_dir),
                PageEditsEntropyFeature(data_dir=self.data_dir),
                PersistenceFeature(data_dir=self.data_dir),
                SubjectSimilarityFeature(data_dir=self.data_dir),
                PageCategoriesOverlap(data_dir=self.data_dir),
                PageLinksOverlap(data_dir=self.data_dir),
                ContributionSimilarity(data_dir=self.data_dir)]

    @staticmethod
    def merge(x, y):
        df = pd.merge(x, y, on=['page_id', 'user_name'], how='left')
        logging.info('Merge: {} + {} = {}'.format(x.shape, y.shape, df.shape))
        return df

    def on_process(self, data_frames):
        logging.info('Merging features...')
        df = reduce(lambda x, y: self.merge(x, y), data_frames)
        logging.info('Features shape: {}'.format(df.shape))
        return df
