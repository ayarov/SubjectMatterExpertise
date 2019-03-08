import pandas as pd
from tasks.features.base import FeatureTask
from tasks.collectors.revision import CollectTalkRevisions
from tasks.calculators.talk_page import CalculateTalkPageTotalEdits


class UserTalkPageEditsFeature(FeatureTask):
    def cache_name(self):
        return 'user_talk_page_edits'

    def on_requires(self):
        return [CollectTalkRevisions(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        data = []
        columns = ['page_id', 'user_name', 'talk_page_edits']
        revs_df = data_frames[0]
        if isinstance(revs_df, pd.DataFrame):
            # for page_id in revs_df['page_id'].unique():
            grouped = revs_df.groupby(by=['page_id', 'user_name'])
            for (page_id, user_name), group in grouped:
                data.append([page_id, user_name, len(group)])

        return pd.DataFrame(data=data, columns=columns)


class UserTalkPageEditsRatioFeature(FeatureTask):
    def cache_name(self):
        return 'user_talk_page_edits_ratio'

    def on_requires(self):
        return [CalculateTalkPageTotalEdits(data_dir=self.data_dir),
                UserTalkPageEditsFeature(data_dir=self.data_dir)]

    def on_process(self, data_frames):
        pte_df = data_frames[0]
        pef_df = data_frames[1]

        if isinstance(pef_df, pd.DataFrame):
            df = pd.merge(left=pef_df, right=pte_df, on=['page_id'], how='left')
            assert(len(df) == len(pef_df))
            df['talk_page_edits_ratio'] = df.apply(lambda row: row['talk_page_edits']/float(row['total_edits']), axis=1)
            return df[['page_id', 'user_name', 'talk_page_edits_ratio']]
