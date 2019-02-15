import pandas as pd
from wikitools import wiki, api


def get_contributions(site, users):
    params = \
        {
            'action': 'query',
            'format': 'json',
            'list': 'usercontribs',
            'ucprop': 'ids|timestamp',
            'uclimit': 1,
            'ucdir': 'newer',
            'ucuser': '|'.join(users)
        }

    return api.APIRequest(site, params).query(querycontinue=False)


def get_site():
    return wiki.Wiki("https://en.wikipedia.org/w/api.php")


def get_revisions():
    return pd.read_hdf(r'D:\data\sme\revisions.h5', mode='r')


def run():
    site = get_site()
    revs_df = get_revisions()
    user_names = revs_df['user_name'].unique()

    for user_name in user_names:
        result = get_contributions(site, user_name)
        print(result)
        break


run()
