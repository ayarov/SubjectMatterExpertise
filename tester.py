# import pandas as pd
#
# df = pd.read_hdf(path_or_buf=r'D:\data\sme\page_edit_entropy.h5')
# print(df.head())
#
#
#
#
#

import mwclient
site = mwclient.Site('en.wikipedia.org')


def get_gender(users):
    result = {}
    users = site.users(users=users, prop=['gender'])
    for user in users:
        name = user['name']
        if 'missing' in user:
            result[name] = 'unknown'
        else:
            gender = user['gender']
            result[name] = gender
    return result

print(get_gender(users=['NRuiz', 'Deacon Vorbis', 'Rick Norwood']))
