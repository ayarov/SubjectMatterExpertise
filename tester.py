# import pandas as pd
# from os import walk
# import os
#
# mypath = r'../../data/sme/csv'
#
# f = []
# for (dirpath, dirnames, filenames) in walk(mypath):
#     for filename in filenames:
#         if str(filename).endswith('.csv'):
#             df = pd.read_csv(os.path.join(dirpath, filename))
#             # df = pd.read_hdf(os.path.join(dirpath, filename))
#             if isinstance(df, pd.DataFrame):
#                 df.to_hdf(path_or_buf=os.path.join(dirpath, str(filename).replace('.csv', '.h5')), key='df', mode='w')

from nltk import downloader
downloader.download()
