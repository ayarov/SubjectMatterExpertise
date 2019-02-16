# import pymongo
# from config import Configuration
#
# config = Configuration()
#
# with pymongo.MongoClient(host=config.get('MONGO', 'host'),
#                          port=config.get_int('MONGO', 'port')) as client:
#     db = client.get_database(config.get('MONGO', 'database'))
#     collection = db.get_collection(config.get('MONGO', 'collection'))
#     user_name = 'AnemoneProjectors'
#
#     agg_result = collection.aggregate([
#         {
#             '$match': {'user_name': user_name}
#         },
#         {
#             '$group': {'_id': "timestamp", 'first_edit': {'$min': "$timestamp"}}
#         },
#         {
#             '$project': {'_id': 0}
#         }])
#
#     for r in agg_result:
#         print(r)
#
#     # print(agg_result)

import pandas as pd


# ns_columns = ['ns{}_edit_dist'.format(ns) for ns in range(0, 16, 1)]
# df = pd.read_hdf(path_or_buf=r'D:\data\sme\name_spaces.h5')
# new_data = []
# new_cols = ['page_id', 'user_name'] + ns_columns
# if isinstance(df, pd.DataFrame):
#     for (page_id, user_name), group in df.groupby(by=['page_id', 'user_name']):
#         new_data.append([page_id, user_name] + list(group.iloc[0][ns_columns]))
#     new_df = pd.DataFrame(data=new_data, columns=new_cols)
#     print(new_df.shape)
#     new_df.to_hdf(path_or_buf=r'D:\data\sme\name_spaces_new.h5', key='df', mode='w')

# df = pd.read_hdf(path_or_buf=r'D:\data\sme\tenure.h5')
# new_data = []
# new_cols = ['page_id', 'user_name', 'tenure']
# if isinstance(df, pd.DataFrame):
#     for (page_id, user_name), group in df.groupby(by=['page_id', 'user_name']):
#         new_data.append([page_id, user_name, group.iloc[0]['tenure']])
#     new_df = pd.DataFrame(data=new_data, columns=new_cols)
#     print(new_df.shape)
#     new_df.to_hdf(path_or_buf=r'D:\data\sme\tenure_new.h5', key='df', mode='w')





