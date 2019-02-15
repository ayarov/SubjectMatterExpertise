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

df = pd.read_hdf(path_or_buf=r'D:\data\sme\tenure.h5')
print(df.shape)




