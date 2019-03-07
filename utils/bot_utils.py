import os
import pandas as pd


def load_bots():
    # bots = {}
    df = pd.read_csv(filepath_or_buffer=os.path.join(os.getcwd(), 'resources', 'bots.csv'), header=None)
    df.rename(columns={1: 'bot_name'}, inplace=True)
    return list(df['bot_name'])


bot_users = load_bots()


def is_bot(user_name):
    lc_name = str(user_name)
    return user_name in bot_users or lc_name.startswith('bot') or lc_name.endswith('bot')
