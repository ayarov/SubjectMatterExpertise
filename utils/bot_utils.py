import os
import csv


def load_bots():
    bots = {}
    with open(os.path.join('resources', 'bots.csv'), 'rb') as csv_file:
        bot_reader = csv.reader(csv_file, delimiter=',', quotechar='|')
        for row in bot_reader:
            bots[row[1]] = True
        csv_file.close()
    return bots


bot_users = load_bots()


def is_bot(user_name):
    lc_name = str(user_name)
    return user_name in bot_users or lc_name.startswith('bot') or lc_name.endswith('bot')
