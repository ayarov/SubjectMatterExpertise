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


# def get_talk_page(page_id):
r = site.pages.get(name='John Monteath Robertson')
print(r)
# print(get_gender(users=['NRuiz', 'Deacon Vorbis', 'Rick Norwood']))