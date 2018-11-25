import unicodedata


def normalize_string(s):
    text = unicodedata.normalize('NFKD', unicode(s)).encode('ascii', 'ignore')
    text = text.replace("\n", " ").replace("\t", " ")
    return text


def parse_string(s):
    return str(normalize_string(s))
