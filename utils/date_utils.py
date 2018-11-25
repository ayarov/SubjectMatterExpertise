from dateutil import parser
from utils.str_utils import normalize_string


def parse_timestamp(timestamp):
    return parser.parse(normalize_string(timestamp))
