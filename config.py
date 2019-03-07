from configparser import ConfigParser


class Configuration:
    def __init__(self):
        self.config = ConfigParser()
        self.config.read('config.ini')

    def get(self, section, option):
        return self.config.get(section=section, option=option)

    def get_int(self, section, option):
        return self.config.getint(section=section, option=option)

    def get_float(self, section, option):
        return self.config.getfloat(section=section, option=option)

