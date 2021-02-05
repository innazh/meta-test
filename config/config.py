# Import libs
from configparser import ConfigParser  # read abt configparser
from pathlib import Path  # read abt pathlib


def config(config_db):
    db_conn_dict = {}  # empty dict
    section = 'postgresql'  # header of database.ini - read abt this
    config_file_path = 'config/' + config_db
    if (len(config_file_path) > 0 and len(section) > 0):
        # Create configParser
        config_parser = ConfigParser()
        # Read values from file
        config_parser.read(config_file_path)
        # check if the section name is present in the file
        if (config_parser.has_section(section)):
            # Read values of this section
            config_params = config_parser.items(section)  # config_vals

            # Convert to dictionary
            db_conn_dict = {}  # empty dict
            # loop through the list of config values and add it to the dict
            for val in config_params:
                db_conn_dict[val[0]] = val[1]
                return db_conn_dict
