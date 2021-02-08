# Import libs
from configparser import ConfigParser  # read abt configparser
from pathlib import Path  # read abt pathlib

#TODO: make this an object

#Parses file that contains configurations for postgresql db
def get_db_config_vals(config_db):
    db_conn_dict = {}  # empty dict
    section = 'postgresql'  # header of database.ini
    config_file_path = 'config/' + config_db
    if (len(config_file_path) > 0 and len(section) > 0):
        # Create configParser and read vals from file
        config_parser = ConfigParser()
        config_parser.read(config_file_path)
        # check if the section name is present in the file
        if (config_parser.has_section(section)):
            # Read values of this section
            config_params = config_parser.items(section)  # config_vals

            for val in config_params:
                db_conn_dict[val[0]] = val[1]

    return db_conn_dict

#Parses file that contains configurations for the interaction with airtable API:
def get_airtable_config_vals(config):
    vals = {} 
    section = 'airtable'
    config_file_path = 'config/' + config

    if (len(config_file_path) > 0 and len(section) > 0):
        config_parser = ConfigParser()
        config_parser.read(config_file_path)

        if (config_parser.has_section(section)):
            config_params = config_parser.items(section)
            
            for val in config_params:
                vals[val[0]] = val[1]
    return vals
