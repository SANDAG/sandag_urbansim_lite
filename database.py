"""
Database utility functions commonly used at SANDAG.
"""
import yaml


def get_connection_string(cfg_file, cfg_section):
    with open(cfg_file, 'r') as db_config_file:
        return _get_connection_string_from_stream(yaml.load(db_config_file), cfg_section)


def _get_connection_string_from_stream(in_stream, cfg_section):
    """
    Function to read a given YAML file's specific
    section and build a SQLAlchemy connection string.

    Args:
        cfg_file (string): Location of YAML file
        cfg_section (string): Section of YAML file to process

    Returns:
        (string): SQLAlchemy connection string
    """
    alchemy_driver = in_stream[cfg_section]['sql_alchemy_driver']
    driver = in_stream[cfg_section]['driver']
    host = in_stream[cfg_section]['host']
    database = in_stream[cfg_section]['database']
    port = in_stream[cfg_section]['port']
    user = in_stream[cfg_section]['user']
    password = in_stream[cfg_section]['password']

    if user is not None and password is not None:
        credentials = "{0}:{1}@".format(user, password)
    else:
        credentials = ""

    if port is not None:
        port_fmt = ":{0}".format(port)
    else:
        port_fmt = ""

    if database is not None:
        database_fmt = "/{0}".format(database)
    else:
        database_fmt = ""

    if driver is not None:
        driver_fmt = "?driver={0}".format(driver)
    else:
        driver_fmt = ""

    return "{0}://{1}{2}{3}{4}{5}".format(
        alchemy_driver, credentials, host,
        port_fmt, database_fmt, driver_fmt)
