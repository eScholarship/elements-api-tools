def process_args():
    import argparse
    parser = argparse.ArgumentParser()

    def validate_connection(arg):
        arg = arg.upper()
        if not (arg == 'QA' or arg == 'PROD'):
            raise ValueError
        return arg

    parser.add_argument("-c", "--connection",
                        dest="connection",
                        type=validate_connection,
                        default='qa',
                        help="Specify 'qa' or 'prod' only.")

    parser.add_argument("-i", "--input",
                        dest='input_file',
                        help="Specify an input file.")

    parser.add_argument("--clear-previous",
                        dest="clear_previous",
                        action='store_true',
                        default=False,
                        help="Add this tag to clear the previous labels (etc)")

    return parser.parse_args()


def get_config():
    from dotenv import dotenv_values
    return dotenv_values(".env")


def get_reporting_db_connection(args, config):
    import pyodbc

    elements_reporting_db_conn = pyodbc.connect(
        driver=config['ELEMENTS_REPORTING_DB_DRIVER_' + args.connection],
        server=(config['ELEMENTS_REPORTING_DB_SERVER_' + args.connection] + ','
                + config['ELEMENTS_REPORTING_DB_PORT_' + args.connection]),
        database=config['ELEMENTS_REPORTING_DB_DATABASE_' + args.connection],
        uid=config['ELEMENTS_REPORTING_DB_USER_' + args.connection],
        pwd=config['ELEMENTS_REPORTING_DB_PASSWORD_' + args.connection],
        trustservercertificate='yes')

    # Required if the queries include TRANSACTION ISOLATION LEVEL
    elements_reporting_db_conn.autocommit = True
    return elements_reporting_db_conn
