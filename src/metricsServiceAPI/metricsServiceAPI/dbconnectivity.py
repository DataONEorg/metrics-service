"""
DB Connectivity module
"""

import psycopg2

HOSTNAME = 'localhost'
USERNAME = 'rushirajnenuji'
PASSWORD = ''
DATABASE = 'mdc'



class DBConnectivity:
    """
    Basic DB Connectivity class
    """

    def get_connection(self):
        """
        This method tries to make connection to the Database and
        returns the connection object when successful.
        :return: Connection object
        """
        try:
            print('Trying to connect to the DB...')
            my_connection = psycopg2.connect(host=HOSTNAME, user=USERNAME \
                                            , password=PASSWORD, dbname=DATABASE)
            print('Connected to the DB')
            return my_connection
        except psycopg2.DataError as error_message:
            print("DataError")
            print(error_message)

        except psycopg2.InternalError as error_message:
            print("InternalError")
            print(error_message)

        except psycopg2.IntegrityError as error_message:
            print("IntegrityError")
            print(error_message)

        except psycopg2.OperationalError as error_message:
            print("OperationalError")
            print(error_message)

        except psycopg2.NotSupportedError as error_message:
            print("NotSupportedError")
            print(error_message)

        except psycopg2.ProgrammingError as error_message:
            print("ProgrammingError")
            print(error_message)

        else:
            print("Unknown error occurred")

        finally:
            if my_connection is None:
                print("Couldn't connect to the Database")
