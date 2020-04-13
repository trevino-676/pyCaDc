from datetime import datetime

import pyodbc


class CDC:
    """
    cdc class

    it is a representation of the cdc connection in a sql server database
    (must be configured previously).

    Properties:
        host: ip address of sql server database server.
        user: database user.
        password: database user password
        database: database name. default <None>
        port: database port. default <1433>
        __sql_connection: sql server database connection <None>
        __last_change: code of the last record detected <None>
        __update_data: dictionary for the updated data in the cdc <Dictionary>
    """

    def __init__(self, host, user, password, database=None, port=1433):
        """
        constructor method for the class cdc

        Parameters:
            host: ip address of sql server <string>
            user: database user <string>
            password: database password <string>
            database: <optional> database name <string> <None>
            port: <optional> database port <int> <1433>
        """
        self.host = host
        self.user = user
        self.__password = password
        self.database = database
        self.port = port
        try:
            self.__sql_connection = self.__create_connection_to_mssql()
        except pyodbc.Error as error:
            raise error
        self.__last_change = ""
        self.__update_data = {}

    def __create_connection_to_mssql(self):
        """
        this method creates a conection to mssql server

        Parameters:

        Returns:
            pyodbc object with a conection
        """
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + self.host
            + ";DATABASE="
            + self.database
            + ";UID="
            + self.user
            + ";PWD="
            + self.__password
            + ";"
        )
        mssql_connector = None
        try:
            mssql_connector = pyodbc.connect(connection_string)
        except pyodbc.Error as pyodbcerror:
            mssql_connector = None
            raise pyodbcerror
        finally:
            return mssql_connector

    def get_changes_from_cdc(self, schema, table_name):
        """
        this methos return a lis of dictionary from changes in the cdc

        Parameters:
            table_name: <string> table name from database

        Returns:
            list_changes: <list>
        """
        from_table = f"cdc.{schema}_{table_name}_CT"
        select_statment = """SELECT
                __$start_lsn,
                __$operation"""
        columns_name = self.__get_columns_name(table_name)
        from_statment = f"""
            FROM {from_table}
            WHERE __$start_lsn > CONVERT(binary, '{self.__last_change}')
            ORDER BY {from_table}.[__$start_lsn] ASC
        """
        sep = ", "
        tsql = f"{select_statment}, {sep.join(columns_name)} {from_statment}"
        changes = self.__sql_connection.execute(tsql)

        list_changes = []
        for row in changes:
            self.__last_change = "".join(chr(x) for x in bytearray(row[0]))
            change = self.__construct_data_response(
                row, str(row[1]), columns_name, from_table
            )
            if change is not None:
                list_changes.append(change)

        if len(list_changes) == 0:
            return None

        return list_changes

    def __get_columns_name(self, table_name):
        """
        this method returns the columns name of the given table

        Parameters:
            table_name: <string> table name of the database

        Returns:
            colums_list: <list>
        """
        tsql = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """
        list_name = []
        try:
            column_names = self.__sql_connection.execute(tsql)
            if not column_names:
                return None
            list_name = [field[0] for field in column_names]
            return list_name
        except pyodbc.Error as error:
            print(error)
            return None

    def __construct_data_response(self, row, operation, columns_name, table_name):
        """
        this method transform a data list returned from cdc sql server
        to dictionary with this structure
        {
            'start_lsn': code <byte(10)> from the moment of replication,
            'operation': code <int> how represent a acction in the database:
                1: Delete,
                2: Insert,
                4: Update,
            'command': code <int> from the number of action,
            'data': field <dictionary>from the table with the values.
                If the action is an update then return 2 dictionaries whith the data
                before updated and after updated.
        }

        Parameters:
            row: data list from the cdc sql server <list>,
            operation: a code of action in the database <string>,
            columns_name: a list of the columns name of the table wached <list>

        Returns:
            <list>: a list of dictionaries of cdc sql server.
        """
        if operation == "1" or operation == "2":
            change = {
                "table": table_name,
                "datetime": datetime.now().strftime("%Y-%m-%d, %H,%M,%S"),
                "start_lsn": str(row[0]),
                "operation": row[1],
                "command": row[2],
                "data": self.__construct_data_dictionary(row, columns_name),
            }
            return change
        elif operation == "3":
            self.__update_data.update(
                {"before_data": self.__construct_data_dictionary(row, columns_name)}
            )
        elif operation == "4":
            self.__update_data.update(
                {"after_data": self.__construct_data_dictionary(row, columns_name)}
            )
            change = {
                "table": table_name,
                "datetime": datetime.now().strftime("%Y-%m-%d, %H,%M,%S"),
                "start_lsn": str(row[0]),
                "operation": row[1],
                "command": row[2],
                "data": self.__update_data,
            }
            self.__update_data = {}
            return change
        return None

    def __construct_data_dictionary(self, row, column_name):
        """
        this method construct a dictionary with the data captured in the cdc

        Parameters:
            data: a list with the data from cdc
            column_name: list with the columns name from the table

        Returns:
            dictionary
        """
        data = {}
        count = 2
        for column in column_name:
            data.update({column: str(row[count]).strip()})
            count = count + 1

        return data

    def check_odbc_connection(self):
        """
        Check the connection of odbc connection
        """
        if self.__sql_connection is not None:
            return True
        return False
