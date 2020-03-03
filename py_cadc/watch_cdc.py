import threading
import time

from .cdc import CDC


class WatchCDC(threading.Thread):
    """
    This class inherits from threading.Thread.
    an instance is created for each table that it watches over the cdc

    Parameters:
        __host = ip or host of sql server <string>
        __user = username of the datbase <string>
        __passwd = user password of the database <string>
        __db_name = name of the database <string>
        __table_name = name of the table that is monitored by the cdc <string>
        __port = number port of the database
        __time_interval =  time interval for monitoring changes <float>
    """

    def __init__(
        self, host, user, passwd, db_name, table_name, port=1433, time_interval=60,
        group=None, target=None, name=None, args=(), kwargs=None, daemon=None,
    ):
        """
        WatchCDC class builder
        """

        super().__init__(group=group, target=target, name=name, daemon=daemon)
        self.__host = host
        self.__user = user
        self.__passwd = passwd
        self.__db_name = db_name
        self.__table_name = table_name
        self.__port = port
        self.__time_interval = float(time_interval)
        self.__is_running = False
        try:
            self.__cdc = CDC(self.__host, self.__user, self.__passwd, self.__db_name)
            if not self.__cdc.check_odbc_connection():
                self.__cdc = None
        except Exception as err:
            self.__cdc = None
            print(f"Error creating CDC object: {err}")

    def run(self):
        """
        This method contains the implementation of what the
        thread will execute when called

        TODO: Logic to send the data obtained from the cdc in each thread
        """
        self.__is_running = True
        while self.__is_running:
            changes = self.__cdc.get_changes_from_cdc(str(self.__table_name))
            time.sleep(self.__time_interval)
            if changes is None:
                self.__is_running = False

    def stop(self):
        """
        this method is responsible for stopping thread execution.
        TODO: Finish the logic of this method to stop the thread
        """
        self.__is_running = False
