import threading
import time
from cdc import CDC


class WatchCDC(threading.Thread):
    """
    This class inherits from threading.Thread.
    an instance is created for each table that it watches over the cdc
    """

    def __init__(
        self, group=None, target=None, name=None, args=(), kwargs=None, daemon=None
    ):
        super().__init__(group=group, target=target, name=name, daemon=daemon)
        self.__host = args[0]
        self.__user = args[1]
        self.__passwd = args[2]
        self.__db_name = args[3]
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
        """
        self.__is_running = True
        while self.__is_running:
            changes = self.__cdc.get_changes_from_cdc("Table_1_cdc")
            print(changes if changes is not None else "nada")
            time.sleep(10)
            if changes is None:
                self.__is_running = False
        print("bye")

    def stop(self):
        """
        this method is responsible for stopping thread execution
        """
        self.__is_running = False
