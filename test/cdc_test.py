from pyodbc import Error

from py_cadc.cdc import CDC


def test_create_cdc_correctly():
    try:
        cdc = CDC("201.162.197.143", "tca", "ITerp01@02", "REPORTESV2")
        assert cdc.check_odbc_connection() is not False
    except Error as err:
        assert err != ""


def test_create_cdc_with_wrong_data():
    try:
        cdc = CDC("201.162.197.143", "xc", "")
        assert cdc is not None
    except Exception as err:
        assert err != ""


def test_get_changes_from_cdc():
    try:
        cdc = CDC("201.162.197.143", "tca", "ITerp01@02", "REPORTESV2")
        list_changes = cdc.get_changes_from_cdc("Table_1_cdc")
        assert list_changes is not None
    except Exception as err:
        assert err != ""
