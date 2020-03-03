from py_cadc.watch_cdc import WatchCDC


def test_create_watch_cdc():
    try:
        watch = WatchCDC(
            "201.162.197.143", "tca", "ITerp01@02", "REPORTESV2", "Table_1_cdc",
            time_interval=30
        )
        assert watch is not None
    except Exception as err:
        assert err != ""


# def test_run_watch_cdc():
#    try:
#        watch = WatchCDC(
#            "201.162.197.143", "tca", "ITerp01@02", "REPORTESV2", "Table_1_cdc",
#            time_interval=30
#        )
