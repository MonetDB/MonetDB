# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

try:
    from MonetDB import MapiLib
except ImportError:
    # if run from the build directory, MapiLib is not in the MonetDB module
    import MapiLib

class Mapi:
    def __init__(self, host = None, port = 0, username = None, password = None, lang = None):
        self.__mid = MapiLib.mapi_connect(host, int(port), username, password, lang, None)
        if not self.__mid:
            raise IOError("Creating connection structure failed")
        if self.error():
            raise IOError(self.error_str())

    def __del__(self):
        if MapiLib:
            MapiLib.mapi_destroy(self.__mid)
        del self.__mid

    def error(self):
        return MapiLib.mapi_error(self.__mid)

    def error_str(self):
        return MapiLib.mapi_error_str(self.__mid)

    def explain(self, f):
        ret = MapiLib.mapi_explain(self.__mid, f)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.error_str())

    def trace(self, flag):
        ret = MapiLib.mapi_trace(self.__mid, flag)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.error_str())

    def trace_log(self, nme):
        ret = MapiLib.mapi_trace_log(self.__mid, nme)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.error_str())

    def ping(self):
        ret = MapiLib.mapi_ping(self.__mid)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.error_str())

    def setAutocommit(self, autocommit):
        ret = MapiLib.mapi_setAutocommit(self.__mid, autocommit)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.error_str())

    def new_handle(self):
        hdl = MapiLib.mapi_new_handle(self.__mid)
        if not hdl:
            raise RuntimeError(self.error_str())
        return MapiQuery(hdl, self)

    def disconnect(self):
        ret = MapiLib.mapi_disconnect(self.__mid)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.error_str())

    def reconnect(self):
        ret = MapiLib.mapi_reconnect(self.__mid)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.error_str())

    def timeout(self, time):
        ret = MapiLib.mapi_timeout(self.__mid, time)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.error_str())

    def prepare(self, cmd):
        hdl = MapiLib.mapi_prepare(self.__mid, cmd)
        if not hdl:
            raise RuntimeError(self.error_str())
        hdl = MapiQuery(hdl, self)
        if self.error():
            raise RuntimeError(self.error_str())
        return hdl

    def prepare_array(self, cmd, argv):
        hdl = MapiLib.mapi_prepare_array(self.__mid, cmd, argv)
        if not hdl:
            raise RuntimeError(self.error_str())
        hdl = MapiQuery(hdl, self)
        if self.error():
            raise RuntimeError(self.error_str())
        return hdl

    def query(self, cmd):
        hdl = MapiLib.mapi_query(self.__mid, cmd)
        if not hdl:
            raise RuntimeError(self.error_str())
        hdl = MapiQuery(hdl, self)
        if self.error():
            raise RuntimeError(hdl.result_error())
        return hdl

    def query_array(self, cmd, argv):
        hdl = MapiLib.mapi_query_array(self.__mid, cmd, argv)
        if not hdl:
            raise RuntimeError(self.error_str())
        hdl = MapiQuery(hdl, self)
        if self.error():
            raise RuntimeError(self.error_str())
        return hdl

    def query_prep(self):
        hdl = MapiLib.mapi_query_prep(self.__mid)
        if not hdl:
            raise RuntimeError(self.error_str())
        hdl = MapiQuery(hdl, self)
        if self.error():
            raise RuntimeError(self.error_str())
        return hdl

    def quick_query(self, cmd, f):
        hdl = MapiLib.mapi_quick_query(self.__mid, cmd, f)
        if not hdl:
            raise RuntimeError(self.error_str())
        hdl = MapiQuery(hdl, self)
        if self.error():
            raise RuntimeError(self.error_str())
        return hdl

    def quick_query_array(self, cmd, argv, f):
        hdl = MapiLib.mapi_quick_query_array(self.__mid, cmd, argv, f)
        if not hdl:
            raise RuntimeError(self.error_str())
        hdl = MapiQuery(hdl, self)
        if self.error():
            raise RuntimeError(self.error_str())
        return hdl

    def stream_query(self, cmd, winsize):
        hdl = MapiLib.mapi_query_array(self.__mid, cmd, winsize)
        if not hdl:
            raise RuntimeError(self.error_str())
        hdl = MapiQuery(hdl, self)
        if self.error():
            raise RuntimeError(self.error_str())
        return hdl

    def cache_limit(self, limit):
        ret = MapiLib.mapi_cache_limit(self.__mid, limit)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.error_str())

    def get_dbname(self):
        ret = MapiLib.mapi_get_dbname(self.__mid)
        if self.error():
            raise IOError(self.error_str())
        return ret

    def get_host(self):
        ret = MapiLib.mapi_get_host(self.__mid)
        if self.error():
            raise IOError(self.error_str())
        return ret

    def get_user(self):
        ret = MapiLib.mapi_get_user(self.__mid)
        if self.error():
            raise IOError(self.error_str())
        return ret

    def get_lang(self):
        ret = MapiLib.mapi_get_lang(self.__mid)
        if self.error():
            raise IOError(self.error_str())
        return ret

    def get_mapi_version(self):
        ret = MapiLib.mapi_get_mapi_version(self.__mid)
        if self.error():
            raise IOError(self.error_str())
        return ret

    def get_monet_version(self):
        ret = MapiLib.mapi_get_monet_version(self.__mid)
        if self.error():
            raise IOError(self.error_str())
        return ret

    def get_monet_versionId(self):
        ret = MapiLib.mapi_get_monet_versionId(self.__mid)
        if self.error():
            raise IOError(self.error_str())
        return ret

    def get_motd(self):
        ret = MapiLib.mapi_get_motd(self.__mid)
        if self.error():
            raise IOError(self.error_str())
        return ret

    def is_blocked(self):
        ret = MapiLib.mapi_is_blocked(self.__mid)
        if self.error():
            raise IOError(self.error_str())
        return ret

    def is_connected(self):
        ret = MapiLib.mapi_is_connected(self.__mid)
        if self.error():
            raise IOError(self.error_str())
        return ret

class Embedded(Mapi):
    def __init__(self, dbfarm = None, dbname = "demo", lang = "sql", version = 5):
        if version == 5:
            import monetdb5 as monetdb
        else:
            import monetdb
        if lang == "sql":
            self._Mapi__mid = monetdb.monetdb_sql(dbfarm, dbname)

        if not self._Mapi__mid:
            raise IOError("Creating connection structure failed")
        if self.error():
            raise IOError(self.error_str())

class MapiQuery:
    def __init__(self, hdl, mid):
        self.__hdl = hdl
        self.__mid = mid

    def __del__(self):
        MapiLib.mapi_close_handle(self.__hdl)
        del self.__hdl
        del self.__mid

    def query_handle(self, cmd):
        ret = MapiLib.mapi_query_handle(self.__hdl, cmd)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())
    query = query_handle

    def fetch_field_line(self):
        ret = MapiLib.mapi_fetch_field_line(self.__hdl)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def explain_query(self, f):
        ret = MapiLib.mapi_explain_query(self.__hdl, f)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())
        explain = explain_query

    def explain_result(self, f):
        ret = MapiLib.mapi_explain_result(self.__hdl, f)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())

    def result_error(self):
        ret = MapiLib.mapi_result_error(self.__hdl)
        if ret:
            return ret
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def next_result(self):
        ret = MapiLib.mapi_next_result(self.__hdl)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def more_results(self):
        ret = MapiLib.mapi_more_results(self.__hdl)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def prepare_handle(self, cmd):
        ret = MapiLib.mapi_prepare_handle(self.__hdl, cmd)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())
    prepare = prepare_handle

    def execute(self):
        ret = MapiLib.mapi_execute(self.__hdl)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())

    def execute_array(self, argv):
        ret = MapiLib.mapi_execute_array(self.__hdl, argv)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())

    def query_part(self, cmd):
        ret = MapiLib.mapi_query_part(self.__hdl, cmd)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())

    def query_done(self):
        ret = MapiLib.mapi_query_done(self.__hdl)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())

    def cache_shuffle(self, percentage):
        ret = MapiLib.mapi_cache_shuffle(self.__hdl, percentage)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())

    def fetch_reset(self):
        ret = MapiLib.mapi_fetch_reset(self.__hdl)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())

    def seek_row(self, rowne, whence):
        ret = MapiLib.mapi_seek_row(self.__hdl, rowne, whence)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())

    def cache_freeup(self, percentage):
        ret = MapiLib.mapi_cache_freeup(self.__hdl, percentage)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())

    def fetch_line(self):
        ret = MapiLib.mapi_fetch_line(self.__hdl)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def finish(self):
        ret = MapiLib.mapi_finish(self.__hdl)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())

    def quick_response(self, fd):
        ret = MapiLib.mapi_quick_response(self.__hdl, fd)
        if ret == MapiLib.MERROR:
            raise RuntimeError(self.__mid.error_str())
        if ret == MapiLib.MTIMEOUT:
            raise IOError(self.__mid.error_str())

    def fetch_row(self):
        ret = MapiLib.mapi_fetch_row(self.__hdl)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def fetch_all_rows(self):
        ret = MapiLib.mapi_fetch_all_rows(self.__hdl)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def fetch_field(self, fnr):
        ret = MapiLib.mapi_fetch_field(self.__hdl, fnr)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def fetch_field_array(self):
        ret = MapiLib.mapi_fetch_field_array(self.__hdl)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def get_field_count(self):
        ret = MapiLib.mapi_get_field_count(self.__hdl)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def get_row_count(self):
        ret = MapiLib.mapi_get_row_count(self.__hdl)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def get_name(self, fnr):
        ret = MapiLib.mapi_get_name(self.__hdl, fnr)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def get_type(self, fnr):
        ret = MapiLib.mapi_get_type(self.__hdl, fnr)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def get_table(self, fnr):
        ret = MapiLib.mapi_get_table(self.__hdl, fnr)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def get_len(self, fnr):
        ret = MapiLib.mapi_get_len(self.__hdl, fnr)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def get_querytype(self):
        ret = MapiLib.mapi_get_querytype(self.__hdl)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

    def rows_affected(self):
        ret = MapiLib.mapi_rows_affected(self.__hdl)
        if self.__mid.error():
            raise IOError(self.__mid.error_str())
        return ret

