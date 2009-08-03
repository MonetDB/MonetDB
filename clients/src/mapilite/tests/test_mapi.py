
import unittest
import ctypes
import ctypes.util
import sys
import os


mapi_int64 = ctypes.c_longlong

class MapiParam(ctypes.Structure):
    pass

class MapiResultSet(ctypes.Structure):
    pass

class MapiStatement(ctypes.Structure):
    pass

class MapiStruct(ctypes.Structure):
    pass

MapiHdl = MapiStatement
Mapi = MapiStruct

MapiParam._fields_ = [
        ('inparam', ctypes.c_void_p),
        ('sizeptr', ctypes.POINTER(ctypes.c_int)),
        ('intype', ctypes.c_int),
        ('outtype', ctypes.c_int),
        ('precision', ctypes.c_int),
        ('scale', ctypes.c_int),
    ]

MapiResultSet._fields_ = [
        ('next', ctypes.POINTER(MapiResultSet)),
        ('hdl', ctypes.POINTER(MapiStatement)),
        ('tableid', ctypes.c_int),
        ('querytype', ctypes.c_int),
        ('row_count', mapi_int64),
        ('last_id', mapi_int64),
        ('fieldcnt', ctypes.c_int),
        ('maxfieldsm', ctypes.c_int),
        ('errorstr', ctypes.c_char_p)
        #struct MapiColumn *fields
        #struct MapiRowBuf cache
    ]

MapiStatement._fields_ = [
        ('mid', ctypes.POINTER(MapiStruct)),
        ('template', ctypes.c_char_p),
        ('query', ctypes.c_char_p),
        ('maxbindings', ctypes.c_int),
        #struct MapiBinding *bindings
        ('maxparams', ctypes.c_int),
        #struct MapiParam *params
        #struct MapiResultSet *result, *active, *lastresult
        ('needmore', ctypes.c_int),
        ('pending_close', ctypes.POINTER(ctypes.c_int)),
        ('npending_close', ctypes.c_int),
        ('prev', ctypes.POINTER(MapiStatement)),
        ('next', ctypes.POINTER(MapiStatement)),
    ]

MapiStruct._fields_ = [
        ('server', ctypes.c_char_p),
        ('mapiversion', ctypes.c_char_p),
        ('hostname', ctypes.c_char_p),
        ('port', ctypes.c_int),
        ('username', ctypes.c_char_p),
        ('password', ctypes.c_char_p),
        ('language', ctypes.c_char_p),
        ('database', ctypes.c_char_p),
        ('languageId', ctypes.c_int),
        ('versionId', ctypes.c_int),
        ('motd', ctypes.c_char_p),

        ('profile', ctypes.c_int),
        ('trace', ctypes.c_int),
        ('auto_commit', ctypes.c_int),
        ('noexplain', ctypes.c_char_p),
        ('error', ctypes.c_int),
        ('errorstr', ctypes.c_char_p),
        ('action', ctypes.c_char_p),

        #struct BlockCache blk
        ('connected', ctypes.c_int),
        ('first', MapiHdl),
        ('active', MapiHdl),

        ('cachelimit', ctypes.c_int),
        ('redircnt', ctypes.c_int),
        ('redirmax', ctypes.c_int),
        #char *redirects[50],

        #stream *tracelog,
        #stream *from, *to,
        ('index', ctypes.c_int),
    ]


class MapiTests(unittest.TestCase):

    def __init__(self, *args):
        location = ctypes.util.find_library('libmapilite')
        if not location:
            locations = {
                    #TODO: add more platforms
                    'darwin': '../.libs/libmapilite.dylib',
                    'linux2': '../.libs/libmapilite.so',
                    }
            location = locations[sys.platform]
        self.libmapi = ctypes.cdll.LoadLibrary(location)
        unittest.TestCase.__init__(self, *args)

    def test_mapi_mapi(self):
        host = 'localhost'
        port = os.environ.get('MAPIPORT', '50000')
        username = 'monetdb'
        password = 'monetdb'
        lang = 'sql'
        db = os.environ.get('TSTDB', 'demo')

        mapi_mapi = self.libmapi.mapi_mapi
        mapi_mapi.argtypes = 6 * [ctypes.c_char_p]
        mapi_mapi.restype = ctypes.POINTER(Mapi)
        mapi = mapi_mapi(host, port, username, password, lang, db)

        print(mapi)
        print mapi.server
        print dir(mapi)
        print type(mapi)
        self.fail("not yet implemented")

    def mapi_destroy(self):
        self.fail("not yet implemented")

    def mapi_start_talking(self):
        self.fail("not yet implemented")

    def mapi_connect(self):
        self.fail("not yet implemented")

    def mapi_resolve(self):
        self.fail("not yet implemented")

    def mapi_embedded_init(self):
        self.fail("not yet implemented")

    def mapi_disconnect(self):
        self.fail("not yet implemented")

    def mapi_reconnect(self):
        self.fail("not yet implemented")

    def mapi_ping(self):
        self.fail("not yet implemented")

    def mapi_error(self):
        self.fail("not yet implemented")

    def mapi_error_str(self):
        self.fail("not yet implemented")

    def mapi_noexplain(self):
        self.fail("not yet implemented")

    def mapi_explain(self):
        self.fail("not yet implemented")

    def mapi_explain_query(self):
        self.fail("not yet implemented")

    def mapi_explain_result(self):
        self.fail("not yet implemented")

    def mapi_output(self):
        self.fail("not yet implemented")

    def mapi_stream_into(self):
        self.fail("not yet implemented")

    def mapi_profile(self):
        self.fail("not yet implemented")

    def mapi_trace(self):
        self.fail("not yet implemented")

    def mapi_get_trace(self):
        self.fail("not yet implemented")

    def mapi_log(self):
        self.fail("not yet implemented")

    def mapi_setAutocommit(self):
        self.fail("not yet implemented")

    def mapi_setAlgebra(self):
        self.fail("not yet implemented")

    def mapi_result_error(self):
        self.fail("not yet implemented")

    def mapi_next_result(self):
        self.fail("not yet implemented")

    def mapi_needmore(self):
        self.fail("not yet implemented")

    def mapi_more_results(self):
        self.fail("not yet implemented")

    def mapi_new_handle(self):
        self.fail("not yet implemented")

    def mapi_close_handle(self):
        self.fail("not yet implemented")

    def mapi_bind(self):
        self.fail("not yet implemented")

    def mapi_bind_var(self):
        self.fail("not yet implemented")

    def mapi_bind_numeric(self):
        self.fail("not yet implemented")

    def mapi_clear_bindings(self):
        self.fail("not yet implemented")

    def mapi_param_type(self):
        self.fail("not yet implemented")

    def mapi_param_string(self):
        self.fail("not yet implemented")

    def mapi_param(self):
        self.fail("not yet implemented")

    def mapi_param_numeric(self):
        self.fail("not yet implemented")

    def mapi_clear_params(self):
        self.fail("not yet implemented")

    def mapi_prepare(self):
        self.fail("not yet implemented")

    def mapi_prepare_handle(self):
        self.fail("not yet implemented")

    def mapi_virtual_result(self):
        self.fail("not yet implemented")

    def mapi_execute(self):
        self.fail("not yet implemented")

    def mapi_execute_array(self):
        self.fail("not yet implemented")

    def mapi_fetch_reset(self):
        self.fail("not yet implemented")

    def mapi_finish(self):
        self.fail("not yet implemented")

    def mapi_prepare_array(self):
        self.fail("not yet implemented")

    def mapi_query(self):
        self.fail("not yet implemented")

    def mapi_query_handle(self):
        self.fail("not yet implemented")

    def mapi_query_prep(self):
        self.fail("not yet implemented")

    def mapi_query_part(self):
        self.fail("not yet implemented")

    def mapi_query_done(self):
        self.fail("not yet implemented")

    def mapi_quick_query(self):
        self.fail("not yet implemented")

    def mapi_query_array(self):
        self.fail("not yet implemented")

    def mapi_quick_query_array(self):
        self.fail("not yet implemented")

    def mapi_stream_query(self):
        self.fail("not yet implemented")

    def mapi_cache_limit(self):
        self.fail("not yet implemented")

    def mapi_cache_shuffle(self):
        self.fail("not yet implemented")

    def mapi_cache_freeup(self):
        self.fail("not yet implemented")

    def mapi_quick_response(self):
        self.fail("not yet implemented")

    def mapi_seek_row(self):
        self.fail("not yet implemented")

    def mapi_timeout(self):
        self.fail("not yet implemented")

    def mapi_fetch_row(self):
        self.fail("not yet implemented")

    def mapi_fetch_all_rows(self):
        self.fail("not yet implemented")

    def mapi_get_field_count(self):
        self.fail("not yet implemented")

    def mapi_get_row_count(self):
        self.fail("not yet implemented")

    def mapi_get_last_id(self):
        self.fail("not yet implemented")

    def mapi_rows_affected(self):
        self.fail("not yet implemented")

    def mapi_fetch_field(self):
        self.fail("not yet implemented")

    def mapi_store_field(self):
        self.fail("not yet implemented")

    def mapi_fetch_field_array(self):
        self.fail("not yet implemented")

    def mapi_fetch_line(self):
        self.fail("not yet implemented")

    def mapi_split_line(self):
        self.fail("not yet implemented")

    def mapi_get_lang(self):
        self.fail("not yet implemented")

    def mapi_get_dbname(self):
        self.fail("not yet implemented")

    def mapi_get_host(self):
        self.fail("not yet implemented")

    def mapi_get_user(self):
        self.fail("not yet implemented")

    def mapi_get_mapi_version(self):
        self.fail("not yet implemented")

    def mapi_get_monet_version(self):
        self.fail("not yet implemented")

    def mapi_get_monet_versionId(self):
        self.fail("not yet implemented")

    def mapi_get_motd(self):
        self.fail("not yet implemented")

    def mapi_is_connected(self):
        self.fail("not yet implemented")

    def mapi_get_table(self):
        self.fail("not yet implemented")

    def mapi_get_name(self):
        self.fail("not yet implemented")

    def mapi_get_type(self):
        self.fail("not yet implemented")

    def mapi_get_len(self):
        self.fail("not yet implemented")

    def mapi_get_query(self):
        self.fail("not yet implemented")

    def mapi_get_querytype(self):
        self.fail("not yet implemented")

    def mapi_get_tableid(self):
        self.fail("not yet implemented")

    def mapi_quote(self):
        self.fail("not yet implemented")

    def mapi_unquote(self):
        self.fail("not yet implemented")

    def mapi_get_active(self):
        self.fail("not yet implemented")

