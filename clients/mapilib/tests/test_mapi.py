
import unittest
import ctypes
import ctypes.util
import sys
import os
import subprocess

import mapi_structs


hostname = 'localhost'
port = int(os.environ.get('MAPIPORT', '50000'))
username = 'monetdb'
password = 'monetdb'
lang = 'sql'
db = os.environ.get('TSTDB', 'demo')


class MapiBaseTests(unittest.TestCase):
    """ the base test class. Loads the mapi library """

    def __init__(self, *args):
        #location = ctypes.util.find_library('libMapi')

        try:
            p = subprocess.Popen(['monetdb-clients-config', '--libdir'], stdout=subprocess.PIPE)
        except OSError:
            raise Exception("monetdb-clients-config is not in your path")

        location = p.communicate()[0].strip()

        files = {
            #TODO: add more platforms
            'darwin': 'libMapi.dylib',
            'linux2': 'libMapi.so',
            'win32': 'libMapi.dll',
        }

        library = os.path.join(location, files[sys.platform])
        self.libmapi = ctypes.cdll.LoadLibrary(library)
        unittest.TestCase.__init__(self, *args)

    def _connect(self):
        mapi_connect = self.libmapi.mapi_connect
        mapi_connect.restype = mapi_structs.Mapi
        mid = mapi_connect(hostname, port, username, password, lang, db)
        self.assertEqual(mid.contents.error, 0, \
            "can't connect to database:  %s" % mid.contents.errorstr)
        return mid

    def _disconnect(self, mid):
        self.assertEqual(self.libmapi.mapi_disconnect(mid), 0)
        self.assertEqual(self.libmapi.mapi_destroy(mid), 0)



class MapiConnectTests(MapiBaseTests):
    """ here we test the connect and disconnect
    functions"""

    def test_mapi_connect(self):
        mid = self._connect()

        self.assertEqual(mid.contents.hostname, hostname)
        self.assertEqual(mid.contents.username, username)
        self.assertEqual(mid.contents.error, 0,
                "%s. would like 0, got %s" % (mid.contents.errorstr, mid.contents.error))
        self.assertEqual(mid.contents.errorstr, None)
        self.assertEqual(mid.contents.connected, 1)
        self.assertEqual(mid.contents.port, port)
        self._disconnect(mid)

    def mapi_mapi(self): # not a external used function
        mid = self._connect()
        self.assertEqual(mid.contents.hostname, hostname)
        self.assertEqual(mid.contents.port, port)
        self.assertEqual(mid.contents.username, username)
        self.assertEqual(mid.contents.error, 0)
        self.assertEqual(mid.contents.errorstr, None)
        self.assertEqual(mid.contents.connected, 1)

    def test_mapi_disconnect(self):
        mid = self._connect()
        mapi_disconnect = self.libmapi.mapi_disconnect
        message = mapi_disconnect(mid)
        self.assertEqual(message, 0)
        self.assertEqual(mid.contents.connected, 0)
        self.libmapi.mapi_destroy(mid)

    def test_mapi_destroy(self):
        mid = self._connect()
        self.libmapi.mapi_disconnect(mid)
        mapi_destroy = self.libmapi.mapi_destroy
        message = mapi_destroy(mid)
        self.assertEqual(message, 0)
        self.assertEqual(mid.contents.connected, 0)

    def test_mapi_reconnect(self):
        mid = self._connect()
        mapi_reconnect = self.libmapi.mapi_reconnect
        message = mapi_reconnect(mid)
        self.assertEqual(message, 0)
        self.assertEqual(mid.contents.connected, 1)
        # also test when disconnected
        self.libmapi.mapi_disconnect(mid)
        message = mapi_reconnect(mid)
        self.assertEqual(message, 0)
        self.assertEqual(mid.contents.connected, 1)
        self._disconnect(mid)

    def test_mapi_ping(self):
        mid = self._connect()
        mapi_ping = self.libmapi.mapi_ping
        message = mapi_ping(mid)
        self.assertEqual(message, 0)
        # also test when disconnected
        self.libmapi.mapi_disconnect(mid)
        message = mapi_ping(mid)
        self.assertEqual(message, -1)
        #self._disconnect(mid)


class MapiFunctionsTests(MapiBaseTests):
    """ Here we test everything _after_ connecting """

    def setUp(self):
        """ called for every test before the test is run """
        self.mid = self._connect()

    def tearDown(self):
        """ called for every test after the test is run """
        self._disconnect(self.mid)
        self.mid = None

    def mapi_start_talking(self):
        self.fail("not yet implemented")

    def mapi_resolve(self):
        self.fail("not yet implemented")

    def mapi_embedded_init(self):
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

