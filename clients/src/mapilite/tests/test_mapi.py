
import unittest
import ctypes


class MapiTests(unittest.TestCase):

    def __init__(self, *args):
        #TODO: add support for non macos platforms
        self.libmapi = ctypes.cdll.LoadLibrary("../.libs/libmapilite.dylib")
        unittest.TestCase.__init__(self, *args)

    def test_mapi_mapi(self):
        print self.libmapi.mapi_mapi()
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

