from ctypes import *

STRING = c_char_p


P_PID = 1
WRITE_ERROR = 3
Q_PREPARE = 5
P_PGID = 2
Q_TRANS = 4
P_ALL = 0
Q_BLOCK = 6
Q_PARSE = 0
Q_TABLE = 1
READ_ERROR = 2
Q_UPDATE = 2
OPEN_ERROR = 1
Q_SCHEMA = 3
NO__ERROR = 0
MapiMsg = c_int
class MapiStruct(Structure):
    pass
Mapi = POINTER(MapiStruct)

# values for enumeration 'sql_query_t'
sql_query_t = c_int # enum
class MapiStatement(Structure):
    pass
MapiHdl = POINTER(MapiStatement)
mapi_uint64 = c_ulonglong
mapi_int64 = c_longlong
class MapiDate(Structure):
    pass
MapiDate._fields_ = [
    ('year', c_short),
    ('month', c_ushort),
    ('day', c_ushort),
]
class MapiTime(Structure):
    pass
MapiTime._fields_ = [
    ('hour', c_ushort),
    ('minute', c_ushort),
    ('second', c_ushort),
]
class MapiDateTime(Structure):
    pass
MapiDateTime._fields_ = [
    ('year', c_short),
    ('month', c_ushort),
    ('day', c_ushort),
    ('hour', c_ushort),
    ('minute', c_ushort),
    ('second', c_ushort),
    ('fraction', c_uint),
]
class MapiColumn(Structure):
    pass
MapiColumn._fields_ = [
    ('tablename', STRING),
    ('columnname', STRING),
    ('columntype', STRING),
    ('columnlength', c_int),
]
class MapiBinding(Structure):
    pass
MapiBinding._fields_ = [
    ('outparam', c_void_p),
    ('outtype', c_int),
    ('precision', c_int),
    ('scale', c_int),
]
class MapiParam(Structure):
    pass
MapiParam._fields_ = [
    ('inparam', c_void_p),
    ('sizeptr', POINTER(c_int)),
    ('intype', c_int),
    ('outtype', c_int),
    ('precision', c_int),
    ('scale', c_int),
]
class MapiRowBuf(Structure):
    pass
class N10MapiRowBuf4DOLLAR_14E(Structure):
    pass
MapiRowBuf._pack_ = 4
MapiRowBuf._fields_ = [
    ('rowlimit', c_int),
    ('shuffle', c_int),
    ('limit', c_int),
    ('writer', c_int),
    ('reader', c_int),
    ('first', mapi_int64),
    ('tuplecount', mapi_int64),
    ('line', POINTER(N10MapiRowBuf4DOLLAR_14E)),
]
N10MapiRowBuf4DOLLAR_14E._pack_ = 4
N10MapiRowBuf4DOLLAR_14E._fields_ = [
    ('fldcnt', c_int),
    ('rows', STRING),
    ('tupleindex', c_int),
    ('tuplerev', mapi_int64),
    ('anchors', POINTER(STRING)),
]
class BlockCache(Structure):
    pass
BlockCache._fields_ = [
    ('buf', STRING),
    ('lim', c_int),
    ('nxt', c_int),
    ('end', c_int),
    ('eos', c_int),
]
class stream(Structure):
    pass
MapiStruct._fields_ = [
    ('server', STRING),
    ('mapiversion', STRING),
    ('hostname', STRING),
    ('port', c_int),
    ('username', STRING),
    ('password', STRING),
    ('language', STRING),
    ('database', STRING),
    ('languageId', c_int),
    ('versionId', c_int),
    ('motd', STRING),
    ('profile', c_int),
    ('trace', c_int),
    ('auto_commit', c_int),
    ('noexplain', STRING),
    ('error', MapiMsg),
    ('errorstr', STRING),
    ('action', STRING),
    ('blk', BlockCache),
    ('connected', c_int),
    ('first', MapiHdl),
    ('active', MapiHdl),
    ('cachelimit', c_int),
    ('redircnt', c_int),
    ('redirmax', c_int),
    ('redirects', STRING * 50),
    ('tracelog', POINTER(stream)),
    ('from', POINTER(stream)),
    ('to', POINTER(stream)),
    ('index', c_int),
]
class MapiResultSet(Structure):
    pass
MapiResultSet._pack_ = 4
MapiResultSet._fields_ = [
    ('next', POINTER(MapiResultSet)),
    ('hdl', POINTER(MapiStatement)),
    ('tableid', c_int),
    ('querytype', c_int),
    ('row_count', mapi_int64),
    ('last_id', mapi_int64),
    ('fieldcnt', c_int),
    ('maxfields', c_int),
    ('errorstr', STRING),
    ('fields', POINTER(MapiColumn)),
    ('cache', MapiRowBuf),
]
MapiStatement._fields_ = [
    ('mid', POINTER(MapiStruct)),
    ('template_', STRING),
    ('query', STRING),
    ('maxbindings', c_int),
    ('bindings', POINTER(MapiBinding)),
    ('maxparams', c_int),
    ('params', POINTER(MapiParam)),
    ('result', POINTER(MapiResultSet)),
    ('active', POINTER(MapiResultSet)),
    ('lastresult', POINTER(MapiResultSet)),
    ('needmore', c_int),
    ('pending_close', POINTER(c_int)),
    ('npending_close', c_int),
    ('prev', MapiHdl),
    ('next', MapiHdl),
]
stream._fields_ = [
]
class buffer(Structure):
    pass
__darwin_size_t = c_ulong
size_t = __darwin_size_t
buffer._fields_ = [
    ('buf', STRING),
    ('pos', size_t),
    ('len', size_t),
]
class bstream(Structure):
    pass
bstream._fields_ = [
    ('s', POINTER(stream)),
    ('buf', STRING),
    ('size', size_t),
    ('pos', size_t),
    ('len', size_t),
    ('eof', c_int),
    ('mode', c_int),
]

# values for enumeration 'stream_errors'
stream_errors = c_int # enum
__darwin_nl_item = c_int
__darwin_wctrans_t = c_int
__darwin_wctype_t = c_ulong
class __darwin_mcontext32(Structure):
    pass
class __darwin_i386_exception_state(Structure):
    pass
__darwin_i386_exception_state._fields_ = [
    ('__trapno', c_uint),
    ('__err', c_uint),
    ('__faultvaddr', c_uint),
]
class __darwin_i386_thread_state(Structure):
    pass
__darwin_i386_thread_state._fields_ = [
    ('__eax', c_uint),
    ('__ebx', c_uint),
    ('__ecx', c_uint),
    ('__edx', c_uint),
    ('__edi', c_uint),
    ('__esi', c_uint),
    ('__ebp', c_uint),
    ('__esp', c_uint),
    ('__ss', c_uint),
    ('__eflags', c_uint),
    ('__eip', c_uint),
    ('__cs', c_uint),
    ('__ds', c_uint),
    ('__es', c_uint),
    ('__fs', c_uint),
    ('__gs', c_uint),
]
class __darwin_i386_float_state(Structure):
    pass
class __darwin_fp_control(Structure):
    pass
__darwin_fp_control._fields_ = [
    ('__invalid', c_ushort, 1),
    ('__denorm', c_ushort, 1),
    ('__zdiv', c_ushort, 1),
    ('__ovrfl', c_ushort, 1),
    ('__undfl', c_ushort, 1),
    ('__precis', c_ushort, 1),
    ('', c_ushort, 2),
    ('__pc', c_ushort, 2),
    ('__rc', c_ushort, 2),
    ('', c_ushort, 1),
    ('', c_ushort, 3),
]
class __darwin_fp_status(Structure):
    pass
__darwin_fp_status._fields_ = [
    ('__invalid', c_ushort, 1),
    ('__denorm', c_ushort, 1),
    ('__zdiv', c_ushort, 1),
    ('__ovrfl', c_ushort, 1),
    ('__undfl', c_ushort, 1),
    ('__precis', c_ushort, 1),
    ('__stkflt', c_ushort, 1),
    ('__errsumm', c_ushort, 1),
    ('__c0', c_ushort, 1),
    ('__c1', c_ushort, 1),
    ('__c2', c_ushort, 1),
    ('__tos', c_ushort, 3),
    ('__c3', c_ushort, 1),
    ('__busy', c_ushort, 1),
]
__uint8_t = c_ubyte
__uint16_t = c_ushort
__uint32_t = c_uint
class __darwin_mmst_reg(Structure):
    pass
__darwin_mmst_reg._fields_ = [
    ('__mmst_reg', c_char * 10),
    ('__mmst_rsrv', c_char * 6),
]
class __darwin_xmm_reg(Structure):
    pass
__darwin_xmm_reg._fields_ = [
    ('__xmm_reg', c_char * 16),
]
__darwin_i386_float_state._fields_ = [
    ('__fpu_reserved', c_int * 2),
    ('__fpu_fcw', __darwin_fp_control),
    ('__fpu_fsw', __darwin_fp_status),
    ('__fpu_ftw', __uint8_t),
    ('__fpu_rsrv1', __uint8_t),
    ('__fpu_fop', __uint16_t),
    ('__fpu_ip', __uint32_t),
    ('__fpu_cs', __uint16_t),
    ('__fpu_rsrv2', __uint16_t),
    ('__fpu_dp', __uint32_t),
    ('__fpu_ds', __uint16_t),
    ('__fpu_rsrv3', __uint16_t),
    ('__fpu_mxcsr', __uint32_t),
    ('__fpu_mxcsrmask', __uint32_t),
    ('__fpu_stmm0', __darwin_mmst_reg),
    ('__fpu_stmm1', __darwin_mmst_reg),
    ('__fpu_stmm2', __darwin_mmst_reg),
    ('__fpu_stmm3', __darwin_mmst_reg),
    ('__fpu_stmm4', __darwin_mmst_reg),
    ('__fpu_stmm5', __darwin_mmst_reg),
    ('__fpu_stmm6', __darwin_mmst_reg),
    ('__fpu_stmm7', __darwin_mmst_reg),
    ('__fpu_xmm0', __darwin_xmm_reg),
    ('__fpu_xmm1', __darwin_xmm_reg),
    ('__fpu_xmm2', __darwin_xmm_reg),
    ('__fpu_xmm3', __darwin_xmm_reg),
    ('__fpu_xmm4', __darwin_xmm_reg),
    ('__fpu_xmm5', __darwin_xmm_reg),
    ('__fpu_xmm6', __darwin_xmm_reg),
    ('__fpu_xmm7', __darwin_xmm_reg),
    ('__fpu_rsrv4', c_char * 224),
    ('__fpu_reserved1', c_int),
]
__darwin_mcontext32._fields_ = [
    ('__es', __darwin_i386_exception_state),
    ('__ss', __darwin_i386_thread_state),
    ('__fs', __darwin_i386_float_state),
]
class __darwin_mcontext64(Structure):
    pass
class __darwin_x86_exception_state64(Structure):
    pass
__uint64_t = c_ulonglong
__darwin_x86_exception_state64._pack_ = 4
__darwin_x86_exception_state64._fields_ = [
    ('__trapno', c_uint),
    ('__err', c_uint),
    ('__faultvaddr', __uint64_t),
]
class __darwin_x86_thread_state64(Structure):
    pass
__darwin_x86_thread_state64._pack_ = 4
__darwin_x86_thread_state64._fields_ = [
    ('__rax', __uint64_t),
    ('__rbx', __uint64_t),
    ('__rcx', __uint64_t),
    ('__rdx', __uint64_t),
    ('__rdi', __uint64_t),
    ('__rsi', __uint64_t),
    ('__rbp', __uint64_t),
    ('__rsp', __uint64_t),
    ('__r8', __uint64_t),
    ('__r9', __uint64_t),
    ('__r10', __uint64_t),
    ('__r11', __uint64_t),
    ('__r12', __uint64_t),
    ('__r13', __uint64_t),
    ('__r14', __uint64_t),
    ('__r15', __uint64_t),
    ('__rip', __uint64_t),
    ('__rflags', __uint64_t),
    ('__cs', __uint64_t),
    ('__fs', __uint64_t),
    ('__gs', __uint64_t),
]
class __darwin_x86_float_state64(Structure):
    pass
__darwin_x86_float_state64._fields_ = [
    ('__fpu_reserved', c_int * 2),
    ('__fpu_fcw', __darwin_fp_control),
    ('__fpu_fsw', __darwin_fp_status),
    ('__fpu_ftw', __uint8_t),
    ('__fpu_rsrv1', __uint8_t),
    ('__fpu_fop', __uint16_t),
    ('__fpu_ip', __uint32_t),
    ('__fpu_cs', __uint16_t),
    ('__fpu_rsrv2', __uint16_t),
    ('__fpu_dp', __uint32_t),
    ('__fpu_ds', __uint16_t),
    ('__fpu_rsrv3', __uint16_t),
    ('__fpu_mxcsr', __uint32_t),
    ('__fpu_mxcsrmask', __uint32_t),
    ('__fpu_stmm0', __darwin_mmst_reg),
    ('__fpu_stmm1', __darwin_mmst_reg),
    ('__fpu_stmm2', __darwin_mmst_reg),
    ('__fpu_stmm3', __darwin_mmst_reg),
    ('__fpu_stmm4', __darwin_mmst_reg),
    ('__fpu_stmm5', __darwin_mmst_reg),
    ('__fpu_stmm6', __darwin_mmst_reg),
    ('__fpu_stmm7', __darwin_mmst_reg),
    ('__fpu_xmm0', __darwin_xmm_reg),
    ('__fpu_xmm1', __darwin_xmm_reg),
    ('__fpu_xmm2', __darwin_xmm_reg),
    ('__fpu_xmm3', __darwin_xmm_reg),
    ('__fpu_xmm4', __darwin_xmm_reg),
    ('__fpu_xmm5', __darwin_xmm_reg),
    ('__fpu_xmm6', __darwin_xmm_reg),
    ('__fpu_xmm7', __darwin_xmm_reg),
    ('__fpu_xmm8', __darwin_xmm_reg),
    ('__fpu_xmm9', __darwin_xmm_reg),
    ('__fpu_xmm10', __darwin_xmm_reg),
    ('__fpu_xmm11', __darwin_xmm_reg),
    ('__fpu_xmm12', __darwin_xmm_reg),
    ('__fpu_xmm13', __darwin_xmm_reg),
    ('__fpu_xmm14', __darwin_xmm_reg),
    ('__fpu_xmm15', __darwin_xmm_reg),
    ('__fpu_rsrv4', c_char * 96),
    ('__fpu_reserved1', c_int),
]
__darwin_mcontext64._fields_ = [
    ('__es', __darwin_x86_exception_state64),
    ('__ss', __darwin_x86_thread_state64),
    ('__fs', __darwin_x86_float_state64),
]
mcontext_t = POINTER(__darwin_mcontext32)
__int8_t = c_byte
__int16_t = c_short
__int32_t = c_int
__int64_t = c_longlong
__darwin_intptr_t = c_long
__darwin_natural_t = c_uint
__darwin_ct_rune_t = c_int
class __mbstate_t(Union):
    pass
__mbstate_t._pack_ = 4
__mbstate_t._fields_ = [
    ('__mbstate8', c_char * 128),
    ('_mbstateL', c_longlong),
]
__darwin_mbstate_t = __mbstate_t
__darwin_ptrdiff_t = c_int
__darwin_va_list = STRING
__darwin_wchar_t = c_int
__darwin_rune_t = __darwin_wchar_t
__darwin_wint_t = c_int
__darwin_clock_t = c_ulong
__darwin_socklen_t = __uint32_t
__darwin_ssize_t = c_long
__darwin_time_t = c_long
sig_atomic_t = c_int
int8_t = c_byte
u_int8_t = c_ubyte
int16_t = c_short
u_int16_t = c_ushort
int32_t = c_int
u_int32_t = c_uint
int64_t = c_longlong
u_int64_t = c_ulonglong
register_t = int32_t
uintptr_t = c_ulong
user_addr_t = u_int64_t
user_size_t = u_int64_t
user_ssize_t = int64_t
user_long_t = int64_t
user_ulong_t = u_int64_t
user_time_t = int64_t
syscall_arg_t = u_int64_t
__darwin_fp_control_t = __darwin_fp_control
__darwin_fp_status_t = __darwin_fp_status
class __darwin_x86_debug_state32(Structure):
    pass
__darwin_x86_debug_state32._fields_ = [
    ('__dr0', c_uint),
    ('__dr1', c_uint),
    ('__dr2', c_uint),
    ('__dr3', c_uint),
    ('__dr4', c_uint),
    ('__dr5', c_uint),
    ('__dr6', c_uint),
    ('__dr7', c_uint),
]
class __darwin_x86_debug_state64(Structure):
    pass
__darwin_x86_debug_state64._pack_ = 4
__darwin_x86_debug_state64._fields_ = [
    ('__dr0', __uint64_t),
    ('__dr1', __uint64_t),
    ('__dr2', __uint64_t),
    ('__dr3', __uint64_t),
    ('__dr4', __uint64_t),
    ('__dr5', __uint64_t),
    ('__dr6', __uint64_t),
    ('__dr7', __uint64_t),
]
ct_rune_t = __darwin_ct_rune_t
rune_t = __darwin_rune_t
wint_t = __darwin_wint_t
class _RuneEntry(Structure):
    pass
_RuneEntry._fields_ = [
    ('__min', __darwin_rune_t),
    ('__max', __darwin_rune_t),
    ('__map', __darwin_rune_t),
    ('__types', POINTER(__uint32_t)),
]
class _RuneRange(Structure):
    pass
_RuneRange._fields_ = [
    ('__nranges', c_int),
    ('__ranges', POINTER(_RuneEntry)),
]
class _RuneCharClass(Structure):
    pass
_RuneCharClass._fields_ = [
    ('__name', c_char * 14),
    ('__mask', __uint32_t),
]
class _RuneLocale(Structure):
    pass
_RuneLocale._fields_ = [
    ('__magic', c_char * 8),
    ('__encoding', c_char * 32),
    ('__sgetrune', CFUNCTYPE(__darwin_rune_t, STRING, __darwin_size_t, POINTER(STRING))),
    ('__sputrune', CFUNCTYPE(c_int, __darwin_rune_t, STRING, __darwin_size_t, POINTER(STRING))),
    ('__invalid_rune', __darwin_rune_t),
    ('__runetype', __uint32_t * 256),
    ('__maplower', __darwin_rune_t * 256),
    ('__mapupper', __darwin_rune_t * 256),
    ('__runetype_ext', _RuneRange),
    ('__maplower_ext', _RuneRange),
    ('__mapupper_ext', _RuneRange),
    ('__variable', c_void_p),
    ('__variable_len', c_int),
    ('__ncharclasses', c_int),
    ('__charclasses', POINTER(_RuneCharClass)),
]
class _opaque_pthread_t(Structure):
    pass
__darwin_pthread_t = POINTER(_opaque_pthread_t)
pthread_t = __darwin_pthread_t
va_list = __darwin_va_list
__darwin_off_t = __int64_t
off_t = __darwin_off_t
fpos_t = __darwin_off_t
class __sbuf(Structure):
    pass
__sbuf._fields_ = [
    ('_base', POINTER(c_ubyte)),
    ('_size', c_int),
]
class __sFILEX(Structure):
    pass
__sFILEX._fields_ = [
]
class __sFILE(Structure):
    pass
__sFILE._pack_ = 4
__sFILE._fields_ = [
    ('_p', POINTER(c_ubyte)),
    ('_r', c_int),
    ('_w', c_int),
    ('_flags', c_short),
    ('_file', c_short),
    ('_bf', __sbuf),
    ('_lbfsize', c_int),
    ('_cookie', c_void_p),
    ('_close', CFUNCTYPE(c_int, c_void_p)),
    ('_read', CFUNCTYPE(c_int, c_void_p, STRING, c_int)),
    ('_seek', CFUNCTYPE(fpos_t, c_void_p, fpos_t, c_int)),
    ('_write', CFUNCTYPE(c_int, c_void_p, STRING, c_int)),
    ('_ub', __sbuf),
    ('_extra', POINTER(__sFILEX)),
    ('_ur', c_int),
    ('_ubuf', c_ubyte * 3),
    ('_nbuf', c_ubyte * 1),
    ('_lb', __sbuf),
    ('_blksize', c_int),
    ('_offset', fpos_t),
]
FILE = __sFILE
class div_t(Structure):
    pass
div_t._fields_ = [
    ('quot', c_int),
    ('rem', c_int),
]
class ldiv_t(Structure):
    pass
ldiv_t._fields_ = [
    ('quot', c_long),
    ('rem', c_long),
]
class lldiv_t(Structure):
    pass
lldiv_t._pack_ = 4
lldiv_t._fields_ = [
    ('quot', c_longlong),
    ('rem', c_longlong),
]
class __darwin_sigaltstack(Structure):
    pass
__darwin_sigaltstack._fields_ = [
    ('ss_sp', c_void_p),
    ('ss_size', __darwin_size_t),
    ('ss_flags', c_int),
]
class timespec(Structure):
    pass
timespec._fields_ = [
    ('tv_sec', __darwin_time_t),
    ('tv_nsec', c_long),
]
class timeval(Structure):
    pass
__darwin_suseconds_t = __int32_t
timeval._fields_ = [
    ('tv_sec', __darwin_time_t),
    ('tv_usec', __darwin_suseconds_t),
]
class __darwin_ucontext(Structure):
    pass
__darwin_sigset_t = __uint32_t
__darwin_ucontext._fields_ = [
    ('uc_onstack', c_int),
    ('uc_sigmask', __darwin_sigset_t),
    ('uc_stack', __darwin_sigaltstack),
    ('uc_link', POINTER(__darwin_ucontext)),
    ('uc_mcsize', __darwin_size_t),
    ('uc_mcontext', POINTER(__darwin_mcontext32)),
]
class fd_set(Structure):
    pass
fd_set._fields_ = [
    ('fds_bits', __int32_t * 32),
]
stack_t = __darwin_sigaltstack
ucontext_t = __darwin_ucontext
class __darwin_pthread_handler_rec(Structure):
    pass
__darwin_pthread_handler_rec._fields_ = [
    ('__routine', CFUNCTYPE(None, c_void_p)),
    ('__arg', c_void_p),
    ('__next', POINTER(__darwin_pthread_handler_rec)),
]
class _opaque_pthread_attr_t(Structure):
    pass
_opaque_pthread_attr_t._fields_ = [
    ('__sig', c_long),
    ('__opaque', c_char * 36),
]
class _opaque_pthread_cond_t(Structure):
    pass
_opaque_pthread_cond_t._fields_ = [
    ('__sig', c_long),
    ('__opaque', c_char * 24),
]
class _opaque_pthread_condattr_t(Structure):
    pass
_opaque_pthread_condattr_t._fields_ = [
    ('__sig', c_long),
    ('__opaque', c_char * 4),
]
class _opaque_pthread_mutex_t(Structure):
    pass
_opaque_pthread_mutex_t._fields_ = [
    ('__sig', c_long),
    ('__opaque', c_char * 40),
]
class _opaque_pthread_mutexattr_t(Structure):
    pass
_opaque_pthread_mutexattr_t._fields_ = [
    ('__sig', c_long),
    ('__opaque', c_char * 8),
]
class _opaque_pthread_once_t(Structure):
    pass
_opaque_pthread_once_t._fields_ = [
    ('__sig', c_long),
    ('__opaque', c_char * 4),
]
class _opaque_pthread_rwlock_t(Structure):
    pass
_opaque_pthread_rwlock_t._fields_ = [
    ('__sig', c_long),
    ('__opaque', c_char * 124),
]
class _opaque_pthread_rwlockattr_t(Structure):
    pass
_opaque_pthread_rwlockattr_t._fields_ = [
    ('__sig', c_long),
    ('__opaque', c_char * 12),
]
_opaque_pthread_t._fields_ = [
    ('__sig', c_long),
    ('__cleanup_stack', POINTER(__darwin_pthread_handler_rec)),
    ('__opaque', c_char * 596),
]
__darwin_blkcnt_t = __int64_t
__darwin_blksize_t = __int32_t
__darwin_dev_t = __int32_t
__darwin_fsblkcnt_t = c_uint
__darwin_fsfilcnt_t = c_uint
__darwin_gid_t = __uint32_t
__darwin_id_t = __uint32_t
__darwin_ino64_t = __uint64_t
__darwin_ino_t = __uint32_t
__darwin_mach_port_name_t = __darwin_natural_t
__darwin_mach_port_t = __darwin_mach_port_name_t
__darwin_mode_t = __uint16_t
__darwin_pid_t = __int32_t
__darwin_pthread_attr_t = _opaque_pthread_attr_t
__darwin_pthread_cond_t = _opaque_pthread_cond_t
__darwin_pthread_condattr_t = _opaque_pthread_condattr_t
__darwin_pthread_key_t = c_ulong
__darwin_pthread_mutex_t = _opaque_pthread_mutex_t
__darwin_pthread_mutexattr_t = _opaque_pthread_mutexattr_t
__darwin_pthread_once_t = _opaque_pthread_once_t
__darwin_pthread_rwlock_t = _opaque_pthread_rwlock_t
__darwin_pthread_rwlockattr_t = _opaque_pthread_rwlockattr_t
__darwin_uid_t = __uint32_t
__darwin_useconds_t = __uint32_t
__darwin_uuid_t = c_ubyte * 16
rlim_t = __uint64_t
class rusage(Structure):
    pass
rusage._fields_ = [
    ('ru_utime', timeval),
    ('ru_stime', timeval),
    ('ru_maxrss', c_long),
    ('ru_ixrss', c_long),
    ('ru_idrss', c_long),
    ('ru_isrss', c_long),
    ('ru_minflt', c_long),
    ('ru_majflt', c_long),
    ('ru_nswap', c_long),
    ('ru_inblock', c_long),
    ('ru_oublock', c_long),
    ('ru_msgsnd', c_long),
    ('ru_msgrcv', c_long),
    ('ru_nsignals', c_long),
    ('ru_nvcsw', c_long),
    ('ru_nivcsw', c_long),
]
class rlimit(Structure):
    pass
rlimit._pack_ = 4
rlimit._fields_ = [
    ('rlim_cur', rlim_t),
    ('rlim_max', rlim_t),
]
time_t = __darwin_time_t
suseconds_t = __darwin_suseconds_t
sigset_t = __darwin_sigset_t
pthread_attr_t = __darwin_pthread_attr_t
class sigval(Union):
    pass
sigval._fields_ = [
    ('sival_int', c_int),
    ('sival_ptr', c_void_p),
]
class sigevent(Structure):
    pass
sigevent._fields_ = [
    ('sigev_notify', c_int),
    ('sigev_signo', c_int),
    ('sigev_value', sigval),
    ('sigev_notify_function', CFUNCTYPE(None, sigval)),
    ('sigev_notify_attributes', POINTER(pthread_attr_t)),
]
class __siginfo(Structure):
    pass
pid_t = __darwin_pid_t
uid_t = __darwin_uid_t
__siginfo._fields_ = [
    ('si_signo', c_int),
    ('si_errno', c_int),
    ('si_code', c_int),
    ('si_pid', pid_t),
    ('si_uid', uid_t),
    ('si_status', c_int),
    ('si_addr', c_void_p),
    ('si_value', sigval),
    ('si_band', c_long),
    ('__pad', c_ulong * 7),
]
siginfo_t = __siginfo
class __sigaction_u(Union):
    pass
__sigaction_u._fields_ = [
    ('__sa_handler', CFUNCTYPE(None, c_int)),
    ('__sa_sigaction', CFUNCTYPE(None, c_int, POINTER(__siginfo), c_void_p)),
]
class __sigaction(Structure):
    pass
__sigaction._fields_ = [
    ('__sigaction_u', __sigaction_u),
    ('sa_tramp', CFUNCTYPE(None, c_void_p, c_int, c_int, POINTER(siginfo_t), c_void_p)),
    ('sa_mask', sigset_t),
    ('sa_flags', c_int),
]
class sigaction(Structure):
    pass
sigaction._fields_ = [
    ('__sigaction_u', __sigaction_u),
    ('sa_mask', sigset_t),
    ('sa_flags', c_int),
]
sig_t = CFUNCTYPE(None, c_int)
class sigvec(Structure):
    pass
sigvec._fields_ = [
    ('sv_handler', CFUNCTYPE(None, c_int)),
    ('sv_mask', c_int),
    ('sv_flags', c_int),
]
class sigstack(Structure):
    pass
sigstack._fields_ = [
    ('ss_sp', STRING),
    ('ss_onstack', c_int),
]
class accessx_descriptor(Structure):
    pass
accessx_descriptor._fields_ = [
    ('ad_name_offset', c_uint),
    ('ad_flags', c_int),
    ('ad_pad', c_int * 2),
]

# values for enumeration 'idtype_t'
idtype_t = c_int # enum
id_t = __darwin_id_t
class wait(Union):
    pass
class N4wait3DOLLAR_9E(Structure):
    pass
N4wait3DOLLAR_9E._fields_ = [
    ('w_Termsig', c_uint, 7),
    ('w_Coredump', c_uint, 1),
    ('w_Retcode', c_uint, 8),
    ('w_Filler', c_uint, 16),
]
class N4wait4DOLLAR_10E(Structure):
    pass
N4wait4DOLLAR_10E._fields_ = [
    ('w_Stopval', c_uint, 8),
    ('w_Stopsig', c_uint, 8),
    ('w_Filler', c_uint, 16),
]
wait._fields_ = [
    ('w_status', c_int),
    ('w_T', N4wait3DOLLAR_9E),
    ('w_S', N4wait4DOLLAR_10E),
]
dev_t = __darwin_dev_t
gid_t = __darwin_gid_t
intptr_t = __darwin_intptr_t
mode_t = __darwin_mode_t
ssize_t = __darwin_ssize_t
useconds_t = __darwin_useconds_t
uuid_t = __darwin_uuid_t
__all__ = ['__uint16_t', 'MapiDate', '__int16_t',
           '__darwin_sigaltstack', '__darwin_pthread_key_t',
           '__darwin_id_t', '_RuneRange', '__darwin_time_t',
           '__darwin_nl_item', '_opaque_pthread_condattr_t', 'FILE',
           'size_t', 'timeval', 'BlockCache', '__uint32_t',
           'accessx_descriptor', '__darwin_pthread_condattr_t',
           'fpos_t', 'P_PGID', '__darwin_gid_t', '__darwin_dev_t',
           'time_t', '__darwin_mach_port_name_t',
           '__darwin_fsfilcnt_t', 'intptr_t',
           '__darwin_pthread_mutex_t', 'user_addr_t',
           '__darwin_pthread_t', 'uid_t', '__darwin_mmst_reg',
           'u_int64_t', 'u_int16_t', '__darwin_ino_t', 'register_t',
           '__darwin_ssize_t', '__darwin_sigset_t', 'ct_rune_t',
           '__darwin_ptrdiff_t', '_RuneEntry', 'wait', 'va_list',
           'sigset_t', '__int32_t', 'bstream',
           '__darwin_x86_exception_state64', 'Q_TABLE',
           '__darwin_intptr_t', '__uint64_t', '__darwin_fp_status_t',
           'mode_t', 'timespec', '__darwin_suseconds_t',
           '__sigaction', 'sigevent', 'user_ulong_t', 'user_ssize_t',
           'syscall_arg_t', '_RuneCharClass', 'int16_t',
           '__darwin_socklen_t', 'mcontext_t', 'rune_t',
           '__darwin_va_list', '__darwin_mcontext64', 'siginfo_t',
           'Q_TRANS', '__sbuf', '__darwin_rune_t', 'mapi_int64',
           'OPEN_ERROR', 'id_t', '__darwin_blksize_t', 'ldiv_t',
           '__darwin_xmm_reg', '__darwin_wctrans_t', 'sql_query_t',
           'N4wait4DOLLAR_10E', 'u_int32_t', 'MapiHdl',
           '__darwin_wchar_t', 'sigval', 'N4wait3DOLLAR_9E', 'P_PID',
           'sigaction', '__darwin_natural_t', 'Q_UPDATE',
           '__darwin_blkcnt_t', '__darwin_fp_status',
           '_opaque_pthread_cond_t', 'MapiStatement',
           '__darwin_ct_rune_t', 'pthread_t',
           '_opaque_pthread_mutexattr_t', 'pthread_attr_t', 'fd_set',
           '__darwin_useconds_t', '__darwin_clock_t',
           '__darwin_fp_control_t', 'N10MapiRowBuf4DOLLAR_14E',
           'MapiResultSet', '__darwin_pthread_handler_rec', 'int32_t',
           'Mapi', 'wint_t', 'rlim_t', 'MapiMsg',
           '__darwin_fsblkcnt_t', 'Q_PREPARE', 'uuid_t',
           '_opaque_pthread_rwlockattr_t', 'sigvec',
           '__darwin_x86_thread_state64', '__darwin_pthread_rwlock_t',
           'rlimit', '__darwin_pthread_mutexattr_t',
           '__darwin_pthread_once_t', 'stack_t', '__darwin_mode_t',
           'sig_t', '__mbstate_t', 'MapiStruct',
           '__darwin_mach_port_t', '__darwin_x86_float_state64',
           '__uint8_t', '__darwin_uid_t', '__int8_t', 'Q_SCHEMA',
           'int8_t', '__darwin_uuid_t', '_opaque_pthread_attr_t',
           '_opaque_pthread_once_t', 'off_t', 'gid_t', 'sigstack',
           'div_t', 'pid_t', '__darwin_size_t',
           '__darwin_x86_debug_state64', '__siginfo',
           '__darwin_mbstate_t', 'useconds_t', 'WRITE_ERROR',
           'mapi_uint64', '__darwin_ino64_t', 'u_int8_t',
           '_opaque_pthread_t', 'stream', 'int64_t',
           '__darwin_wctype_t', 'uintptr_t', 'MapiRowBuf',
           'READ_ERROR', '__darwin_ucontext', 'stream_errors',
           '__sFILE', '__darwin_fp_control', 'sig_atomic_t',
           '__darwin_pid_t', 'lldiv_t',
           '__darwin_pthread_rwlockattr_t', 'MapiColumn',
           '__darwin_i386_float_state', 'Q_BLOCK', '__sFILEX',
           'MapiDateTime', '__darwin_i386_exception_state',
           '_RuneLocale', 'buffer', '__darwin_pthread_attr_t',
           '_opaque_pthread_mutex_t', 'user_time_t', 'ssize_t',
           '__darwin_wint_t', '__darwin_pthread_cond_t',
           'user_size_t', '__darwin_x86_debug_state32', 'rusage',
           'ucontext_t', 'idtype_t', 'MapiBinding', '__sigaction_u',
           '_opaque_pthread_rwlock_t', '__darwin_off_t',
           '__darwin_i386_thread_state', 'MapiTime', 'P_ALL',
           '__int64_t', '__darwin_mcontext32', 'Q_PARSE', 'MapiParam',
           'user_long_t', 'dev_t', 'NO__ERROR', 'suseconds_t']
