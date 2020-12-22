
def parse_mapi_err_msg(error:str=''):
    """Parse error string and returns (err_code, err_msg) tuple
    """
    err_code = None
    err_msg = None
    tmp = error.split('!')
    if len(tmp) > 1:
        try:
            err_code = tmp[0].strip()
        except (ValueError, TypeError):
            pass
        # reconstruct
        err_msg = ('!'.join(tmp[1:])).strip()
    elif len(tmp) == 1:
        if tmp[0]:
            err_msg = tmp[0].strip()
    return err_code, err_msg

