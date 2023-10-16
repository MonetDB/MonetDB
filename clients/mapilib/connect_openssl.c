
#include "monetdb_config.h"

#include "stream.h"		/* include before mapi.h */
#include "stream_socket.h"
#include "mapi.h"
#include "mapi_prompt.h"
#include "mcrypt.h"
#include "matomic.h"
#include "mstring.h"

#include "mapi_intern.h"


MapiMsg
wrap_tls(Mapi mid, SOCKET sock)
{
	closesocket(sock);
	return mapi_setError(mid, "It's a work in progress", __func__, MERROR);
}
