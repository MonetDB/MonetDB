/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2010 MonetDB B.V.
 * All Rights Reserved.
 */

typedef struct _merovingian_proxy {
	stream *in;      /* the input to read from and to dispatch to out */
	stream *out;     /* where to write the read input to */
	stream *co_in;   /* the input stream of the co-thread,
	                    don't read from this stream!  close only */
	stream *co_out;  /* the output stream of the co-thread,
	                    don't write to this stream!  close only */
	char *name;      /* a description to log when this thread ends */
	pthread_t co_thr;/* the other proxyThread */
} merovingian_proxy;

static void
proxyThread(void *d)
{
	merovingian_proxy *p = (merovingian_proxy *)d;
	int len;
	char data[8 * 1024];

	/* pass everything from in to out, until either reading from in, or
	 * writing to out fails, then close in and its related out-stream
	 * (not out!) to make sure the co-thread dies as well */
	while ((len = stream_read(p->in, data, 1, sizeof(data))) >= 0) {
		if (len > 0 && stream_write(p->out, data, len, 1) != 1)
			break;
		if (len == 0 &&	stream_flush(p->out) == -1)
			break;
	}

	stream_close(p->co_out);  /* out towards target B */
	stream_close(p->in);      /* related in from target B */

	stream_close(p->out);     /* out towards target A */
	stream_close(p->co_in);   /* related in from target A */

	if (p->name != NULL) {
		/* name is only set on the client-to-server thread */
		if (len <= 0) {
			Mfprintf(stdout, "client %s has disconnected from proxy\n",
					p->name);
		} else {
			Mfprintf(stdout, "server has terminated proxy connection, "
					"disconnecting client %s\n", p->name);
		}
		GDKfree(p->name);

		/* wait for the other thread to finish, after which we can
		 * finally destroy the streams */
		pthread_join(p->co_thr, NULL);
		stream_destroy(p->co_out);
		stream_destroy(p->in);
		stream_destroy(p->out);
		stream_destroy(p->co_in);
	}

	GDKfree(p);
}

static err
startProxy(stream *cfdin, stream *cfout, char *url, char *client)
{
	struct hostent *hp;
	struct sockaddr_in server;
	struct sockaddr *serv;
	socklen_t servsize;
	int ssock = INVALID_SOCKET;
	char *port, *t;
	char *conn;
	struct stat statbuf;
	stream *sfdin, *sfout;
	merovingian_proxy *pctos, *pstoc;
	pthread_t ptid;
	pthread_attr_t detachattr;

	/* quick 'n' dirty parsing */
	if (strncmp(url, "mapi:monetdb://", sizeof("mapi:monetdb://") - 1) == 0) {
		conn = alloca(sizeof(char) * (strlen(url) + 1));
		memcpy(conn, url, strlen(url) + 1);
		conn += sizeof("mapi:monetdb://") - 1;
		/* drop anything off after the hostname */
		if ((port = strchr(conn, ':')) != NULL) {
			*port = '\0';
			port++;
			if ((t = strchr(port, '/')) != NULL)
				*t = '\0';
		} else if (stat(conn, &statbuf) != -1) {
			ssock = 0;
		} else {
			return(newErr("can't find a port in redirect, "
						"or is not a UNIX socket file: %s", url));
		}
	} else {
		return(newErr("unsupported protocol/scheme in redirect: %s", url));
	}

	if (ssock != INVALID_SOCKET) {
		/* UNIX socket connect, don't proxy, but pass socket fd */
		struct sockaddr_un server;
		struct msghdr msg;
		char ccmsg[CMSG_SPACE(sizeof(ssock))];
		struct cmsghdr *cmsg;
		struct iovec vec;
		char buf[1];
		int psock = getSock(cfdin);

		if ((ssock = socket(PF_UNIX, SOCK_STREAM, 0)) < 0)
			return(newErr("cannot open socket: %s", strerror(errno)));
		memset(&server, 0, sizeof(struct sockaddr_un));
		server.sun_family = AF_UNIX;
		strncpy(server.sun_path, conn, sizeof(server.sun_path) - 1);
		if (connect(ssock, (SOCKPTR) &server, sizeof(struct sockaddr_un)))
			return(newErr("cannot connect: %s", strerror(errno)));

		/* send first byte, nothing special to happen */
		msg.msg_name = (struct sockaddr*)&server;
		msg.msg_namelen = sizeof(server);
		*buf = '1'; /* pass fd */
		vec.iov_base = buf;
		vec.iov_len = 1;
		msg.msg_iov = &vec;
		msg.msg_iovlen = 1;
		cmsg = CMSG_FIRSTHDR(&msg);
		cmsg->cmsg_level = SOL_SOCKET;
		cmsg->cmsg_type = SCM_RIGHTS;
		cmsg->cmsg_len = CMSG_LEN(sizeof(psock));
		*(int *)CMSG_DATA(cmsg) = psock;
		msg.msg_control = ccmsg;
		msg.msg_controllen = cmsg->cmsg_len;
		msg.msg_flags = 0;

		Mfprintf(stdout, "target connection is on local UNIX domain socket, "
				"passing on filedescriptor instead of proxying\n");
		if (sendmsg(ssock, &msg, 0) < 0) {
			close(ssock);
			return(newErr("could not send initial byte: %s", strerror(errno)));
		}
		close(psock);
		
		close_stream(cfdin);
		close_stream(cfout);
		return(NO_ERR);
	} else {
		hp = gethostbyname(conn);
		if (hp == NULL)
			return(newErr("cannot get address for hostname '%s': %s",
						conn, strerror(errno)));

		memset(&server, 0, sizeof(server));
		memcpy(&server.sin_addr, hp->h_addr_list[0], hp->h_length);
		server.sin_family = hp->h_addrtype;
		server.sin_port = htons((unsigned short) (atoi(port) & 0xFFFF));
		serv = (struct sockaddr *) &server;
		servsize = sizeof(server);

		ssock = socket(serv->sa_family, SOCK_STREAM, IPPROTO_TCP);
		if (ssock == INVALID_SOCKET)
			return(newErr("cannot open socket: %s", strerror(errno)));

		if (connect(ssock, serv, servsize) < 0)
			return(newErr("cannot connect: %s", strerror(errno)));
	}

	sfdin = block_stream(socket_rastream(ssock, "merovingian<-server (proxy read)"));
	sfout = block_stream(socket_wastream(ssock, "merovingian->server (proxy write)"));

	if (sfdin == 0 || sfout == 0) {
		close_stream(sfout);
		close_stream(sfdin);
		return(newErr("merovingian-server inputstream or outputstream problems"));
	}

	/* our proxy schematically looks like this:
	 *
	 *                  A___>___B
	 *        out     in |     | out     in
	 * client  --------- |  M  | ---------  server
	 *        in     out |_____| in     out
	 *                  C   <   D
	 *
	 * the thread that does A -> B is called ctos, C -> D stoc
	 * the merovingian_proxy structs are filled like:
	 * ctos: in = A, out = B, co_in = D, co_out = C
	 * stoc: in = D, out = C, co_in = A, co_out = B
	 */

	pstoc = GDKmalloc(sizeof(merovingian_proxy));
	pstoc->in     = sfdin;
	pstoc->out    = cfout;
	pstoc->co_in  = cfdin;
	pstoc->co_out = sfout;
	pstoc->name   = NULL;  /* we want only one log-message on disconnect */
	pstoc->co_thr = 0;

	if (pthread_create(&ptid, NULL,
				(void *(*)(void *))proxyThread, (void *)pstoc) < 0)
	{
		close_stream(sfout);
		close_stream(sfdin);
		return(newErr("failed to create proxy thread"));
	}

	pctos = GDKmalloc(sizeof(merovingian_proxy));
	pctos->in     = cfdin;
	pctos->out    = sfout;
	pctos->co_in  = sfdin;
	pctos->co_out = cfout;
	pctos->name   = GDKstrdup(client);
	pctos->co_thr = ptid;

	pthread_attr_init(&detachattr);
	pthread_attr_setdetachstate(&detachattr, PTHREAD_CREATE_DETACHED);
	if (pthread_create(&ptid, &detachattr,
				(void *(*)(void *))proxyThread, (void *)pctos) < 0)
	{
		close_stream(sfout);
		close_stream(sfdin);
		return(newErr("failed to create proxy thread"));
	}

	return(NO_ERR);
}

#ifdef MYSQL_EMULATION_BLEEDING_EDGE_STUFF
static err
handleMySQLClient(int sock)
{
	stream *fdin, *fout;
	str buf = alloca(sizeof(char) * 8096);
	str p;
	int len;

	fdin = socket_rastream(sock, "merovingian<-mysqlclient (read)");
	if (fdin == 0)
		return(newErr("merovingian-mysqlclient inputstream problems"));

	fout = socket_wastream(sock, "merovingian->mysqlclient (write)");
	if (fout == 0) {
		close_stream(fdin);
		return(newErr("merovingian-mysqlclient outputstream problems"));
	}

#ifdef WORDS_BIGENDIAN
#define le_int(P, X) \
	*(P)++ = (unsigned int)X & 255; \
	*(P)++ = ((unsigned int)X >> 8) & 255; \
	*(P)++ = ((unsigned int)X >> 16) & 255; \
	*(P)++ = ((unsigned int)X >> 24) & 255;
#define le_sht(P, X) \
	*(P)++ = (unsigned short)X & 255; \
	*(P)++ = ((unsigned short)X >> 8) & 255;
#else
#define le_int(P, X) \
	*(P)++ = ((unsigned int)X >> 24) & 255; \
	*(P)++ = ((unsigned int)X >> 16) & 255; \
	*(P)++ = ((unsigned int)X >> 8) & 255; \
	*(P)++ = (unsigned int)X & 255;
#define le_sht(P, X) \
	*(P)++ = ((unsigned short)X >> 8) & 255; \
	*(P)++ = (unsigned short)X & 255;
#endif

	/* Handshake Initialization Packet */
	p = buf + 4;   /* skip bytes for package header */
	*p++ = 0x10;   /* protocol_version */
	p += sprintf(p, MERO_VERSION "-merovingian") + 1; /* server_version\0 */
	le_int(p, 0);  /* thread_number */
	p += sprintf(p, "voidvoid"); /* scramble_buff */
	*p++ = 0x00;   /* filler */
	/* server_capabilities:
	 * CLIENT_CONNECT_WITH_DB CLIENT_NO_SCHEMA CLIENT_PROTOCOL_41
	 * CLIENT_INTERACTIVE CLIENT_MULTI_STATEMENTS CLIENT_MULTI_RESULTS
	 */
	le_sht(p, (8 | 16 | 512 | 1024 | 8192 | 65536 | 131072));
	*p++ = 0x33;   /* server_language = utf8_general_ci */
	le_sht(p, 2);  /* server_status = SERVER_STATUS_AUTOCOMMIT */
	p += sprintf(p, "             ");  /* filler 14 bytes */

	/* packet header */
	len = p - buf;
	p = buf;
	le_int(p, len);
	*p = *(p + 1); p++;
	*p = *(p + 1); p++;
	*p = *(p + 1); p++;
	*p = 0x00;   /* packet number */
	stream_flush(fout);

	return(NO_ERR);
}
#endif
