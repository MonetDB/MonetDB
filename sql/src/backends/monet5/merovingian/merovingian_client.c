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

static err
handleClient(int sock)
{
	stream *fdin, *fout;
	str buf = alloca(sizeof(char) * 8096);
	char *user = NULL, *algo = NULL, *passwd = NULL, *lang = NULL;
	char *database = NULL, *s;
	char *host = NULL;
	sabdb *top = NULL;
	sabdb *stat = NULL;
	struct sockaddr_in saddr;
	socklen_t saddrlen = sizeof(struct sockaddr_in);
	err e;
	confkeyval *ckv, *kv;
	char mydoproxy;
	sabdb redirs[24];  /* do we need more? */
	int r = 0;

	fdin = socket_rastream(sock, "merovingian<-client (read)");
	if (fdin == 0)
		return(newErr("merovingian-client inputstream problems"));
	fdin = block_stream(fdin);

	fout = socket_wastream(sock, "merovingian->client (write)");
	if (fout == 0) {
		close_stream(fdin);
		return(newErr("merovingian-client outputstream problems"));
	}
	fout = block_stream(fout);

	/* note that we claim to speak proto 8 here */
	stream_printf(fout, "%s:merovingian:8:%s:%s",
			"void",  /* some bs */
			"md5,plain", /* we actually don't look at the password */
#ifdef WORDS_BIGENDIAN
			"BIG"
#else
			"LIT"
#endif
			);
	stream_flush(fout);

	/* get response */
	if (stream_read_block(fdin, buf, 8095, 1) < 0) {
		/* we didn't get a terminated block :/ */
		e = newErr("client sent challenge in incomplete block: %s", buf);
		stream_printf(fout, "!merovingian: client sent something this server could not understand, sorry\n", user);
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}
	buf[8095] = '\0';

	/* decode BIG/LIT:user:{cypher}passwordchal:lang:database: line */

	user = buf;
	/* byte order */
	s = strchr(user, ':');
	if (s) {
		*s = 0;
		/* we don't use this in merovingian */
		/* stream_set_byteorder(fin->s, strcmp(user, "BIG") == 0); */
		user = s + 1;
	} else {
		e = newErr("client challenge error: %s", buf);
		stream_printf(fout, "!merovingian: incomplete challenge '%s'\n", user);
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}

	/* passwd */
	s = strchr(user, ':');
	if (s) {
		*s = 0;
		passwd = s + 1;
		/* decode algorithm, i.e. {plain}mypasswordchallenge */
		if (*passwd != '{') {
			e = newErr("client challenge error: %s", buf);
			stream_printf(fout, "!merovingian: invalid password entry\n");
			stream_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			return(e);
		}
		algo = passwd + 1;
		s = strchr(algo, '}');
		if (!s) {
			e = newErr("client challenge error: %s", buf);
			stream_printf(fout, "!merovingian: invalid password entry\n");
			stream_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			return(e);
		}
		*s = 0;
		passwd = s + 1;
	} else {
		e = newErr("client challenge error: %s", buf);
		stream_printf(fout, "!merovingian: incomplete challenge '%s'\n", user);
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}

	/* lang */
	s = strchr(passwd, ':');
	if (s) {
		*s = 0;
		lang = s + 1;
	} else {
		e = newErr("client challenge error: %s", buf);
		stream_printf(fout, "!merovingian: incomplete challenge, missing language\n");
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}

	/* database */
	s = strchr(lang, ':');
	if (s) {
		*s = 0;
		database = s + 1;
		/* since we don't know where the string ends, we need to look
		 * for another : */
		s = strchr(database, ':');
		if (s == NULL) {
			e = newErr("client challenge error: %s", buf);
			stream_printf(fout, "!merovingian: incomplete challenge, missing trailing colon\n");
			stream_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			return(e);
		} else {
			*s = '\0';
		}
	}

	if (*database == '\0') {
		/* we need to have a database, if we haven't gotten one,
		 * complain */
		stream_printf(fout, "!merovingian: please specify a database\n");
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(newErr("no database specified"));
	} else {
		if (strcmp(lang, "resolve") == 0) {
			/* ensure the pattern ends with '/\*' such that we force a
			 * remote entry, including those for local databases, this
			 * way we will get a redirect back to merovingian for such
			 * database if it is proxied and hence not remotely
			 * available */
			size_t len = strlen(database);
			if (len > 2 &&
					database[len - 2] != '/' &&
					database[len - 1] != '*')
			{
				char *n = alloca(sizeof(char) * len + 2 + 1);
				snprintf(n, len + 2 + 1, "%s/*", database);
				database = n;
			}
		}
		if ((e = forkMserver(database, &top, 0)) != NO_ERR) {
			if (top == NULL) {
				stream_printf(fout, "!merovingian: no such database '%s', please create it first\n", database);
			} else {
				stream_printf(fout, "!merovingian: internal error while starting mserver, please refer to the logs\n");
			}
			stream_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			return(e);
		}
		stat = top;
	}

	/* collect possible redirects */
	for (stat = top; stat != NULL; stat = stat->next) {
		if (stat->conns == NULL || stat->conns->val == NULL) {
			Mfprintf(stdout, "dropping database without available "
					"connections: '%s'\n", stat->dbname);
		} else if (r == 24) {
			Mfprintf(stdout, "dropping database connection because of "
					"too many already: %s\n", stat->conns->val);
		} else {
			redirs[r++] = *stat;
		}
	}

	/* if we can't redirect, our mission ends here */
	if (r == 0) {
		e = newErr("there are no available connections for '%s'", database);
		stream_printf(fout, "!merovingian: %s\n", e);
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		SABAOTHfreeStatus(&top);
		return(e);
	}

	if (getpeername(sock, (struct sockaddr *)&saddr, &saddrlen) == -1) {
		Mfprintf(stderr, "couldn't get peername of client: %s\n",
				strerror(errno));
		host = "(unknown)";
	} else {
		size_t len;
		struct hostent *hoste = 
			gethostbyaddr(&saddr.sin_addr.s_addr, 4, saddr.sin_family);
		if (hoste == NULL) {
			len = (3 + 1 + 3 + 1 + 3 + 1 + 3 + 1 + 5) + 1;
			host = alloca(sizeof(char) * len);
			snprintf(host, len, "%u.%u.%u.%u:%u",
					(unsigned) ((ntohl(saddr.sin_addr.s_addr) >> 24) & 0xff),
					(unsigned) ((ntohl(saddr.sin_addr.s_addr) >> 16) & 0xff),
					(unsigned) ((ntohl(saddr.sin_addr.s_addr) >> 8) & 0xff),
					(unsigned) (ntohl(saddr.sin_addr.s_addr) & 0xff),
					(unsigned) (ntohs(saddr.sin_port)));
		} else {
			len = strlen(hoste->h_name) + 1 + 5 + 1;
			host = alloca(sizeof(char) * len);
			snprintf(host, len, "%s:%u",
					hoste->h_name, (unsigned) (ntohs(saddr.sin_port)));
		}
	}

	/* need to send a response, either we are going to proxy, or we send
	 * a redirect, if we have multiple options, a redirect is our only
	 * option, but if the redir is a single remote we need to stick to
	 * our default, there is a special case when the client indicates it
	 * is only resolving a pattern, in which we always need to send
	 * redirects, even if it's one */
	mydoproxy = 0;
	if (r == 1 && strcmp(lang, "resolve") != 0) {
		if (redirs[0].dbname != redirs[0].path) {
			/* this is a real local database (not a remote) */
			ckv = getDefaultProps();
			readProps(ckv, redirs[0].path);
			kv = findConfKey(ckv, "forward");
		} else {
			ckv = NULL;
			kv = NULL;
		}
		if (kv == NULL || kv->val == NULL)
			kv = findConfKey(_mero_props, "forward");
		mydoproxy = strcmp(kv->val, "proxy") == 0;
		if (ckv != NULL) {
			freeConfFile(ckv);
			GDKfree(ckv);
		}
	}

	if (mydoproxy == 0) {
		fprintf(stdout, "redirecting client %s for database '%s' to",
				host, database);
		/* client is in control, send all redirects */
		while (--r >= 0) {
			fprintf(stdout, " %s%s",
					redirs[r].conns->val, redirs[r].dbname);
			stream_printf(fout, "^%s%s\n",
					redirs[r].conns->val, redirs[r].dbname);
		}
		/* flush redirect */
		fprintf(stdout, "\n");
		fflush(stdout);
		stream_flush(fout);
	} else {
		Mfprintf(stdout, "proxying client %s for database '%s' to "
				"%s?database=%s\n",
				host, database, redirs[0].conns->val, redirs[0].dbname);
		/* merovingian is in control, only consider the first redirect */
		stream_printf(fout, "^mapi:merovingian://proxy?database=%s\n",
				redirs[0].dbname);
		/* flush redirect */
		stream_flush(fout);

		/* wait for input, or disconnect in a proxy runner */
		if ((e = startProxy(fdin, fout,
						redirs[0].conns->val, host)) != NO_ERR)
		{
			/* we need to let the client login in order not to violate
			 * the protocol */
			stream_printf(fout, "void:merovingian:8:plain:BIG");
			stream_flush(fout);
			stream_read_block(fdin, buf, 8095, 1); /* eat away client response */
			stream_printf(fout, "!merovingian: an internal error has occurred, refer to the logs for details, please try again later\n");
			stream_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			Mfprintf(stdout, "starting a proxy failed: %s\n", e);
			SABAOTHfreeStatus(&top);
			return(e);
		};
	}

	SABAOTHfreeStatus(&top);
	return(NO_ERR);
}

static str
acceptConnections(int sock)
{
	str msg;
	int retval;
	fd_set fds;
	int msgsock;
	err e;

	do {
		/* handle socket connections */
		FD_ZERO(&fds);
		FD_SET(sock, &fds);

		retval = select(sock + 1, &fds, NULL, NULL, NULL);
		if (retval == 0) {
			/* nothing interesting has happened */
			continue;
		}
		if (retval < 0) {
			if (_mero_keep_listening == 0)
				break;
			if (errno != EINTR) {
				msg = strerror(errno);
				goto error;
			}
			continue;
		}
		if (FD_ISSET(sock, &fds)) {
			if ((msgsock = accept(sock, (SOCKPTR) 0, (socklen_t *) 0)) < 0) {
				if (_mero_keep_listening == 0)
					break;
				if (errno != EINTR) {
					msg = strerror(errno);
					goto error;
				}
				continue;
			}
		} else
			continue;
		e = handleClient(msgsock);
		if (e != NO_ERR) {
			Mfprintf(stderr, "client error: %s\n", getErrMsg(e));
			freeErr(e);
		}
	} while (_mero_keep_listening);
	shutdown(sock, SHUT_RDWR);
	close(sock);
	return(NO_ERR);

error:
	_mero_keep_listening = 0;
	shutdown(sock, SHUT_RDWR);
	close(sock);
	return(newErr("accept connection: %s", msg));
}

