/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f dcsocket
 * @a Romulo Goncalves, Erietta Liarou, Martin Kersten
 * @v 1
 * @+ DataCell Socket handling
 * This code abstracts away the socket handling needed
 * for the Datacell.
 * @-
 * @+ Implementation
 * The implementation is inspired by the tablet module.
 */

#include "monetdb_config.h"
#include "dcsocket.h"

str
socket_server_connect(SOCKET *sfd, int port)
{
	SOCKET sockfd, on = 1;
	struct sockaddr_in serv_addr;
#ifdef WIN32
	int optlen;
#else
	socklen_t optlen;
#endif
	int sock_send_buf = 5000, sock_rec_buf = 8200; /*buffers for receive and send in bytes*/

#ifdef WIN32
	WSADATA wsaData;

	if (WSAStartup(MAKEWORD(1, 1), &wsaData) != 0)
		return GDKstrdup("WSAStartup failed .\n");
#endif

#ifdef _DEBUG_SOCKET_
	stream_printf(DCout, "start the server socket connection on port %d\n", port);
#endif

	*sfd = sockfd = socket(PF_INET, SOCK_STREAM, 0);

	if (sockfd == INVALID_SOCKET)
		return GDKstrdup("Can not create the server socket");

	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on));
	setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF,
		(char *)&sock_send_buf, sizeof(sock_send_buf));
	setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF,
		(char *)&sock_rec_buf, sizeof(sock_rec_buf));

	optlen = sizeof(sock_send_buf);
	getsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, (char *)&sock_send_buf, &optlen);
	assert(optlen == sizeof(sock_send_buf));

	optlen = sizeof(sock_rec_buf);
	getsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, (char *)&sock_rec_buf, &optlen);
	assert(optlen == sizeof(sock_rec_buf));

	memset((char *)&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons(port);

#ifdef _DEBUG_SOCKET_
	stream_printf(DCout, "Bind socket \n");
#endif

	if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
		return GDKstrdup("Can not bind to socket");
	return NULL;
}

str
socket_server_listen(SOCKET sockfd, SOCKET *newsfd)
{
	struct sockaddr_in cli_addr;
#ifdef WIN32
	int clilen = sizeof(cli_addr);
#else
	socklen_t clilen = sizeof(cli_addr);
#endif
	listen(sockfd, 5);
	*newsfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);
	if (*newsfd == INVALID_SOCKET)
		return GDKstrdup("Can not accept on the socket\n");
	return NULL;
}

str
socket_client_connect(SOCKET *sfd, char * host, int port)
{
	struct hostent *he;
	struct sockaddr_in their_addr;

#ifdef WIN32
	WSADATA wsaData;

	if (WSAStartup(MAKEWORD(1, 1), &wsaData) != 0)
		return GDKstrdup("WSAStartup failed .\n");
#endif

	if ((he = gethostbyname(host)) == NULL) {
		return GDKstrdup("Can not find host\n");
	}

#ifdef _DEBUG_SOCKET_
	stream_printf(DCout, "start the client socket connection on port %d\n", port);
#endif

	if ((*sfd = socket(PF_INET, SOCK_STREAM, 0)) == -1) {
		return GDKstrdup("Can not create the socket");
	}

	memset(&their_addr, 0, sizeof(their_addr));
	their_addr.sin_family = AF_INET;
	their_addr.sin_port = htons(port); // short, network byte order
	memcpy(&their_addr.sin_addr, he->h_addr_list[0], he->h_length);

#ifdef _DEBUG_SOCKET_
	stream_printf(DCout, "connect client to host %s on port %d\n", host, port);
#endif

	if (connect(*sfd, (struct sockaddr *)&their_addr, sizeof(struct sockaddr)) < 0) {
		return GDKstrdup("Can not connect to host");
	}
	return NULL;
}


str
socket_close(SOCKET sockfd)
{
#ifdef _DEBUG_SOCKET_
	stream_printf(DCout, "close socket %d\n", sockfd);
#endif
	if (closesocket(sockfd) < 0) {
		return GDKstrdup("Can not close the socket");
	}
	return NULL;
}

