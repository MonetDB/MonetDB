// The contents of this file are subject to the MonetDB Public License
// Version 1.1 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
// http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
//
// Software distributed under the License is distributed on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
// License for the specific language governing rights and limitations
// under the License.
//
// The Original Code is the MonetDB Database System.
//
// The Initial Developer of the Original Code is CWI.
// Portions created by CWI are Copyright (C) 1997-2006 CWI.
// All Rights Reserved.

#include <windows.h>
#include <iostream>
#include "service.h"

using std::cout;
using std::endl;

static void
error(const char *msg)
{
	cout << "Error: " << msg << endl;
	exit(1);
}

static void
install(int argc, char **argv, const char *user, const char *passwd)
{
	SC_HANDLE handle;
	SERVICE_DESCRIPTION descr;
	char server[_MAX_PATH];

	handle = ::OpenSCManager(NULL, NULL, SC_MANAGER_CREATE_SERVICE);
	if (handle == NULL)
		error("Failed to open Service Control Manager");

	SC_HANDLE service;

	// name must be quoted if it contains spaces!
	if (((('a' <= argv[0][0] && argv[0][0] <= 'z') ||
	      ('A' <= argv[0][0] && argv[0][0] <= 'Z')) &&
	     argv[0][1] == ':' &&
	     (argv[0][2] == '\\' || argv[0][2] == '/')) ||
	    (argv[0][0] == '\\' && argv[0][1] == '\\')) {
		// absolute path name: either driveletter ":" \path,
		// driveletter ":" /path, or \\path
		// find last / or \ (i.e. end of folder name
		char *base, *p;
		for (p = argv[0]; *p; p++)
			if (*p == '\\' || *p == '/')
				base = p;
		_snprintf(server, sizeof(server), "\"%.*s\\%s\"",
			  base - argv[0], argv[0], MSERVICEEXE);
	} else {
		_fullpath(server, MSERVICEEXE, sizeof(server));
	}

	for (int i = 2; i < argc; i++) {
		if (strlen(server) + strlen(argv[i]) + 4 >= sizeof(server))
			error("argument list too long");
		strcat(server, " \"");
		strcat(server, argv[i]);
		strcat(server, "\"");
	}

	service = ::CreateService(handle,
				  MSERVICE,
				  "MonetDB Database Server",
				  GENERIC_ALL,
				  SERVICE_WIN32_OWN_PROCESS,
				  SERVICE_AUTO_START,
				  SERVICE_ERROR_NORMAL,
				  server,
				  NULL,
				  NULL,
				  NULL,
				  user,
				  passwd);
	if (service == NULL)
		error("Failed to create the MonetDB Database Service");
	char *buf = (char *) malloc(strlen(server) + 100);
	strcpy(buf, "Manages a MonetDB Database Server.\n"
	       "The server is started with the following command:\n");
	for (int i = 2; i < argc; i++) {
		strcat(buf, argv[i]);
		strcat(buf, "\n");
	}
	descr.lpDescription = buf;
	if (!::ChangeServiceConfig2(service, SERVICE_CONFIG_DESCRIPTION, &descr))
		error("Failed to change service description");

	free(buf);
	::CloseServiceHandle(service);
	::CloseServiceHandle(handle);
}

static void
uninstall(void)
{
	SC_HANDLE handle;

	handle = ::OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);
	if (handle == NULL)
		error("Failed to open Service Control Manager");

	SC_HANDLE service;

	service = ::OpenService(handle, MSERVICE, DELETE);
	if (service != NULL) {
		if (!::DeleteService(service))
			error("Failed to delete the MonetDB Database Service");
		::CloseServiceHandle(service);
	}
	::CloseServiceHandle(handle);
}

static void
start(void)
{
	SC_HANDLE handle;

	handle = ::OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);
	if (handle == NULL)
		error("Failed to open Service Control Manager");

	SC_HANDLE service;

	service = ::OpenService(handle, MSERVICE, GENERIC_EXECUTE);
	if (service == NULL)
		error("Cannot get handle to service");
	if (!::StartService(service, 0, NULL))
		error("Failed to start the MonetDB Database Service");
	::CloseServiceHandle(service);
	::CloseServiceHandle(handle);
}

static void
stop(void)
{
	SC_HANDLE handle;
	SERVICE_STATUS status;

	handle = ::OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);
	if (handle == NULL)
		error("Failed to open Service Control Manager");

	SC_HANDLE service;

	service = ::OpenService(handle, MSERVICE, GENERIC_EXECUTE);
	if (service == NULL)
		error("Cannot get handle to service");
	if (!::ControlService(service, SERVICE_CONTROL_STOP, &status))
		error("Failed to stop the MonetDB Database Service");
	::CloseServiceHandle(service);
	::CloseServiceHandle(handle);
}

void
usage(char *progname)
{
	cout << "Usage: " << progname << " {/Install,/Uninstall,/Start,/Stop}" << endl;
	exit(1);
}

int
main(int argc, char **argv)
{
	if (argc < 2)
		usage(argv[0]);

	if (strcmp(argv[1], "-install") == 0 ||
	    strcmp(argv[1], "/Install") == 0) {
		install(argc, argv, NULL, NULL);
	} else if (argc == 2 &&
		   (strcmp(argv[1], "-uninstall") == 0 ||
		    strcmp(argv[1], "/Uninstall") == 0)) {
		uninstall();
	} else if (argc == 2 &&
		   (strcmp(argv[1], "-start") == 0 ||
		    strcmp(argv[1], "/Start") == 0)) {
		start();
	} else if (argc == 2 &&
		   (strcmp(argv[1], "-stop") == 0 ||
		    strcmp(argv[1], "/Stop") == 0)) {
		stop();
	} else {
		usage(argv[0]);
	}
	return 0;
}
