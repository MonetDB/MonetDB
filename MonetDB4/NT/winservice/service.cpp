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
#include <direct.h>
#include <iostream>
#include "service.h"

using std::cout;
using std::endl;

static char *server;
static char *initialization;
static FILE *mserver;
static SERVICE_STATUS_HANDLE handle;

static void
error(const char *msg)
{
	char buf[1024];

	sprintf(buf, " [MonetDB] Error: %s (%d)\n", msg, GetLastError());
	OutputDebugStringA(buf);
//	cout << "Error: " << msg << endl;
	exit(1);
}

static DWORD WINAPI
ctrlhandler(DWORD control, DWORD eventtype, LPVOID eventdata, LPVOID context)
{
	SERVICE_STATUS status;

	(void) eventtype;
	(void) eventdata;
	(void) context;

	status.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
	status.dwCurrentState = SERVICE_RUNNING;
	status.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
	status.dwWin32ExitCode = 0;
	status.dwServiceSpecificExitCode = 0;
	status.dwCheckPoint = 0;
	status.dwWaitHint = 0;

	switch (control) {
	case SERVICE_CONTROL_SHUTDOWN:
	case SERVICE_CONTROL_STOP:
		// stop Mserver
		fprintf(mserver, "quit();\n");
		fflush(mserver);
		_pclose(mserver);
		mserver = NULL;
		status.dwCurrentState = SERVICE_STOPPED;
		status.dwWin32ExitCode = 0;
		break;
	case SERVICE_CONTROL_INTERROGATE:
		// do a ping
		break;
	default:
		return ERROR_CALL_NOT_IMPLEMENTED;
	}
	::SetServiceStatus(handle, &status);
	return NO_ERROR;
}

static void WINAPI
dispatch(DWORD argc, char **argv)
{
	SERVICE_STATUS status;

	handle = ::RegisterServiceCtrlHandlerEx(MSERVICE, ctrlhandler, NULL);
	if (handle == NULL)
		error("Failed to register service control handler");
// 	FILE *f = fopen("C:\\cmd.log", "w");
// 	fprintf(f, "%s\n", server);
// 	if (initialization)
// 		fprintf(f, "%s\n", initialization);
// 	fclose(f);
	mserver = _popen(server, "w");
	if (mserver == NULL) {
		status.dwCurrentState = SERVICE_STOPPED;
		status.dwWin32ExitCode = ERROR_SERVICE_SPECIFIC_ERROR;
		status.dwServiceSpecificExitCode = 1;
	} else {
		if (initialization) {
			fprintf(mserver, "%s", initialization);
			fflush(mserver);
		}
		status.dwCurrentState = SERVICE_RUNNING;
		status.dwWin32ExitCode = NO_ERROR;
		status.dwServiceSpecificExitCode = 0;
	}

	status.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
	status.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
	status.dwCheckPoint = 0;
	status.dwWaitHint = 0;
	::SetServiceStatus(handle, &status);
}

int
main(int argc, char **argv)
{
	char *cwd;
	char *argv0;		// full path of this program
	SERVICE_TABLE_ENTRY dispatchTable[] = {
		{MSERVICE, dispatch},
		{NULL, NULL}
	};

	if (((('a' <= argv[0][0] && argv[0][0] <= 'z') ||
	      ('A' <= argv[0][0] && argv[0][0] <= 'Z')) &&
	     argv[0][1] == ':' &&
	     (argv[0][2] == '\\' || argv[0][2] == '/')) ||
	    (argv[0][0] == '\\' && argv[0][1] == '\\')) {
		// absolute path name: either driveletter ":" \path,
		// driveletter ":" /path, or \\path
		// find last / or \ (i.e. end of folder name
		argv0 = argv[0];
	} else {
		if ((argv0 = _fullpath(NULL, argv[0], 0)) == NULL)
			error("cannot get full path name of program");
	}

	char *base, *p;
	for (p = argv0; *p; p++)
		if (*p == '\\' || *p == '/')
			base = p;
	// stick folder name in cwd
	cwd = (char *) malloc(base - argv0 + 1);
	strncpy(cwd, argv0, base - argv0);
	cwd[base - argv0] = 0;
	if (argv[0] != argv0)
		free(argv0);

	_chdir(cwd);
	free(cwd);

	server = (char *) malloc(strlen(MSERVER) + 50);
	sprintf(server, "%s --set monet_prompt= > service.log 2>&1", MSERVER);

	size_t len = 0;
	for (int i = 1; i < argc; i++)
		len += strlen(argv[i]) + 1;
	if (len) {
		initialization = (char *) malloc(len + 1);
		*initialization = 0;
		for (int i = 1; i < argc; i++) {
			strcat(initialization, argv[i]);
			strcat(initialization, "\n");
		}
	}
		
	if (!::StartServiceCtrlDispatcher(dispatchTable))
		error("Failed to start service");
	return 0;
}
