#include <windows.h>
#include <iostream>

using std::cout;
using std::endl;

static FILE *mserver;
static char *argv0;
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

	handle = ::RegisterServiceCtrlHandlerEx("MonetDB", ctrlhandler, NULL);
	if (handle == NULL)
		error("Failed to register service control handler");
	mserver = _popen("\"C:\\Program Files\\CWI\\MonetDB\\MSQLserver.bat\"", "w");
	if (mserver == NULL) {
		status.dwCurrentState = SERVICE_STOPPED;
		status.dwWin32ExitCode = ERROR_SERVICE_SPECIFIC_ERROR;
		status.dwServiceSpecificExitCode = 1;
	} else {
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
	SERVICE_TABLE_ENTRY dispatchTable[] = {
		{"MonetDB", dispatch},
		{NULL, NULL}
	};

	argv0 = argv[0];

	if (!::StartServiceCtrlDispatcher(dispatchTable))
		error("Failed to start service");
	return 0;
}
