#include <windows.h>
#include <iostream>

using std::cout;
using std::endl;

static void
error(const char *msg)
{
	cout << "Error: " << msg << endl;
	exit(1);
}

static void
install(const char *serverpath, const char *user, const char *passwd)
{
	SC_HANDLE handle;
	SERVICE_DESCRIPTION descr;

	handle = ::OpenSCManager(NULL, NULL, SC_MANAGER_CREATE_SERVICE);
	if (handle == NULL)
		error("Failed to open Service Control Manager");

	SC_HANDLE service;

	service = ::CreateService(handle,
				  "MonetDB",
				  "MonetDB Database Server",
				  GENERIC_ALL,
				  SERVICE_WIN32_OWN_PROCESS,
				  SERVICE_AUTO_START,
				  SERVICE_ERROR_NORMAL,
				  serverpath,
				  NULL,
				  NULL,
				  NULL,
				  user,
				  passwd);
	if (service == NULL)
		error("Failed to create the MonetDB Database Service");
	descr.lpDescription = "Manages a MonetDB Database Server";
	if (!::ChangeServiceConfig2(service, SERVICE_CONFIG_DESCRIPTION, &descr))
		error("Failed to change service description");

	::CloseServiceHandle(service);
	::CloseServiceHandle(handle);
}

static void
uninstall()
{
	SC_HANDLE handle;

	handle = ::OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);
	if (handle == NULL)
		error("Failed to open Service Control Manager");

	SC_HANDLE service;

	service = ::OpenService(handle, "MonetDB", DELETE);
	if (service != NULL) {
		if (!::DeleteService(service))
			error("Failed to delete the MonetDB Database Service");
		::CloseServiceHandle(service);
	}
	::CloseServiceHandle(handle);
}

static void
start()
{
	SC_HANDLE handle;

	handle = ::OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);
	if (handle == NULL)
		error("Failed to open Service Control Manager");

	SC_HANDLE service;

	service = ::OpenService(handle, "MonetDB", GENERIC_EXECUTE);
	if (service == NULL)
		error("Cannot get handle to service");
	if (!::StartService(service, 0, NULL))
		error("Failed to start the MonetDB Database Service");
	::CloseServiceHandle(service);
	::CloseServiceHandle(handle);
}

int
main(int argc, char **argv)
{
	if (argc != 2)
		error("Program requires an argument");

	if (strcmp(argv[1], "-install") == 0) {
		// name must be quoted if it contains spaces!
		install("\"C:\\Program Files\\CWI\\MonetDB\\MserverService.exe\"",
			NULL, NULL);
	} else if (strcmp(argv[1], "-uninstall") == 0)
		uninstall();
	else if (strcmp(argv[1], "-start") == 0)
		start();
	else
		error("bad argument");
	return 0;
}
