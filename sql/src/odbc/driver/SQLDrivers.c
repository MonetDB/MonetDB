#include "ODBCGlobal.h"
#include "ODBCEnv.h"

SQLRETURN SQL_API
SQLDrivers(SQLHENV EnvironmentHandle, SQLUSMALLINT Direction, SQLCHAR *DriverDescription, SQLSMALLINT BufferLength1, SQLSMALLINT *DescriptionLengthPtr, SQLCHAR *DriverAttributes, SQLSMALLINT BufferLength2, SQLSMALLINT *AttributesLengthPtr)
{
	(void) Direction;
	(void) DriverDescription;
	(void) BufferLength1;
	(void) DescriptionLengthPtr;
	(void) DriverAttributes;
	(void) BufferLength2;
	(void) AttributesLengthPtr;
	addEnvError((ODBCEnv *) EnvironmentHandle, "HY000", "Driver Manager only function", 0);
	return SQL_ERROR;
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLDriversA(SQLHENV EnvironmentHandle, SQLUSMALLINT Direction, SQLCHAR *DriverDescription, SQLSMALLINT BufferLength1, SQLSMALLINT *DescriptionLengthPtr, SQLCHAR *DriverAttributes, SQLSMALLINT BufferLength2, SQLSMALLINT *AttributesLengthPtr)
{
	return SQLDrivers(EnvironmentHandle, Direction, DriverDescription, BufferLength1, DescriptionLengthPtr, DriverAttributes, BufferLength2, AttributesLengthPtr);
}

SQLRETURN SQL_API
SQLDriversW(SQLHENV EnvironmentHandle, SQLUSMALLINT Direction, SQLWCHAR * DriverDescription, SQLSMALLINT BufferLength1, SQLSMALLINT *DescriptionLengthPtr, SQLWCHAR * DriverAttributes, SQLSMALLINT BufferLength2, SQLSMALLINT *AttributesLengthPtr)
{
	(void) Direction;
	(void) DriverDescription;
	(void) BufferLength1;
	(void) DescriptionLengthPtr;
	(void) DriverAttributes;
	(void) BufferLength2;
	(void) AttributesLengthPtr;
	addEnvError((ODBCEnv *) EnvironmentHandle, "HY000", "Driver Manager only function", 0);
	return SQL_ERROR;
}
#endif /* WITH_WCHAR */
