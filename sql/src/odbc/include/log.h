/**********************************************************************************
 * log.h
 *
 * Include file for liblog.a. Coding? Include this and link against liblog.a.
 *
 * At this time; its a simple list manager but I expect that this will evolve into
 * a list manager which;
 *
 * - allows for messages of different severity and types to be stored
 * - allows for actions (such as saving to file or poping) to occur on selected message
 *   types and severities
 *
 **********************************************************************************/

#ifndef INCLUDED_LOG_H
#define INCLUDED_LOG_H

#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

#include <lst.h>

/*****************************************************************************
 * FUNCTION RETURN CODES
 *****************************************************************************/
#define     LOG_ERROR               0
#define     LOG_SUCCESS             1
#define		LOG_NO_DATA				2

/*****************************************************************************
 * SEVERITY
 *****************************************************************************/
#define		LOG_INFO				0
#define		LOG_WARNING				1
#define		LOG_CRITICAL			2

/*****************************************************************************
 *
 *****************************************************************************/
#define		LOG_MSG_MAX				1024

/*****************************************************************************
 * HANDLES
 *****************************************************************************/
typedef struct	tLOGMSG
{
	char			*pszModuleName;							/* liblog will malloc, copy, and free	*/
	char			*pszFunctionName;						/* liblog will malloc, copy, and free	*/
	int				nLine;
	int				nSeverity;
	int				nCode;
	char			*pszMessage;							/* liblog will malloc, copy, and free	*/

} LOGMSG, *HLOGMSG;


typedef struct	tLOG
{
	HLST		hMessages;								/* list of messages	( LOGMSG )			*/

	char		*pszProgramName;						/* liblog will malloc, copy, and free	*/
	char		*pszLogFile;							/* NULL, or filename					*/
	long		nMaxMsgs;								/* OLDEST WILL BE DELETED ONCE MAX		*/
	int			bOn;									/* turn logging on/off (default=off)	*/

} LOG, *HLOG;


/*****************************************************************************
 * API
 *****************************************************************************/

/******************************
 * logOpen
 *
 ******************************/
int logOpen( HLOG *phLog, char *pszProgramName, char *pszLogFile, long nMaxMsgs );

/******************************
 * logClose
 *
 ******************************/
int logClose( HLOG hLog );

/******************************
 * logPushMsg
 *
 ******************************/
int logPushMsg( HLOG hLog, char *pszModule, char *pszFunctionName, int nLine, int nSeverity, int nCode, char *pszMsg );
int logPushMsgv( HLOG hLog, char *pszModule, char *pszFunctionName, int nLine, int nSeverity, int nCode, ... );

/******************************
 * logPopMsg
 *
 * 1. Pops last message from stack and deletes it.
 * 2. Message returned in pszMsg (make sure its large enough)
 ******************************/
int logPopMsg( HLOG hLog, char *pszMsgHdr, int *pnCode, char *pszMsg );

/******************************
 * logOn
 *
 * 1. if false then no storage on memory or file
 * 2. default is false
 * 3. you may want to turn logging on off for
 *    debugging purposes
 ******************************/
int logOn( HLOG hLog, int bOn );

/*****************************************************************************
 * SUPPORTING FUNCS (do not call directly)
 *****************************************************************************/

/******************************
 * _logFreeMsg
 *
 * 1. This is called by lstDelete()
 ******************************/
void _logFreeMsg( void *pMsg );

#endif
