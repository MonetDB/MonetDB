/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @f vault
 * @a Martin Kersten
 * @v 0.1
 * @+ Data Vaults
 * The Data Vault module provides the basic infrastructure to manage
 * a repository with datafiles whose integrity and use is shared between
 * MonetDB and the repository owner.
 *
 * Once a vault is created, the system administrator or crontab job adds files to the vault catalog.
 * The source attribute provides the universal resource identifier (URI)
 * in a format understood by the CURL library. In most cases, it represents a file
 * to be accessed using FTP.
 *
 * A target denotes its name in the staging area, i.e. a local cache where copies are to be stored.
 * The local cache can be hierarchical structured to spread the load over multiple volumns
 * and to retain the structure of the source repository.
 * Files are dropped from the local cache using a SQL vacuum() operation based on a LRU time stamp.
 * The retention period depends on the amount of disk resources available.
 * The vaultVacuum() operation should be automatically triggered when disk space becomes a scarce resource.
 *
 * An vaultImport() operation copies a file from the remote repository into the staging area.
 *
 * The vaultBasename() operation extract the tail of the argument. It can be used to derive
 * target filename locations.
 *
 * If source and target files reside on the same file system then a symbolic link is sufficient
 * and vaultVacuum() need not be triggered.
 *
 * The file mapping catalog is kept lean. The attribute 'created' marks the actual time
 * the file was obtained from the remote source. The lru attribute is set each time we access its content.
 * Files that are bound to internal database structures may want to set it into the future.
 * @verbatim
 * CREATE SEQUENCE sys.vaultid AS int;
 *
 * CREATE TABLE sys.vault (
 * vid 			int PRIMARY KEY,-- Internal key
 * kind			string,			-- vault kind (CSV, FITS,..)
 * source			string,			-- remote file name for cURL to access
 * target			string,			-- file name of source file in vault
 * created			timestamp,		-- timestamp upon entering the cache
 * lru				timestamp		-- least recently used
 * );
 *
 * create function vaultLocation()
 * returns string
 * external name vault.getdirectory;
 *
 * create function vaultSetLocation(dir string)
 * returns string
 * external name vault.setdirectory;
 *
 * create function vaultBasename(fnme string, split string)
 * returns string
 * external name vault.basename;
 *
 * create function vaultImport(source string, target string)
 * returns timestamp
 * external name vault.import;
 *
 * create function vaultRemove(target string)
 * returns timestamp
 * external name vault.remove;
 *
 * create procedure vaultVacuum( t timestamp)
 * begin
 * update vault
 *   set created= remove(target),
 *   lru = null
 *   where  created < t;
 * end;
 * @end verbatim
 *
 * The module is developed solely for a Linux environment.
 * The vault root is a subdirectory of the dbpath/vault/ and contains
 * a subdirectory for each vault kind. In turn, each vault kind comes
 * with a refinement of the catalog identified above using the vid to relate the two.
 *
 * For large staging pools it may be advisable to pre-create the repository
 * structure, e.g. mounting multiple volumns for its partitions.
 *
 * The session structure would be something like:
 * @begin verbatim
 * insert into vault(vid,kind,source) values(0,'dummy','ftp://ftp.rep.edu/repos/station-1'),
 * 	(1,'dummy','ftp://ftp.rep.edu/repos/station-2');
 * update vault
 *   set target = basename(source,'repos');
 * update vault
 *   set created= import(source,target)
 *   where created is null;
 * select * from vault limit 2;
 * call vacuum(now());
 * @end
 */
/*
 * Module initializaton
 */
#include "monetdb_config.h"
#include "vault.h"
#include "mal_client.h"
#include "mal_interpreter.h"

#ifdef HAVE_CURL
#include <curl/curl.h>
#endif


char vaultpath[BUFSIZ];
/*
 * The curl sample code has been copied from http://curl.haxx.se/libcurl/c/import.html
 */
#ifdef HAVE_CURL
struct FtpFile {
  const char *filename;
  FILE *stream;
};

static size_t my_fwrite(void *buffer, size_t size, size_t nmemb, void *stream)
{
	struct FtpFile *out=(struct FtpFile *)stream;

	if (!out)
		return -1;
	if (!out->stream) {
		/* open file for writing */
		out->stream=fopen(out->filename, "wb");
		if (!out->stream)
			return -1; /* failure, can't open file to write */
	}
	return fwrite(buffer, size, nmemb, out->stream);
}
#endif

str
VLTimport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	timestamp *ret = getArgReference_TYPE(stk,pci,0,timestamp);
	str *source = getArgReference_str(stk,pci,1);
	str *target = getArgReference_str(stk,pci,2);
	str msg = MAL_SUCCEED;

#ifdef HAVE_CURL
	CURL *curl;
	CURLcode res;
	char path[BUFSIZ];
	struct FtpFile ftpfile={
		path, /* name to store the file as if succesful */
		NULL
	};

	/*curl_global_init(CURL_GLOBAL_DEFAULT);*/

	snprintf(path,BUFSIZ,"%s%c%s", vaultpath, DIR_SEP, *target);
	/*mnstr_printf(GDKout,"#vault.import: %s\n",path);*/
	if (strcmp(path, *source) == 0) 
		return MTIMEcurrent_timestamp(ret);
	/* create the subdir */
	GDKcreatedir(path);
	curl = curl_easy_init();
	if(curl) {
		/* get a copy */
		curl_easy_setopt(curl, CURLOPT_URL, *source);
		/*
		 * Actually, before copying the file it is better to check its
		 * properties, such as last modified date to see if it needs a refresh.
		 * Have to find the CURL method to enact this. It may be protocol
		 * dependent.
		 */
		if (access(path, R_OK) == 0){
			/* TODO */
		}

		/* Define our callback to get called when there's data to be written */
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, my_fwrite);
		/* Set a pointer to our struct to pass to the callback */
		/* coverity[bad_sizeof] */
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ftpfile);

		/* Switch on full protocol/debug output */
		IODEBUG curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

		res = curl_easy_perform(curl);

		/* always cleanup */
		curl_easy_cleanup(curl);

		if(CURLE_OK != res)
			msg = createException(MAL,"vault.import", SQLSTATE(42000) "curl [%d] %s '%s' -> '%s'\n", res, curl_easy_strerror(res), *source,path);
	}

	if(ftpfile.stream)
		fclose(ftpfile.stream); /* close the local file */

	curl_global_cleanup();
#else
	(void) source;
	(void) target;
	msg = createException(MAL,"vault.import", SQLSTATE(42000) "No curl library");
#endif
	if (msg)
		return msg;
	(void) mb;
	(void) cntxt;
	return MTIMEcurrent_timestamp(ret);
}



str
VLTprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
#ifdef HAVE_CURL
	if (vaultpath[0] == 0){
		curl_global_init(CURL_GLOBAL_DEFAULT);
	}
#endif
	if ( vaultpath[0] == 0){
		snprintf(vaultpath, FILENAME_MAX, "%s%cvault", GDKgetenv("gdk_dbpath"), DIR_SEP);
		if (mkdir(vaultpath, 0755) < 0 && errno != EEXIST)
			return createException(MAL,"vault.getLocation", SQLSTATE(42000) "can not access vault directory");
	}
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	return MAL_SUCCEED;
}

str
VLTbasename(str *ret, str *fname, str *split)
{
	str r;
	r= strstr(*fname, *split);

	if ( r ){
		*ret = GDKstrdup( r);
		return MAL_SUCCEED;
	}
	throw(MAL,"vault.basename",SQLSTATE(42000) "Split of file failed:%s",*fname);
}

str VLTremove(timestamp *ret, str *t)
{
	(void) remove(*t);
	*ret = *timestamp_nil;
	return MAL_SUCCEED;
}

str
VLTepilogue(void *ret)
{
	(void)ret;
	return MAL_SUCCEED;
}

str
VLTsetLocation(str *ret, str *src){
	strncpy(vaultpath,*src,FILENAME_MAX);
	*ret= GDKstrdup(vaultpath);
	return MAL_SUCCEED;
}

str
VLTgetLocation(str *ret){
	*ret= GDKstrdup(vaultpath);
	return MAL_SUCCEED;
}
