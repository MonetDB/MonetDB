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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f mseed
 * @a Martin Kersten
 * @v 0.1
 * @+ Mseed
 * These routines are meant to interpret mseed files already stored in the vault.
 * The simplifying situation is that mseed has a single model.
 * The code base assumes that libmseed has been installed on your system.
 *
 * The mseed catalog initialization script should have been run.
 * @begin verbatim
 * -- this schema is intended to experiment with accessing mseed files
 * DROP FUNCTION mseedImport();
 * DROP TABLE mseedCatalog;
 *
 * -- all records in the mseed files correspond to a row in the catalog
 * CREATE TABLE mseedCatalog (
 * mseed			int, 			-- Vault file id
 * seqno			int,			-- SEED record sequence number, should be between 0 and 999999
 * 		 PRIMARY KEY (mseed,seqno),
 * dataquality 	char,			-- Data record indicator, should be 'D=data unknown qual',
 * 								-- 'R=raw no quality', 'Q= quality controlled' or 'M'
 * network			varchar(11),	-- Network
 * station			varchar(11),	-- Station
 * location		varchar(11),	-- Location
 * channel			varchar(11),	-- Channel
 * starttime 		timestamp,		-- Record start time, the time of the first sample, as a high precision epoch time
 * samplerate		double,			-- Nominal sample rate (Hz)
 * sampleindex		int,			-- record offset in the file
 * samplecnt		int,			-- Number of samples in record
 * sampletype		string,			-- storage type in mseed record
 * minval			float,			-- statistics for search later
 * maxval			float
 * );
 *
 * -- The reference table for querying is simply
 * CREATE TABLE mseed(
 * mseed			int, 			-- Vault file id
 * seqno			int,			-- SEED record sequence number, should be between 0 and 999999
 * time			timestamp,		-- click
 * data			int,			-- The actual measurement value.
 * FOREIGN KEY (mseed,seqno) REFERENCES mseedCatalog(mseed,seqno)
 * );
 *
 * SELECT * FROM mseed WHERE data >3000;
 * -- can be answered by preselecting the catalog first.
 * SELECT mseed FROM mseedcatalog WHERE maxval >3000;
 * -- followed by loading the corresponding files if they are not cached yet
 * SELECT * from mseed((SELECT * FROM tmp));
 *
 * -- this function inserts the mseed record information into the catalog
 * -- errors are returned for off-line analysis.
 *
 * CREATE FUNCTION mseedImport(vid int, entry string)
 * RETURNS int
 * EXTERNAL NAME mseed.import;
 *
 * CREATE FUNCTION mseedLoad(entry string)
 * RETURNS TABLE(time timestamp, data int)
 * EXTERNAL NAME mseed.load;
 * @end verbatim
 *
 * @- How to use the mseed catalog.
 * First, the vault directory is populated with the location of the mseed source files.
 * This information is gathered with a script, which also creates the tables for the
 * individual stations.
 * The corresponding local name is set using the basename property,
 * and all files creation and access times are set to null.
 * Following, a limited number of files are loaded into the vault and analysed.
 * The information extracted ends up in the catalog, and remains there forever.
 * The underlying mseed file is not decrypted directly, it will be done as soon
 * as a query requests its.
 *
 * A test sequence (after the vault directory has been populated)
 * to populate the mseedcatalog.
 * @begin verbatim
 * create table batch(created timestamp,vid int,source string, target string);
 *
 * insert into batch
 * select null, vid, source, target from vault where created is null limit 2;
 * select vaultImport( source, target) from batch;
 * update vault set created = now() where target in (select target from batch where created is not null);
 * select mseedImport(vid,target) from batch;
 * select mseedLoad(target) from batch limit 10;
 * drop table batch;
 * @end verbatim
 *
 */
#include "mseed.h"
#include "vault.h"
#include "mtime.h"

str SQLstatementIntern(Client c, str *expr, str nme, int execute, bit output);

#define QRYinsertI "INSERT INTO mseedCatalog(mseed, seqno, dataquality, network, \
	 station, location, channel, starttime , samplerate, sampleindex, samplecnt, sampletype, minval,maxval) \
	 VALUES(%d, %d,'%c','%s', '%s','%s','%s','%s',%f,%d,%d,'%s',%d,%d);"

str
MseedImport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	int *vid = (int*) getArgReference(stk,pci,1);
	str *targetfile = (str*) getArgReference(stk,pci,2);
	str msg = MAL_SUCCEED;
	MSRecord *msr = 0;

	int verbose   = 1;
	//int ppackets  = 2;
	int reclen    = -1;
	int dataflag  = 1;
	int retcode;
	int j;
	int sampleindex = 0;
	time_t t;
	struct tm *tm;
	char file[BUFSIZ];
	char buf[BUFSIZ], *s= buf;
	char starttime[BUFSIZ];
	int imin = INT_MAX, imax = INT_MIN;

	/* keep state of a file to detect major deviances */
	str network =0, station = 0 , location = 0 , channel = 0;
	char sampletype = 0;

	(void) mb;
	*ret = int_nil;

	snprintf(file,BUFSIZ,"%s%c%s",vaultpath,DIR_SEP,*targetfile);
	if ( access(file,R_OK) )
		throw(MAL, "mseed.load", "Cannot access %s\n", file);
	while ( (retcode = ms_readmsr (&msr, file, reclen, NULL, NULL, 1, dataflag, verbose)) == MS_NOERROR  )
	{
		if ( network == 0){
			network= GDKstrdup(msr->network);
			station= GDKstrdup(msr->station);
			location= GDKstrdup(msr->location);
			channel= GDKstrdup(msr->channel);
			sampletype = msr->sampletype;
		} else {
			if( strcmp(network,msr->network))
				msg = createException(MAL,"mseed.import","network name is not stable");
			if( strcmp(station,msr->station))
				msg = createException(MAL,"mseed.import","station name is not stable");
			if( strcmp(location,msr->location))
				msg = createException(MAL,"mseed.import","location name is not stable");
			if( strcmp(channel,msr->channel))
				msg = createException(MAL,"mseed.import","channel name is not stable");
			if ( sampletype != msr->sampletype)
				msg = createException(MAL,"mseed.import","sample type is not stable");
			if (msg) goto wrapup;
		}
		t= MS_HPTIME2EPOCH(msr->starttime);
		tm = gmtime(&t);
		snprintf(starttime,BUFSIZ,"%d-%02d-%02d %02d:%02d:%02d.%06ld", tm->tm_year +(tm->tm_year > 80?1900:2000), tm->tm_mon+1,tm->tm_mday, tm->tm_hour, tm->tm_min,tm->tm_sec, msr->starttime % HPTMODULUS);
		/* collect the statistics */
		switch(msr->sampletype){
		case 'i':
			imin = INT_MAX, imax = INT_MIN;
			if (msr->datasamples)
			for ( j=0;j< msr->samplecnt; j++){
					if ( imin > ((int*) msr->datasamples)[j]) imin = ((int*) msr->datasamples)[j];
					if ( imax < ((int*) msr->datasamples)[j]) imax = ((int*) msr->datasamples)[j];
			}
			snprintf(buf,BUFSIZ,QRYinsertI, *vid, msr->sequence_number,msr->dataquality,msr->network, msr->station, msr->location, msr->channel,
			starttime,msr->samprate, sampleindex,msr->samplecnt,"int",imin,imax);
			break;
		case 'a': case 'f': case 'd':
		default:
			msg = createException(MAL,"mseed.import","data type not yet implemented");
			goto wrapup;
		}
		if ( ( msg =SQLstatementIntern(cntxt,&s,"mseed.import",TRUE,FALSE)) != MAL_SUCCEED)
				break;

		sampleindex += msr->samplecnt;
	}
wrapup:
	/* Make sure everything is cleaned up */
	ms_readmsr (&msr, NULL, 0, NULL, NULL, 0, 0, 0);
	if ( network) GDKfree(network);
	if ( station) GDKfree(station);
	if ( location) GDKfree(location);
	if ( channel) GDKfree(channel);
	if ( msg)
		return msg;
	if ( retcode != MS_ENDOFFILE )
		throw(MAL, "mseed.dump", "Cannot read %s: %s\n", *targetfile, ms_errorstr(retcode));
	*ret = *vid;
	return msg;
}

static str
MseedLoadIntern(BAT **bbtime, BAT **bbdata, str targetfile)
{
	str msg = MAL_SUCCEED;
	MSRecord *msr = 0;
	BAT *btime, *bdata;

	int verbose   = 1;
	//int ppackets  = 2;
	int reclen    = -1;
	int dataflag  = 1;
	int retcode;
	int j;
	time_t t;
	struct tm *tm;
	timestamp ts;
	char file[BUFSIZ];
	date d;
	daytime dt;
	tzone tz;
	int ms,stepsize,minutes=0;

	snprintf(file,BUFSIZ,"%s%c%s",vaultpath,DIR_SEP,targetfile);
	if ( access(file,R_OK) )
		throw(MAL, "mseed.load", "Cannot access %s\n", file);

	btime = BATnew(TYPE_void,TYPE_timestamp,0);
	if ( btime == NULL)
		throw(MAL,"mseed.load",MAL_MALLOC_FAIL);
	BATseqbase(btime,0);
	bdata = BATnew(TYPE_void,TYPE_int,0);
	if ( bdata == NULL){
		BBPreleaseref(btime->batCacheid);
		throw(MAL,"mseed.load",MAL_MALLOC_FAIL);
	}
	BATseqbase(bdata,0);
	if ( btime == NULL || bdata == NULL ){
		if ( btime) BBPreleaseref(btime->batCacheid);
		if ( bdata) BBPreleaseref(bdata->batCacheid);
		throw(MAL, "mseed.load", MAL_MALLOC_FAIL);
	}
	*bbtime = btime;
	*bbdata = bdata;
	MTIMEtzone_create(&tz,  &minutes);
	while ( (retcode = ms_readmsr (&msr, file, reclen, NULL, NULL, 1, dataflag, verbose)) == MS_NOERROR  )
	{
		stepsize = 1000000/ msr->samprate;
		/* collect the statistics */
		switch(msr->sampletype){
		case 'i':
			if (msr->datasamples)
			for ( j=0;j< msr->samplecnt; j++){
					t= MS_HPTIME2EPOCH(msr->starttime);
					tm = gmtime(&t);
					tm->tm_year += (tm->tm_year > 80?1900:2000);
					ms = (msr->starttime % HPTMODULUS)/1000;
					tm->tm_mon++;
					MTIMEdate_create(&d,&tm->tm_year, &tm->tm_mon, &tm->tm_mday);
					MTIMEdaytime_create(&dt, &tm->tm_hour, &tm->tm_min, &tm->tm_sec, &ms);
					MTIMEtimestamp_create(&ts, &d, &dt, &tz);
					BUNappend(btime, (ptr) &ts , FALSE);
					BUNappend(bdata, (ptr)(((int*) msr->datasamples)+j) , FALSE);
					msr->starttime += stepsize;
			}
			break;
		case 'a': case 'f': case 'd':
		default:
			msg = createException(MAL,"mseed.load","data type not yet implemented");
			goto wrapup;
		}
	}
wrapup:
	/* Make sure everything is cleaned up */
	ms_readmsr (&msr, NULL, 0, NULL, NULL, 0, 0, 0);
	if ( retcode != MS_ENDOFFILE )
		throw(MAL, "mseed.load", "Cannot read %s: %s\n", *targetfile, ms_errorstr(retcode));
	return msg;
}

str
MseedLoadSQL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	str *targetfile = (str*) getArgReference(stk,pci,1);
	str msg = MAL_SUCCEED;
	BAT *btime, *bdata, *table;

	(void) cntxt;
	(void) mb;

	table = BATnew(TYPE_str,TYPE_bat,0);
	if ( table == NULL)
		throw(MAL, "mseed.load", MAL_MALLOC_FAIL);
	msg = MseedLoadIntern(&btime, &bdata, *targetfile);
	if ( msg == MAL_SUCCEED){
		BUNins(table, (ptr)"time", (ptr)&btime->batCacheid, FALSE);
		BUNins(table, (ptr)"data", (ptr)&bdata->batCacheid, FALSE);
		BBPreleaseref(btime->batCacheid);
		BBPreleaseref(bdata->batCacheid);
		BBPkeepref(*ret= table->batCacheid);
	}
	return msg;
}

str
MseedLoad(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret0 = (int*) getArgReference(stk,pci,0);
	int *ret1 = (int*) getArgReference(stk,pci,1);
	str *targetfile = (str*) getArgReference(stk,pci,2);
	str msg;
	BAT *btime, *bdata;

	(void) cntxt;
	(void) mb;

	msg = MseedLoadIntern(&btime, &bdata, *targetfile);
	if ( msg == MAL_SUCCEED){
		BBPkeepref(*ret0 = btime->batCacheid);
		BBPkeepref(*ret1 = bdata->batCacheid);
	}
	return msg;
}
