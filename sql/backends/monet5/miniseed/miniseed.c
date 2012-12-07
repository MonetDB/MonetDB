#include "monetdb_config.h"

#include "miniseed.h"
/* #include "vault.h" */
#include "mtime.h"

str MiniseedMount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat** ret;

	str *targetfile = (str*) getArgReference(stk,pci,4); /* arg 1: string containing the input file path. */
	BAT *btime, *bdata, *bfile, *bseqno; /* BATs to return, representing columns of a table. */

	wrd num_rows = 0;

	MSRecord *msr = NULL;
	MSFileParam *msfp = NULL;
	int retcode;
	int verbose = 1;
	int r;

	int seq_no_fake = 1;
	
	ret = (bat**) GDKmalloc(pci->retc*sizeof(bat*));
	if(ret == NULL)
	{
		throw(MAL,"miniseed.mount",MAL_MALLOC_FAIL);
	}

	for(r = 0; r < pci->retc; r++)
	{
		ret[r] = (int*) getArgReference(stk,pci,r);
	}

	cntxt = cntxt; /* to escape 'unused' parameter error. */
	mb = mb; /* to escape 'unused' parameter error. */

	bfile = BATnew(TYPE_void, TYPE_str, 0); /* create empty BAT for ret0. */
	if ( bfile == NULL)
		throw(MAL,"miniseed.mount",MAL_MALLOC_FAIL);
	BATseqbase(bfile, 0);
	bseqno = BATnew(TYPE_void, TYPE_int, 0); /* create empty BAT for ret1. */
	if ( bseqno == NULL)
		throw(MAL,"miniseed.mount",MAL_MALLOC_FAIL);
	BATseqbase(bseqno, 0);

	btime = BATnew(TYPE_void, TYPE_timestamp, 0); /* create empty BAT for ret2. */
	if ( btime == NULL)
		throw(MAL,"miniseed.mount",MAL_MALLOC_FAIL);
	BATseqbase(btime, 0);
	bdata = BATnew(TYPE_void, TYPE_int, 0); /* create empty BAT for ret3. */
	if ( bdata == NULL)
		throw(MAL,"miniseed.mount",MAL_MALLOC_FAIL);
	BATseqbase(bdata, 0);

	if(bfile == NULL || bseqno == NULL || btime == NULL || bdata == NULL) /* exception handling. */
	{
		if(bfile)
			BBPreleaseref(bfile->batCacheid);
		if(bseqno)
			BBPreleaseref(bseqno->batCacheid);
		if(btime)
			BBPreleaseref(btime->batCacheid);
		if(bdata)
			BBPreleaseref(bdata->batCacheid);
		throw(MAL,"miniseed.mount", MAL_MALLOC_FAIL);
	}

	/* loop through all records in the target mseed file. */
	while ((retcode = ms_readmsr_r (&msfp, &msr, *targetfile, 0, NULL, NULL, 1, 1, verbose)) == MS_NOERROR)
	{
		int seq_no = seq_no_fake;
		double sample_interval = HPTMODULUS / msr->samprate; /* calculate sampling interval from frequency */
		long sampling_time = msr->starttime;

		long num_samples = msr->numsamples;
		int *data_samples = msr->datasamples;

		int i = 0;
		for(;i<num_samples;i++)
		{
			timestamp sampling_timestamp;
			lng st = (lng) sampling_time / 1000;
			MTIMEtimestamp_lng(&sampling_timestamp, &st);

			/* For each sample add one row to the table */
			BUNappend(bfile, (ptr) *targetfile, FALSE);
			BUNappend(bseqno, (ptr) &seq_no, FALSE);
			BUNappend(btime, (ptr) &sampling_timestamp, FALSE);
			BUNappend(bdata, (ptr) (data_samples+i), FALSE);
			sampling_time += sample_interval;
		}

		num_rows += i;
		seq_no_fake++;

	}

	if ( retcode != MS_ENDOFFILE )
		ms_log (2, "Cannot read %s: %s\n", *targetfile, ms_errorstr(retcode));

	/* cleanup memory and close file */
	ms_readmsr_r (&msfp, &msr, NULL, 0, NULL, NULL, 0, 0, 0);

	printf("num_rows: %ld\n", num_rows);

	BBPkeepref(*ret[0] = bfile->batCacheid); /* return BAT. */
	BBPkeepref(*ret[1] = bseqno->batCacheid); /* return BAT. */
	BBPkeepref(*ret[2] = btime->batCacheid); /* return BAT. */
	BBPkeepref(*ret[3] = bdata->batCacheid); /* return BAT. */

	return MAL_SUCCEED;
}