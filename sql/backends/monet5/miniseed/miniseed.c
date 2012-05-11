#include "monetdb_config.h"

#include "miniseed.h"
//#include "vault.h"
#include "mtime.h"

str MiniseedMount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret0 = (int*) getArgReference(stk,pci,0);	//return value 1: BAT containing file_locations.
	int *ret1 = (int*) getArgReference(stk,pci,1);	//return value 2: BAT containing seq_nos.
	int *ret2 = (int*) getArgReference(stk,pci,2);	//return value 3: BAT containing timestamps.
	int *ret3 = (int*) getArgReference(stk,pci,3);	//return value 4: BAT containing integer data.
	str *targetfile = (str*) getArgReference(stk,pci,4); //arg 1: string containing the input file path.
	BAT *btime, *bdata, *bfile, *bseqno; // BATs to return, representing columns of a table.
	
	VarRecord low, high;
	wrd num_rows = 0;
	
	MSRecord *msr = NULL;
	int retcode;
	int verbose = 1;
	
	cntxt = cntxt; //to escape 'unused' parameter error.
	mb = mb; //to escape 'unused' parameter error.
	
	/* prepare to set low and high oids of return vars */
	high.value.vtype= low.value.vtype= TYPE_oid;
	low.value.val.oval= 0;
	
	bfile = BATnew(TYPE_void, TYPE_str, 0); //create empty BAT for ret0.
	if ( bfile == NULL)
		throw(MAL,"miniseed.mount",MAL_MALLOC_FAIL);
	BATseqbase(bfile, 0);
	bseqno = BATnew(TYPE_void, TYPE_int, 0); //create empty BAT for ret1.
	if ( bseqno == NULL)
		throw(MAL,"miniseed.mount",MAL_MALLOC_FAIL);
	BATseqbase(bseqno, 0);	
	
	btime = BATnew(TYPE_void, TYPE_timestamp, 0); //create empty BAT for ret2.
	if ( btime == NULL)
		throw(MAL,"miniseed.mount",MAL_MALLOC_FAIL);
	BATseqbase(btime, 0);
	bdata = BATnew(TYPE_void, TYPE_int, 0); //create empty BAT for ret3.
	if ( bdata == NULL)
		throw(MAL,"miniseed.mount",MAL_MALLOC_FAIL);
	BATseqbase(bdata, 0);
	
	if(bfile == NULL || bseqno == NULL || btime == NULL || bdata == NULL) //exception handling.
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
	
	//loop through all records in the target mseed file.
	while ((retcode = ms_readmsr (&msr, *targetfile, 0, NULL, NULL, 1, 1, verbose)) == MS_NOERROR)
	{
	
		int32_t seq_no = msr->sequence_number;
		double sample_interval = HPTMODULUS / msr->samprate; //calculate sampling interval from frequency
		long sampling_time = msr->starttime;
		
		long num_samples = msr->numsamples;
		int *data_samples = msr->datasamples;
		
		//long *sample_timestamps = (long*)malloc(num_samples*sizeof(long));
		
		//sample_timestamps[0] = start_time;
		
		int i = 0;
		for(;i<num_samples;i++)
		{
// 			char* space = (char*) malloc(27*sizeof(char));
// 			ms_hptime2isotimestr(sampling_time, space, 1);

			timestamp sampling_timestamp;
			lng st = (lng) sampling_time / 1000;
			MTIMEtimestamp_lng(&sampling_timestamp, &st);
			
			// For each sample add one row to the table
			BUNappend(bfile, (ptr) *targetfile, FALSE);
			BUNappend(bseqno, (ptr) &seq_no, FALSE);
			BUNappend(btime, (ptr) &sampling_timestamp, FALSE);
			BUNappend(bdata, (ptr) (data_samples+i), FALSE);
			sampling_time += sample_interval;
		}
		
		num_rows += i;
		
	}
	
	if ( retcode != MS_ENDOFFILE )
		ms_log (2, "Cannot read %s: %s\n", *targetfile, ms_errorstr(retcode));
	
	//cleanup memory and close file
	ms_readmsr (&msr, NULL, 0, NULL, NULL, 0, 0, 0);
	
	printf("num_rows: %ld\n", num_rows);
	high.value.val.oval= (BUN) num_rows;
	
	varSetProp(mb, getArg(pci, 0), PropertyIndex("hlb"), op_gte, (ptr) &low.value);
	varSetProp(mb, getArg(pci, 0), PropertyIndex("hub"), op_lt, (ptr) &high.value);
	
	varSetProp(mb, getArg(pci, 1), PropertyIndex("hlb"), op_gte, (ptr) &low.value);
	varSetProp(mb, getArg(pci, 1), PropertyIndex("hub"), op_lt, (ptr) &high.value);
	
	varSetProp(mb, getArg(pci, 2), PropertyIndex("hlb"), op_gte, (ptr) &low.value);
	varSetProp(mb, getArg(pci, 2), PropertyIndex("hub"), op_lt, (ptr) &high.value);
	
	varSetProp(mb, getArg(pci, 3), PropertyIndex("hlb"), op_gte, (ptr) &low.value);
	varSetProp(mb, getArg(pci, 3), PropertyIndex("hub"), op_lt, (ptr) &high.value);
	
	BBPkeepref(*ret0 = bfile->batCacheid); //return BAT.
	BBPkeepref(*ret1 = bseqno->batCacheid); //return BAT.
	BBPkeepref(*ret2 = btime->batCacheid); //return BAT.
	BBPkeepref(*ret3 = bdata->batCacheid); //return BAT.
	
	return MAL_SUCCEED;
}