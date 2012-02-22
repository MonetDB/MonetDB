#include "monetdb_config.h"

#include "miniseed.h"
//#include "vault.h"
#include "mtime.h"

str MiniseedMount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret0 = (int*) getArgReference(stk,pci,0);	//return value 1: BAT containing timestamps.
	int *ret1 = (int*) getArgReference(stk,pci,1);	//return value 2: BAT containing integer data.
	str *targetfile = (str*) getArgReference(stk,pci,2); //arg1: string containing the input file path.
	BAT *btime, *bdata; // BATs to return.
	
	MSRecord *msr = NULL;
	int retcode;
	int verbose = 1;
	
	cntxt = cntxt; //to escape 'unused' parameter error.
	mb = mb; //to escape 'unused' parameter error.
	
	btime = BATnew(TYPE_void, TYPE_timestamp, 0); //create empty BAT for ret0.
	BATseqbase(btime, 0);
	bdata = BATnew(TYPE_void, TYPE_int, 0); //create empty BAT for ret1.
	BATseqbase(bdata, 0);
	
	if(btime == NULL || bdata == NULL) //exception handling.
	{
		if(btime)
			BBPreleaseref(btime->batCacheid);
		if(bdata)
			BBPreleaseref(bdata->batCacheid);
		throw(MAL,"mseed.mount", MAL_MALLOC_FAIL);
	}
	
	
	
	while ((retcode = ms_readmsr (&msr, *targetfile, 0, NULL, NULL, 1, 1, verbose)) == MS_NOERROR)
	{
		
		double sample_interval = HPTMODULUS / msr->samprate;
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
			
			BUNappend(btime, (ptr) &sampling_timestamp, FALSE);
			BUNappend(bdata, (ptr) (data_samples+i), FALSE);
			sampling_time += sample_interval;
		}
		
	}
	
	if ( retcode != MS_ENDOFFILE )
		ms_log (2, "Cannot read %s: %s\n", *targetfile, ms_errorstr(retcode));
	
	/* Cleanup memory and close file */
	ms_readmsr (&msr, NULL, 0, NULL, NULL, 0, 0, 0);
	
	BBPkeepref(*ret0 = btime->batCacheid); //return BAT.
	BBPkeepref(*ret1 = bdata->batCacheid); //return BAT.
	
	return MAL_SUCCEED;
}