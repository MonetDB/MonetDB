/***************************************************************************\
*                     SMP CHECK                                             *
\***************************************************************************/
typedef struct {
	caliblng	nrCpus;
} SMPinfo;

#define SMP_TOLERANCE 0.30
#define SMP_ROUNDS 1024*1024*64
caliblng checkNrCpus()
{
	caliblng i,curr=1,lasttime=0,thistime,cpus;
	struct timeval t0,t1;
	while(1) 
	{	
#ifdef CALIBRATOR_PRINT_OUTPUT
		printf("SMP: Checking %d CPUs\n",curr);
#endif
		gettimeofday(&t0,0);
		for(i=0;i<curr;i++) 
		{
			switch(fork())
			{
				case -1 : fatalex("fork");
				case 0  : 
				{
					unsigned int s=1,r;
					for(r=0;r<SMP_ROUNDS;r++)
						s=s*r+r;
					exit(0);
					
				}
			}
		}
		for(i=0;i<curr;i++)
			wait(0);
		gettimeofday(&t1,0);
		thistime=((t1.tv_sec-t0.tv_sec)*1000000+t1.tv_usec-t0.tv_usec);
		if (lasttime>0 && (float)thistime/lasttime>1+SMP_TOLERANCE)
			break;
		lasttime=thistime;
		cpus=curr;
		curr*=2; /* only check for powers of 2 */
	}
#ifdef CALIBRATOR_PRINT_OUTPUT
	printf("\n",curr);
#endif
	return cpus;
}

void checkSMP(SMPinfo *smp)
{
	smp->nrCpus=checkNrCpus();
}
