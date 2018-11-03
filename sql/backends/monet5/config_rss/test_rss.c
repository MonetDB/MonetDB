#include "monetdb_config.h"
#include <stdio.h>
#include <stdlib.h>

#include <Windows.h>
#include <Psapi.h>

#include "mal.h"
#include "mal_exception.h"

#define MB(x) ((SIZE_T) (x) << 20)

#define ALLOC_CHUNK_SIZE (MB(10))
#define MIN_WORKING_SET MB(100)
#define MAX_WORKING_SET MB(200)
#define MAX_VIRTUAL_MEMORY MB(400)

#define TEST_FILE "test_file"

extern __declspec(dllexport) str TESTrestricted_rss(lng* RetVal, lng* GDK_mem_maxsize /*in bytes*/);

str TESTrestricted_rss(lng* RetVal, lng* GDK_mem_maxsize /*in bytes*/)
{
	*RetVal = 0; // Use a dummy return value to make interfacing with sql/mal easy.


	// We are going to try to allocate twice the maximum configured rss bound.
	size_t rss_bound = (size_t) *GDK_mem_maxsize, to_be_allocated = 2 * rss_bound, allocated = 0;

	// A test file is going to be (re)created in the current directory.
	FILE* const pfile = fopen(TEST_FILE, "w");

	char* committedMemory = NULL;

	while(allocated < to_be_allocated)
	{
		if(!(committedMemory = (char*) malloc(ALLOC_CHUNK_SIZE)))
		{
			break;
		}

		// Write something to newly committed memory.
		for (int i = 0; i < ALLOC_CHUNK_SIZE - 1; i++)
		{
			committedMemory[i] = rand()%26+'a';
		}
			committedMemory[ALLOC_CHUNK_SIZE] = '\0';

		// Perform some I/O to make sure the OS is actually doing something with these dirty pages.

		fputs(committedMemory, pfile);

		// Query process memory counters from OS.
		PROCESS_MEMORY_COUNTERS_EX psmemCounters;
		GetProcessMemoryInfo(GetCurrentProcess(), (PROCESS_MEMORY_COUNTERS *) &psmemCounters, sizeof(PROCESS_MEMORY_COUNTERS_EX));
		
		printf("Working Set:\t%lld MB\t", psmemCounters.WorkingSetSize >> 20);
		printf("Total Memory:\t%lld MB\n", psmemCounters.PrivateUsage >> 20);

		if (psmemCounters.WorkingSetSize > rss_bound) {
			fclose(pfile);
			throw(MAL, "test_config_rss.run_test_config_rss", "physical memory allocation violates configured working set size limit" );
		}
	}

	fclose(pfile);

	return MAL_SUCCEED;
}
