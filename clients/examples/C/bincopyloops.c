
#include "bincopydata.h"


void convert16(void *start, void *end);
void convert32(void *start, void *end);
void convert64(void *start, void *end);
void convert128(void *start, void *end);

void
convert16(void *start, void *end)
{
	for (uint16_t *p = start; p < (uint16_t*)end; p++)
		copy_binary_convert16(p);
}

void
convert32(void *start, void *end)
{
	for (uint32_t *p = start; p < (uint32_t*)end; p++)
		copy_binary_convert32(p);
}

void
convert64(void *start, void *end)
{
	for (uint64_t *p = start; p < (uint64_t*)end; p++)
		copy_binary_convert64(p);
}

void
convert128(void *start, void *end)
{
#ifdef HAVE_HGE
	for (uhge *p = start; p < (uhge*)end; p++) {
		copy_binary_convert128(p);
	}
#else
	(void)start;
	(void)end;
#endif
}



int
main(void)
{
	(void)convert16;
	(void)convert32;
	(void)convert64;
	(void)convert128;
	return 0;
}
