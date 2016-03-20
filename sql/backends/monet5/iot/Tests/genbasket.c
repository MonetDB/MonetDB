#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Create a binay column for testing purposes */

int main(int argc, char **argv)
{
	int i,j,lim,val;
	FILE *f;

	if ( argc <3 || (argc %2 != 0) ){
		printf("use:%s <recordcount> [<filename> <type>] ...\n", argv[0]);
		return -1;
	}
	lim = atoi(argv[1]);
	if( lim < 0){
		printf("record count <0\n");
		return -1;
	}
	for( i= 2; i< argc; i+=2){
		f= fopen(argv[i],"w");
		if( f== NULL){
			printf("could not create '%s'\n",argv[i]);
			return -1;
		}

		if( strcmp(argv[i+1],"int") == 0 ){
			for(j=0; j< lim; j++){
				fwrite((void*)&val, sizeof(val), 1, f);
				val++;
			}
		} else printf("invalid type %s\n", argv[i+1]);
		fclose(f);
	}
}
