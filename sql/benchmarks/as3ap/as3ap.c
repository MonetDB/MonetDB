/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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

@T The AS3AP generator
@a

@c

/*----------------------------------------------------------------+
|                                                                 |
|   AS3APgen.c                                                    |
|                                                                 |
|   Version 0.9 Beta                                              |
|                                                                 |
|   Generates data for the four tables:                           |
|     hundred, uniques, tenpct, updates                           |
|                                                                 |
|   Created 8 June 1992                                           |
|                                                                 |
|       at:    School of Information Systems                      |
|              University of East Anglia                          |
|              Norwich  NR4 7TJ                                   |
|              United Kingdom           Phone +44 603 56161       |
|                                                                 |
|       by:    Aram Balian    ba@uk.ac.uea.sys    Ext. 3171       |
|              Jens Krabbe    jk@uk.ac.uea.sys    Ext. 3171       |
|              Dan Smith      djs@uk.ac.uea.sys   Ext. 2608       |
|                                                                 |
|                                                                 |
|                                                                 |
|                                                                 |
|   Known problems:                                               |
|                                                                 |
|       Memory allocation: Memory is not freed between generation |
|                          of individual tables.                  |
|                Solution: Generate only one table at time.       |
|                                                                 |
|       Optimization:      Many similar data are re-generated     |
|                          where they could have been re-used.    |
|                                                                 |
+----------------------------------------------------------------*/

#include <math.h>
#include <stdio.h>

static int ANINT  = (-1000000008);
static int INIT   = (-1);
static int NORMAL =  1;

float ran1(), gasdev(), poidev();

#define DATELEN 12
#define CODELEN 11
#define NAMELEN 21
#define ADDRLEN 81

/*****************************/
/*** MEM allocation header ***/

#ifndef C_ARGS
#ifdef CPP
#define C_ARGS(arglist) arglist
#else CPP
#define C_ARGS(arglist) ()
#endif CPP
#endif  C_ARGS

#ifndef MEMALLOC_CORPS
#define C_EXT extern
#else MEMALLOC_CORPS
#define C_EXT
#endif MEMALLOC_CORPS

typedef struct elem
{
    char *buff;
    struct elem *next;
    struct elem *prev;
} ELEM;

typedef struct
{
  ELEM *last;
  ELEM *first; 
} MEM_BUFF;


/************************************************************************/
/*--------------------------EXPORTED FUNCTIONS--------------------------*/
/************************************************************************/

C_EXT MEM_BUFF  *memgraph;
C_EXT MEM_BUFF *MEM_alloc C_ARGS((unsigned size));
C_EXT void      MEM_free C_ARGS((MEM_BUFF *mem_buff));
C_EXT char     *MEM_get C_ARGS((MEM_BUFF *mem_buff, unsigned size));
C_EXT void      MEM_unget C_ARGS((MEM_BUFF *mem_buff, char *elem));

#undef C_EXT

#endif MEMALLOC_HEADER

/*** end MEM alloc header ***/
/****************************/

static MEM_BUFF *tampon;
    /*  tampon allocation memoire */
static unsigned gest_mem;

int (*intcompar)();

static int intcompare(i,j) int *i, *j;
{
  return(*i - *j);
}
@

@c
main( argc, argv ) int argc; char *argv[];
{
  int ntup;

  if ( ( argc < 2 ) || ( argc > 3 ) ) {
    printf( "Usage: %s <numberoftuples> [<tablename>]\n", argv[0] );
    printf( "       where <numberoftuples> should be divisible with 10000\n" );
    printf( "       and the optional <tablename> must be one of:\n" );
    printf( "       \"tenpct\" \"uniques\" \"updates\" \"hundred\"\n" );
    exit(1);
  }

  ntup = atoi( argv[1] );

  if ( argc == 2 ) {
    generate_hundred( ntup );
    generate_tenpct ( ntup );
    generate_uniques( ntup );
    generate_updates( ntup );
  }

  if ( argc == 3 ) {
    if ( ! strcmp( argv[2], "hundred" ) ) generate_hundred( ntup );
    if ( ! strcmp( argv[2], "tenpct"  ) ) generate_tenpct ( ntup );
    if ( ! strcmp( argv[2], "uniques" ) ) generate_uniques( ntup );
    if ( ! strcmp( argv[2], "updates" ) ) generate_updates( ntup );
  }
}
@

@c
generate_tenpct(ntup) int ntup;
{
   FILE  * OutputFile;
   int  ** iarray;
   char ** datearray;
   char ** codearray;    
   char ** namearray;
   char ** addressarray;
   int i, ia=6; /* number of int arrays */

  gest_mem  = ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * DATELEN ) );
  gest_mem += ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * CODELEN ) );
  gest_mem += ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * NAMELEN ) );
  gest_mem += ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * ADDRLEN ) );
  gest_mem += ( ( ia   * sizeof ( int*  ) ) + ( ia   * sizeof(int ) * ntup    ) );

  gest_mem += ( ( sizeof (int) * ntup ) * 4 ) ; /* pour appeler toutes      
        /*  les fonctions   date_gen,code_gen,tenpct_name et tenpct_address */
   


  tampon  = ( MEM_BUFF* ) MEM_alloc (gest_mem);

  datearray= (char **) MEM_get(tampon, ( ntup * sizeof ( char* ) ) );    
  for(i=0;i<ntup;i++) {
    datearray[i] = (char *) MEM_get(tampon, (sizeof(char) * DATELEN) );
    if( datearray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: tenpct.date array\n");
      fflush(stdout);
      exit(1);
    }
  }

  codearray= (char **)  MEM_get(tampon, ( ntup * sizeof ( char* ) ) );
  for(i=0;i<ntup; i++) {
    codearray[i] = (char *)  MEM_get(tampon, (sizeof(char) * CODELEN) ); 
    if( codearray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: tenpct.code array\n");
      fflush(stdout);
      exit(1);
    }
  }

  namearray= (char **)  MEM_get(tampon, ( ntup * sizeof ( char* ) ) ); 
  for(i=0;i<ntup; i++) {
    namearray[i] = (char *)  MEM_get(tampon, (sizeof(char) * NAMELEN) ); 
    if( namearray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: tenpct.name array\n");
      fflush(stdout);
      exit(1);
    }
  }

  addressarray= (char **)  MEM_get(tampon, ( ntup * sizeof ( char* ) ) );   
  for(i=0;i<ntup; i++) {
    addressarray[i] = (char *) MEM_get(tampon, (sizeof(char) * ADDRLEN) ); 
    if( addressarray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: tenpct.address array\n");
      fflush(stdout);
      exit(1);
    }  
  }

  iarray= (int **) MEM_get(tampon, ( ia * sizeof ( int* ) ) );    
 
  for(i=0;i<ia;i++) {
    iarray[i]= (int *)  MEM_get(tampon,  ( sizeof(int) * ntup) );
    if( iarray[i] == 0 ) {
      fprintf(stdout,"Error: Memory allocation: tenpct %d array\n",i);
      fflush(stdout);
      exit(1);
    }
  }

  key_sparse(iarray[0],ntup);  /* key and int are correlated
  int_sparse(iarray[1],ntup);    and equal. */
  signed_sparse(iarray[2],ntup);  
  float_zipf(iarray[3],ntup);
  double_tenpct(iarray[4],ntup); /* double and decim are correlated
  decim_tenpct(iarray[5],ntup);     and equal */
  date_gen(datearray,ntup);
  code_gen(codearray,ntup);
  tenpct_name(namearray,ntup);
  tenpct_address(addressarray,ntup);
 
  if ( ! ( OutputFile = fopen( "tenpct", "w" ) ) ) {
    fprintf( stderr, "ERROR: Unable to open tenpct\n");
    exit(1);
  }

  for(i=0;i<ntup;i++)
    fprintf(OutputFile, "%d,%d,%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"Filled\"\n",
      *(*(iarray+0) +i), *(*(iarray+0) +i), *(*(iarray+2) +i), 
      *(*(iarray+3) +i), *(*(iarray+4) +i), *(*(iarray+4) +i),
      datearray[i], codearray[i], namearray[i], addressarray[i]);
 
  fclose(OutputFile);

/*  free(datearray);
    free(codearray);
    free(namearray);
    free(addressarray);
    for(i=0;i<ia;i++) { free( iarray[i] ); }
    free( iarray ); */

    MEM_free (tampon);

} 
@


@c
generate_uniques(ntup) int ntup;
{
  FILE  * OutputFile;
  int  ** iarray;
  char ** datearray;
  char ** codearray;    
  char ** namearray;
  char ** addressarray;
  int i, ia=6; /* number of int arrays */ 


  gest_mem  = ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * DATELEN ) );
  gest_mem += ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * CODELEN ) );
  gest_mem += ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * NAMELEN ) );
  gest_mem += ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * ADDRLEN ) );
  gest_mem += ( ( ia   * sizeof ( int*  ) ) + ( ia   * sizeof(int ) * ntup    ) );

  gest_mem += ( ( sizeof (int) * ntup ) * 4 ) ; /* pour appeler toutes      
        /*  les fonctions   date_gen,code_gen,uniques_name et uniques_address */
   


  tampon  = ( MEM_BUFF* ) MEM_alloc (gest_mem);

  datearray= (char **) MEM_get(tampon, ( ntup * sizeof ( char* ) ) );
  for(i=0;i<ntup;i++) {
    datearray[i] = (char *) MEM_get(tampon,(sizeof(char) * DATELEN) );
    if( datearray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: uniques.date array\n");
      fflush(stdout);
      exit(1);
    }
  }
 
  codearray= (char **) MEM_get(tampon, ( ntup * sizeof ( char* ) ) );
  for(i=0;i<ntup; i++) {
    codearray[i] = (char *) MEM_get(tampon,(sizeof(char) * CODELEN) );
    if( codearray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: uniques.code array\n");
      fflush(stdout);
      exit(1);
    }
  }


  namearray= (char **) MEM_get(tampon, ( ntup * sizeof ( char* ) ) ); 
  for(i=0;i<ntup; i++) {
    namearray[i] = (char *) MEM_get(tampon,(sizeof(char) * NAMELEN) );
    if( namearray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: uniques.name array\n");
      fflush(stdout);
      exit(1);
    }
  }

  addressarray= (char **) MEM_get(tampon, ( ntup * sizeof ( char* ) ) );   
  for(i=0;i<ntup; i++) {
    addressarray[i] = (char *) MEM_get(tampon,(sizeof(char) * ADDRLEN) );
    if( addressarray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: uniques.address array\n");
      fflush(stdout);
      exit(1);
    }
  }

  iarray= (int **) MEM_get(tampon, ( ia * sizeof ( int* ) ) );    
 
  for(i=0;i<ia;i++) {
    iarray[i]= (int *)  MEM_get(tampon,( sizeof(int) * ntup) );
    if( iarray[i] == 0 ) {
      fprintf(stdout,"Error:Memory allocation: uniques %d array\n",i);
      fflush(stdout);
      exit(1);
    }
  }

  key_sparse(iarray[0],ntup);   /* key and int are correlated
  int_sparse(iarray[1],ntup);      and equal */
  signed_sparse(iarray[2],ntup);
  float_zipf(iarray[3],ntup);
  double_normal(iarray[4],ntup);
  uniques_decim(iarray[5],ntup); 
  date_gen(datearray,ntup);
  code_gen(codearray,ntup);
  uniques_name(namearray,ntup);
  uniques_address(addressarray,ntup);

  if ( ! ( OutputFile = fopen( "uniques", "w" ) ) ) {
    fprintf( stderr, "ERROR: Unable to open uniques\n");
    exit(1);
  }

  for(i=0;i<ntup;i++)
    fprintf(OutputFile, "%d,%d,%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"Filled\"\n",
      *(*(iarray+0) +i), *(*(iarray+0) +i), *(*(iarray+2) +i), 
      *(*(iarray+3) +i), *(*(iarray+4) +i), *(*(iarray+5) +i),
      datearray[i], codearray[i],  namearray[i], addressarray[i]);
 
  fclose(OutputFile);


  /*  free(datearray);
      free(codearray);
      free(namearray);
      free(addressarray);
      for(i=0;i<ia;i++) { free( iarray[i] ); }
      free( iarray ); */

  MEM_free(tampon);

} 
@

@c
generate_updates (ntup) int ntup;
{
  FILE  * OutputFile;
  int  ** iarray;
  char ** datearray;
  char ** codearray;    
  char ** namearray;
  char ** addressarray;
  int i, ia=6; /* number of int arrays */
  gest_mem  = ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * DATELEN ) );
  gest_mem += ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * CODELEN ) );
  gest_mem += ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * NAMELEN ) );
  gest_mem += ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * ADDRLEN ) );
  gest_mem += ( ( ia   * sizeof ( int*  ) ) + ( ia   * sizeof(int ) * ntup    ) );

  gest_mem += ( ( sizeof (int) * ntup ) * 4 ) ; /* pour appeler toutes      
        /*  les fonctions   date_gen,code_gen,uniques_name et uniques_address */
   


  tampon  = ( MEM_BUFF* ) MEM_alloc (gest_mem);

  datearray= (char **) MEM_get(tampon, ( ntup * sizeof ( char* ) ) );
 
  for(i=0;i<ntup;i++) {
    datearray[i] = (char *) MEM_get(tampon,(sizeof(char) * DATELEN) );
    if( datearray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: updates.date array\n");
  fflush(stdout);
  exit(1);
     }
  }

  codearray= (char **) MEM_get(tampon, ( ntup * sizeof ( char* ) ) );
 
 
  for(i=0;i<ntup; i++) {
    codearray[i] = (char *) MEM_get(tampon,(sizeof(char) * CODELEN) );
     if( codearray[i] == 0) {
      fprintf(stdout,"%s\n","Error: Memory allocation: updates.code array\n");
      fflush(stdout);
      exit(1);
    }
  }
 
  namearray= (char **) MEM_get(tampon, ( ntup * sizeof ( char* ) ) ); 
  for(i=0;i<ntup; i++) {
    namearray[i] = (char *) MEM_get(tampon,(sizeof(char) * NAMELEN) );
    if( namearray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: updates.name array\n");
      fflush(stdout);
      exit(1);
    }
  }

  addressarray= (char **) MEM_get(tampon, ( ntup * sizeof ( char* ) ) ); 
  for(i=0;i<ntup; i++) {
    addressarray[i] = (char *) MEM_get(tampon,(sizeof(char) * ADDRLEN) );
    if( addressarray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: updates.address array\n");
      fflush(stdout);
      exit(1);
    }
  }

  iarray= (int **) MEM_get(tampon, ( ia * sizeof ( int* ) ) ); 

  for(i=0;i<ia;i++) {
    iarray[i]= (int *)  MEM_get(tampon,( sizeof(int) * ntup) );
    if( iarray[i] == 0 ) {
      fprintf(stdout,"Error: Memory allocation: updates %d array\n",i);
      fflush(stdout);
      exit(1);
    }
  }

  key_dense(iarray[0],ntup);  /* key and int are correlated
  int_dense(iarray[5],ntup);     and equal */
  signed_sparse(iarray[1],ntup);
  float_zipf(iarray[3],ntup);
  updates_double(iarray[4],ntup);
  uniques_decim(iarray[2],ntup);  
  date_gen(datearray,ntup);
  code_gen(codearray,ntup);
  uniques_name(namearray,ntup);
  uniques_address(addressarray,ntup);

  if ( ! ( OutputFile = fopen( "updates", "w" ) ) ) {
    fprintf( stderr, "ERROR: Unable to open updates\n");
    exit(1);
  }

  for(i=0;i<ntup;i++)
    fprintf(OutputFile, "%d,%d,%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"Filled\"\n",
    *(*(iarray+0) +i), *(*(iarray+0) +i), *(*(iarray+1) +i), 
    *(*(iarray+3) +i), *(*(iarray+4) +i), *(*(iarray+2) +i),
    datearray[i], codearray[i],  namearray[i], addressarray[i]);
 
  fclose(OutputFile);

/*  free(datearray);
    free(codearray);
    free(namearray);
    free(addressarray);
    for(i=0;i<ia;i++) { free( iarray[i] ); }
    free( iarray ); */

   MEM_free(tampon) ;   

} 
@

@c
generate_hundred(ntup) int ntup;
{
  FILE  * OutputFile;
  int  ** iarray;
  char ** datearray;
  char ** codearray;    
  char ** namearray;
  char ** addressarray;
  int i, ia=5; /* number of int arrays */

  gest_mem  = ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * DATELEN ) );
  gest_mem += ( ( ntup * sizeof ( char* ) ) + ( ntup * sizeof(char) * CODELEN ) );
  gest_mem += ( ( 100  * sizeof ( char* ) ) + ( 100  * sizeof(char) * NAMELEN ) );
  gest_mem += ( ( 100  * sizeof ( char* ) ) + ( 100  * sizeof(char) * ADDRLEN ) );
  gest_mem += ( ( ia   * sizeof ( int*  ) ) + (   5  * sizeof(int ) * ntup    ) );

  gest_mem += ( ( sizeof (int) * ntup ) * 2 ) ; /* pour appeler toutes      
        /*  les fonctions   date_gen,code_gen    */
   


  tampon  = ( MEM_BUFF* ) MEM_alloc (gest_mem);

  datearray= (char **) MEM_get(tampon, ( ntup * sizeof ( char* ) ) );

  for(i=0;i<ntup;i++) {
    datearray[i] = (char *) MEM_get(tampon,(sizeof(char) * DATELEN) );
    if( datearray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: hundred.date array\n");
      fflush(stdout);
      exit(1);
    }
  }
  codearray= (char **) MEM_get(tampon, ( ntup * sizeof ( char* ) ) );
 
  for(i=0;i<ntup; i++) {
    codearray[i] = (char *) MEM_get(tampon,(sizeof(char) * CODELEN) );
    if( codearray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: hundred.code array");
      fflush(stdout);
      exit(1);
    }
  }
  namearray= (char **) MEM_get(tampon, ( 100 * sizeof ( char* ) ) ); 
  
  for(i=0;i<100; i++) {
    namearray[i] = (char *) MEM_get(tampon,(sizeof(char) * NAMELEN) );
    if( namearray[i] == 0) {
      fprintf(stdout,"%s\n","Error: Memory allocation: hundred.name array");
      fflush(stdout);
      exit(1);
    }
  }


  addressarray= (char **) MEM_get(tampon, ( 100 * sizeof ( char* ) ) );  
  for(i=0;i<100; i++) {
    addressarray[i] = (char *) MEM_get(tampon,(sizeof(char) * ADDRLEN) );
    if( addressarray[i] == 0) {
      fprintf(stdout,"Error: Memory allocation: hundred.address array\n");
      fflush(stdout);
      exit(1);
    }
  }
    
  iarray= (int **) MEM_get(tampon, ( ia * sizeof ( int* ) ) );  
  for(i=0;i<3;i++) {
    iarray[i]= (int *)  MEM_get(tampon,( sizeof(int) * ntup) );
    if( iarray[i] == 0 ) {
      fprintf(stdout,"Error: Memory allocation: hundred %d array\n",i);
      fflush(stdout);
      exit(1);
    }
  }
  for(i=3;i<5;i++) {
    iarray[i]= (int *) MEM_get(tampon,( sizeof(int) * 100 ) );
    if( iarray[i] == 0 ) {
      fprintf(stdout,"Error: Memory allocation: hundred %d array\n",i);
      fflush(stdout);
      exit(1);
    }
  }

/* signed, float, double, decim, name and address are correlated.
   double and decim are equal too. Therefore we only generate 100
   distinct values for signed, float double, name and address,
   distribute and permute signed, and use signed as index to write
   the correlated values. */

  key_dense      (iarray[0], ntup);  
  int_sparse     (iarray[1], ntup);
  signed_hundred (iarray[2], ntup);
  float_hundred  (iarray[3]);
  double_hundred (iarray[4]); /* double and decim is
  decim_hundred  (iarray[5]);     correlated and equal */
  date_gen       (datearray,ntup);
  code_gen       (codearray,ntup);
  hundred_name   (namearray);
  hundred_address(addressarray);

  if ( ! ( OutputFile = fopen( "hundred", "w" ) ) ) {
    fprintf( stderr, "ERROR: Unable to open hundred\n");
    exit(1);
  }

  for(i=0;i<ntup;i++)
    fprintf(OutputFile, "%d,%d,%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"Filled\"\n",
      *(*(iarray+0) +i), *(*(iarray+1) +i), *(*(iarray+2) +i), 
      *(*(iarray+3) + (*(*(iarray+2)+i)-100)),
      *(*(iarray+4) + (*(*(iarray+2)+i)-100)),
      *(*(iarray+4) + (*(*(iarray+2)+i)-100)),
      datearray[i], codearray[i],
      namearray[(*(*(iarray+2)+i)-100)],
      addressarray[(*(*(iarray+2)+i)-100)] );

  fclose(OutputFile);
 /*  free(datearray);
     free(codearray);
     free(namearray);
     free(addressarray);
     for(i=0;i<ia;i++) { free( iarray[i] ); }
     free( iarray );  */ 

   MEM_free (tampon);

} 
@



/*
**  Function: key_dense
**  Purpose : Used in generation of KEY attribute for HUNDRED, UPDATES
**         distribution is dense uniform in range 0-ntup value 1 is missing
**         strategy is  populate vector containing values 1-ntup 
**         set keyarray[0] = 0 
*/
@c
key_dense(keyarray, ntup) int ntup; int keyarray[];
{
  int i;
  for(i = 0; i < ntup; i++) { keyarray[i] = i; }
  permute(keyarray, ntup);  
}
@

/*
**  Function: signed_sparse
**  Purpose : Used in generation of SIGNED attribute for UNIQUES, TENPCT, 
**         UPDATES distribution is sparse uniform in range +- 5 * 10**8
**        range is 10**9  steps are 10**9/ntup strategy is find interval 
**        = 10**9/ntup populate vector containing values 1-ntup multiplied
**        by interval subtract 10**8 to scale values
*/
@c
signed_sparse(signedarray, ntup) int ntup; int signedarray[];
{
  int interval, i;  

  interval = (int) ((int) pow(10.0,9.0) / ntup);
  for(i = 0; i < ntup; i++)
    { signedarray[i] = i*interval - ( (int) 5*pow(10.0,8.0)  ); }
  permute(signedarray, ntup);
}
@



/*
**  Function: uniques_decim
**  Purpose : Used to generate DECIM attribute for UNIQUES, UPDATES
**         distribution is sparse uniform in range +- 10**9
**        range is 2 * 10**9 steps are 2 * 10**9/ntup 
*/
@c
uniques_decim(decimarray, ntup) int ntup; int decimarray[];
{
  int interval, i;
  interval = (int)  (  (2 * (int)  pow(10.0,9.0) ) / ntup) ;
  for(i = 0; i < ntup; i++)
    { decimarray[i] = i * interval - (int) pow(10.0,9.0); }
  permute(decimarray, ntup);

  return;
}
@


/*
**  Function: date_gen
**  Purpose : used to generate DATE attribute for all tables
**      distribution is sparse uniform in range 1/1/1900 - 1/12/2000
**  range is 36860 (days)
**  random integer in range 1-36860
**   cannot use integer division and increment in loop (as other 
**  routines)  as often ntup > 36860     
**   one change of century in range, so have to worry about the 
**  odd day, and need to deal with ordinary leap years
*/
@c
date_gen(datearray, ntup) int ntup; char **datearray;
{
  char str[12];
  int  i, j, val;
  int year, days, yeardays, month_calc, day_calc;
  char month[4];
  int *indexarray;

  static int mths[12]= { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

  static char *mthstr[12] =
   { "jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"};

     

  indexarray= (int *)  MEM_get ( tampon, ( sizeof(int) * ntup ) ) ;
    
  /* generate integer values of date */

  for(i = 0; i < ntup; i++) {
    if(i==0) val = (int) ( 36860.0 * ran1(&INIT) );
    else val = (int) ( 36860.0 * ran1(&NORMAL) );
    indexarray[i] = val;
  }

  /* now change integer into date*/
  /* second main loop*/

  for(i = 0; i < ntup; i++) {
    /* now find the calendar year*/
    year =    ( (float) ( 99 - (indexarray[i]) / 365.25 ) );
    if (year < 0) year = (0 - year);

    mths[1] = 28;
    /* is it a leap year?*/
    if ( (!( year % 4 )) && ( year % 100 ) ) mths[1]=29;

    /* now  month and day*/
    yeardays = (int) (fmod ( (double) (indexarray[i]) , 365.25) );

    /* dec 31 is special as yeardays = 0*/
    if (yeardays == 0) {
      strcpy(month, "dec");
      days = 31; 
    }
    else {
      j = month_cal(yeardays, mths);

      strcpy(month, mthstr[ month_cal(yeardays, mths) ] );
      days = day_cal(yeardays, mths);
    }
    /* write in character representation to internal file */
    sprintf(str,"%d-%s-%d", days, month, year);
    strcpy((datearray[i]),str);
  }
   /*  free(indexarray); */

   MEM_unget(tampon, (char*) indexarray);
}
@


/*
**  Function: month_cal
**  Purpose :  integer number of month
**     days is no. days since 1-jan
** strategy is to compare no. days since 1-jan with no. at end
** of months until find greater value
*/
@c
int month_cal(days, mths) int days; int mths[];
{
  int yeardays, i;
  yeardays = 0;
  for(i = 0; i < 12; i++) {
    yeardays = yeardays + mths[i];
    if(days <= yeardays) return(i);
  }
  return(0);
}
@

/*
**  Function: day_cal
**  Purpose :  strategy is 
**     keep running total of days in months so far
**     compare with days
**     if total greater than days, 
**     subtract total days at end of previois month, return 
*/
@c
day_cal(days, mths) int days; int mths[];
{
  int yeardays;
  int i;
  yeardays = 0;
  for(i = 0; i < 12; i++) {
    yeardays = yeardays + mths[i];
    if (yeardays >= days) 
      return(days - (yeardays - mths[i]) );
  }
  return(0);
}
@
/*
**  Function: float_zipf
**  Purpose : used to generate FLOAT attribute for UNIQUES, UPDATES
**     distribution is Zipf-like range is +- 5 * 10**8     
**     10 distinct values definition of this distribution is    
**     rank * frequency = constant rank**z is 1-10     
**     frequencies sum to 1.0 constant is 1/H(z)    
**     where H(z) is the zth harmonic number, approximated by
**     ln(z) + G + 1/2*z + 1/12*z**2 + 1/120*z**4
**     G is Euler's constant = 0.5772156649 z = 2
**
** Parameters:
**
** Status    : 
*/

@c
float_zipf(floatarray, ntup) int ntup; int floatarray[];
{
  int  i, j, k,  interval, noofinterval, rangemin, range;
  double gamma, zipfconst, z;
  int num;
  range = (int) pow(10.0,9.0);
  rangemin = -5 * (int) pow(10.0,8.0);
  noofinterval = 9;
  interval = range / noofinterval;
  z = 2.0;
  k = 0;
  gamma = 0.5772156649;    /*Euler's constant*/
  zipfconst = 1.0 /  ( log(z) + gamma + 1 / (2*z) + 1 / (12 * pow(z,2.0) ) 
       + 1 / ( 120 * pow(z,4.0) ) );

  for(j = 1; j < 11; j++) {
    num = (int) ( (zipfconst / pow((double) j,z) ) * (double) ntup);

    for(i = 0; i < num; i++) {
      k++;
      floatarray[k] = rangemin + interval * j;
    }
  }
  permute(floatarray, ntup);
}
@

/*
**  Function:   updates_double()
**  Purpose :   Used in generation of DOUBLE attribute for UNIQUES
**     distribution is normal in range +- 10**9 mean is 0
**      std deviation is 241700000 (observation from distribution tape)
**    strategy is to generate single value using NAG routine
**     don't need array as duplicates allowed
**
** Parameters:
**
** Status    : Tested 
*/
@c
updates_double(doublearray,ntup) int doublearray[]; int ntup;
{
  int duplicates=1;
  int i,j, tmp;

  doublearray[0]= (int) (gasdev(&INIT) * 241700000.0);
  for(i=1;i<ntup; i++) {
    doublearray[i]= (int) (gasdev(&NORMAL) * 241700000.0);
  }

  while ( duplicates > 0 ) {
       duplicates = 0;

        qsort( doublearray, ntup, sizeof(int), intcompare );

        for ( i = 0 ; i < ntup ; i++ ) {
            if ( doublearray[i] == doublearray[i+1] ) {
                doublearray[i] = ANINT;
                duplicates++;
            }
        }

        for ( i = 0 ; i < ntup ; i++ ) {
            if ( doublearray[i] == ANINT ) 
      doublearray[i] = (int) (gasdev(&NORMAL) * 241700000.0);
        }
    }
}  
@

/*
**  Function:   int_dense
**  Purpose :    Used in generation of INT attribute for UPDATES
**     distribution is dense uniform in range 0-ntup
**     value 1 is missing strategy is     
**     populate vector containing values 1-ntup 
**     set intarray[0] = 0 
**
**
** Parameters:
**
** Status    : Tested
*/

@c
int_dense(intarray, ntup) int ntup; int intarray[];
{
  int i;
  for(i = 0; i < ntup; i++) { intarray[i] = i; }
  intarray[0] = 0;
  permute(intarray, ntup);  
}
@

/*
**  Function:   code_gen
**  Purpose :    Used to generate CODE attribute for UNIQUES, HUNDRED, 
**    TENPCT, UPDATES attribute has ntup unique values,
**     10 chars long uses characters  0-9, A-Z (ASCII 48-57, 65-90)
**     strategy is to generate unique values by varying rightmost
**    values systematically then permuting vector
**
** Parameters:
**
** Status    : Tested
*/

@c
code_gen(codearray,  ntup) int ntup; char **codearray;
{
  int i, j, k, val, codeval[10];
  int *indexarray;
  char codestr[10], tmpc[11];


  
  indexarray= (int *)  MEM_get(tampon,( sizeof(int) * ntup ) );
  for (i=0;i<ntup;i++) indexarray[i] = i;
  permute(indexarray, ntup);

  for(i = 0; i < 10; i++) {
    if(i==0) val = (int) (36.0 * ran1(&INIT) ) + 48;
        else val = (int) (36.0 * ran1(&NORMAL) ) + 48;    
    if (val > 57) val = val + 7;
    codeval[i] = val;
  }
  for(i=0;i<ntup;i++) {
    for(j=0;j<10;j++) codestr[j] = (char) codeval[j];
    for(j=0;j<10;j++) tmpc[j]= codestr[j];
    tmpc[10] = '\0';
    strcpy(codearray[ indexarray[i] ], tmpc);
    codeval[9]=codeval[9] +1;
    for(j = 9; j >= 0; j--) {
      if ( codeval[j] > 90 ) {    
        codeval[j] = 48;
        codeval[j-1]++;
      }
      if( codeval[0] > 90) {    	     
        for(k = 0; k > 10; k++) { codeval[k] = 48; }
      }
      if( (codeval[j] > 57) && (codeval[j] < 65)) 
        codeval[j] = 65;
    }
  }
  strcpy(codearray[ 0 ], "BENCHMARKS" );
 /* free(indexarray); */
 MEM_unget(tampon, (char*) indexarray);
}
@


/*
**  Function:   uniques_name
**  Purpose :    Used to generate NAME attribute for UNIQUES, 
**     UPDATES attribute has ntup unique values,
**     20 chars long uses characters  0-9, A-Z (ASCII 48-57, 65-90)
**     strategy is to generate unique values by varying rightmost
**    values systematically then permuting vector
**
** Parameters:
**
** Status    : Tested
*/

@c
uniques_name(namearray,  ntup) int ntup; char **namearray;
{
  int i, j, l, val, nameval[20];
  int *indexarray;
  char namestr[20], tmpc[21];
    
  indexarray= (int *)  MEM_get (tampon, ( sizeof(int) * ntup ) );
  for (i=0;i<ntup;i++) indexarray[i] = i;
  permute(indexarray, ntup);

  /* generate integer values to initialize */
  for(i = 0; i < 20; i++) {
    if(i==0) val = (int) (36.0 * ran1(&INIT) ) + 48;
    else val = (int) (36.0 * ran1(&NORMAL) ) + 48;    
    if (val > 57) val = val + 7;
    nameval[i] = val;
  }

  for(i=0;i<ntup;i++) {
    for(j=0;j<20;j++) namestr[j] = (char) nameval[j];
    for(j=0;j<20;j++) tmpc[j]= namestr[j];
    tmpc[20] = '\0';
    strcpy(namearray[ indexarray[i] ], tmpc);
    nameval[19]=nameval[19] +1;
    for(j = 19; j >= 0; j--) {
      if ( nameval[j] > 90 ) {    
        nameval[j] = 48;
        nameval[j-1]++;
      }
      if( nameval[0] > 90) {    	     
        for(l = 0; l > 20; l++) { nameval[l] = 48; }
      }
      if( (nameval[j] > 57) && (nameval[j] < 65)) 
        nameval[j] = 65;
    }
  }
  strcpy(namearray[0], "THE+ASAP+BENCHMARKS+" );
    /*  free(indexarray);  */
   MEM_unget (tampon,(char*) indexarray);
}
@


/*
**  Function:   uniques_address
**  Purpose :    Used to generate ADDRESS attribute for UNIQUES,  UPDATES 
**     attribute is between 2-80 chars long, average 20
**   values are not unique (but should be approximately 80% unique)
**   The distribution of length of values is a Poisson distribution
**   calculated using NAG routines, with a check to exclude
**   values above 80 (which are rare).
**   The mean of the target distribution is 20, but the distribution
**   is calculated with a mean of 18 and all values are scaled by +2
**   to eliminate values of 0, 1 without changing the distribution.
**   uses characters 0-9, A-Z (ASCII 48-57, 65-90)
**   strategy is to generate values by varying leftmost values
**   systematically,selecting right no. chars, then permuting an index 
**  vector
**    
** Parameters:
**
** Status    : Tested
*/
@c
uniques_address(addressarray,  ntup) int ntup; char **addressarray;
{
  int  nameval[80], i, j, l, nchar, val;
  char namestr[80];
  char tmpc[81];
  int nr;  
  int *indexarray;

  indexarray= (int *)  MEM_get(tampon, ( sizeof(int) * ntup ) );
  
  for (i=0;i<ntup;i++) indexarray[i] = i;
  permute(indexarray, ntup);
  
  for(i = 0; i < 80; i++) {
    if(i==0) val = (int) (36.0 * ran1(&INIT) ) + 48;
    else val = (int) (36.0 * ran1(&NORMAL) ) + 48;  
    if (val > 57) val = val + 7;
    nameval[i] = val;
  }
     
  for(i=0;i<ntup;i++) {
    if(i==0) nchar=  (int) ( 2 + poidev(18.0, &INIT)   );
    else nchar=  (int) ( 2 + poidev(18.0, &NORMAL) );
   
    if(nchar > 80) nchar=80;
    for(j=0;j<nchar;j++) namestr[j]= (char) nameval[j];
    for(j=nchar+1;j<80;j++) namestr[j] = '\0';
    for(j=0;j<80;j++) tmpc[j]= namestr[j];
    tmpc[80] = '\0';
    strcpy(addressarray [ indexarray[i] ], tmpc);
    nameval[0]=nameval[0] + 1; /*increment leftmost */
    for(j=0;j<80;j++) {
      if(nameval[j] > 90) {
        nameval[j] = 48;
        if(j < 79)
            nameval[j+1] = nameval[j+1] +1;
      }
      if( nameval[79] > 90 ) {
        for(l=0;l<80;l++) nameval[l] = 48;
      }
      if( (nameval[j] > 57) && (nameval[j] < 65)) 
        nameval[j] = 65;
    }
  }
 
 
  strcpy(addressarray [0], "SILICON VALLEY" );
   /* free(indexarray);  */
   MEM_unget ( tampon, (char*) indexarray);

}
@

/*
**  Function:   int_sparse
**  Purpose :   Used in generation of INT attribute for UNIQUES, HUNDRED, TENPCT
**    distribution is sparse uniform in range 1-10**9
**    steps are 10**9/ntup value 1 is missing (uses 1000 instead)
**     strategy is find interval = 10**9/ntup
**     populate vector containing values 1-ntup multiplied by interval
**     set intarray(1) = 1000 - works for ntup < 2M
**
** Parameters:
**
** Status    : Tested
*/
@c
int_sparse(intarray, ntup) int ntup; int intarray[];
{
  int interval, i;

  interval= (int) pow(10.0,9.0) / ntup;
  
  for(i=0;i<ntup;i++) intarray[i]= i * interval;
  intarray[0]=1000;
  permute(intarray,ntup);
}
@



/*
**  Function:   signed_hundred
**  Purpose :   Used in generation of SIGNED attribute for HUNDRED
**    distribution is uniform in range 100-199 strategy is
**    populate vector containing values 100-199 using counter
**     reset counter after every 100 iterations 
**
** Parameters:
**
** Status    : Tested
*/

@c
signed_hundred(signedarray, ntup) int ntup; int signedarray[];
{
  int i, j=100;
  for(i=0;i<ntup;i++) {
          signedarray[i] = j;
          j++;
          if ( j > 199 ) j = 100;
        }
  permute(signedarray,ntup);
}
@


/*
**  Function:   float_hundred
**  Purpose :   Used to generate FLOAT attribute for HUNDRED
**    distribution is uniform in range +-5 * 10**8, 100 distinct values
**    range is 10**9 steps are 10**9/100 = 10**7 strategy is
**    generate integer vector of length ntup, containing values 1-100 
**     subtract 5 * 10**8 to scale values permute integer vector
** Parameters:
**
** Status    : Tested
*/

@c
float_hundred(floatarray) int floatarray[];
{
  int i, interval;
  interval = (int)  pow(10.0,7.0);
  for(i=0;i<100;i++) {
    floatarray[i]= (int) (i * interval - 5 * pow(10.0,8.0) );
  }
  permute(floatarray,100);
}
@

/*
**  Function:   double_hundred
**  Purpose :   Used to generate DOUBLE attribute for HUNDRED
**    distribution is uniform in range +- 10**9, 100 distinct values
**    range is 2 * 10**9 steps are 2 * 10**7 strategy is
**     generate integer vector of length ntup, containing values 1-100 
**     generate values for vector subtract 10**9 to scale values 
**     permute integer vector
** Parameters:
**
** Status    : Tested
*/

@c
double_hundred(doublearray) int doublearray[];
{
  int i, interval;
  interval = (int) pow(10.0,7.0) * 2;

  for(i=0;i<100;i++) {
    doublearray[i]= (int) (i * interval - pow(10.0,9.0) );
  }
  permute(doublearray,100);
}
@


/*
**  Function:   decim_hundred
**  Purpose :   Used to generate DECIM attribute for HUNDRED
**     distribution is uniform in range +- 10**9, 100 distinct values
**    range is 2 * 10**9 steps are 2 * 10**7
**
**  Parameters:
**
** Status    : Tested
*/

@c
decim_hundred(decimarray, ntup) int ntup; int decimarray[];
{
  int i, interval;
  interval = (int) pow(10.0,7.0) * 2;
  for(i=0;i<100;i++) {
    decimarray[i]= (int) (i * interval - pow(10.0,9.0) );
  }
  permute(decimarray,100);
}
@

/*
**  Function:   hundred_name
**  Purpose :   Used to generate NAME attribute for HUNDRED
**    attribute has 100 unique values, 20 chars long
**    uses characters 0-9, A-Z (ASCII 48-57, 65-90)    
**    strategy is to generate unique values by varying rightmost values
**    systematically then permuting an index vector
**
**  Parameters:
**
** Status    : Tested
*/


@c
hundred_name(namearray) char **namearray;
{
  int i, j, k, p, val, nameval[20];
  char namestr[20], tmpc[21];

  for(i = 0; i < 20; i++) {
    if(i==0) val = (int) (36.0 * ran1( &INIT ) ) + 48;
        else val = (int) (36.0 * ran1(&NORMAL) ) + 48;    
    if (val > 57) val = val + 7;
    nameval[i] = val;
  }

  for(i=0;i<100;i++) {
    for(j=0;j<20;j++) namestr[j] = (char) nameval[j];
    for(p=0;p<20;p++) tmpc[p]= namestr[p];
    tmpc[20] = '\0';
    strcpy( namearray[i], tmpc);

    /* Increment the left most character. If overflow occurs increment
       the ones to the right of the left most. */

    nameval[19]=nameval[19] +1;
    for(j = 19; j >= 0; j--) {
      if ( nameval[j] > 90 ) {
  nameval[j] = 48;
  nameval[j-1]++;
      }
      if( nameval[0] > 90) {
        for(k=0;k<20;k++)
          { nameval[k] = 48; }
      }
      if( (nameval[j] > 57) && (nameval[j] < 65)) nameval[j] = 65;
    }
  }
  strcpy( namearray[0], "THE+ASAP+BENCHMARKS+" );
}
@

/*
**  Function:   hundred_address
**  Purpose :   Used to generate ADDRESS attribute for HUNDRED
**    attribute is between 2-80 chars long, average 20
**    approximately 100 distinct values
**    The distribution of length of values is a Poisson distribution
**    with a check to exclude values above 80 (which are rare).
**    The mean of the target distribution is 20, but the distribution
**    is calculated with a mean of 18 and all values are scaled by +2
**    to eliminate values of 0, 1 without changing the distribution.
**    uses characters 0-9, A-Z (ASCII 48-57, 65-90)
**    strategy is to generate values by varying leftmost values 
**    systematically 
**    selecting right no. chars, then permuting an index vector
**  Parameters:
**
** Status    : Tested
*/

@c
hundred_address(addressarray) char **addressarray;
{
  int  nameval[80], i, p, nr, j, k, nchar, val;
  char namestr[80];
  char tmpc[81];

  for(i = 0; i < 80; i++) {
    if(i==0) val = (int) (36.0 * ran1(&INIT) ) + 48;
    else val = (int) (36.0 * ran1(&NORMAL) ) + 48;  
    if (val > 57) val = val + 7;
    nameval[i] = val;
  }

  for(i=0;i<100;i++) {
    if(i==0) nchar=  (int) ( 2 + poidev(18.0, &INIT)   );
        else nchar=  (int) ( 2 + poidev(18.0, &NORMAL) );
    if(nchar > 80) nchar=80;
    for(j=0;j<nchar;j++) namestr[j]= (char) nameval[j];
    for(j=nchar+1;j<80;j++) namestr[j] = '\0';
    for(p=0;p<80;p++) tmpc[p]= namestr[p];
    tmpc[80] = '\0';
    strcpy(addressarray[i], tmpc);
    nameval[0]=nameval[0] + 1; /*increment leftmost */

    for(j=0;j<80;j++) {
      if(nameval[j] > 90) {
        nameval[j] = 48;
        if(j < 79) nameval[j+1] = nameval[j+1] +1;
      }
      if( nameval[79] > 90 ) {
        for(k=0;k<80;k++)
          nameval[k] = 48;
      }
      if( (nameval[j] > 57) && (nameval[j] < 65)) 
        nameval[j] = 65;
    }
  }
  strcpy( addressarray [ 0 ], "SILICON VALLEY" );
}
@



/*
**  Function: key_sparse
**  Purpose : Used in generation of KEY attribute for UNIQUES, TENPCT
**         distribution is sparse uniform in range 1-10**9
**         steps are 10**9/ntup value 1 is missing (uses 1000 instead) 
**         strategy is find interval = 10**9/ntup
**        populate vector containing values 1-ntup multiplied by interval
**         set keyarray[0] = 1000 - works for ntup < 2M
** Parameters:
**
** Status    :Tested
*/

@c
key_sparse(keyarray, ntup) int ntup; int keyarray[];
{
  int interval, i;

  interval = (int) ((int) pow(10.0,9.0) / ntup);


  for(i = 0; i < ntup; i++) { keyarray[i] = i*interval; }

  keyarray[0] = 1000;

  permute(keyarray, ntup);

  return;
}
@


/*
**  Function:   double_normal
**  Purpose :   Used in generation of DOUBLE attribute for UNIQUES
**     distribution is normal in range +- 10**9 mean is 0
**      std deviation is 241700000 (observation from distribution tape)
**    strategy is to generate single value using NAG routine
**     don't need array as duplicates allowed
**
**
**
** Parameters:
**
** Status    : Tested 
*/

@c
double_normal(doublearray,ntup) int doublearray[]; int ntup;
{
  int i,j, tmp;

  doublearray[0]= (int) (gasdev(&INIT) * 241700000.0);
  for(i=1;i<ntup; i++) {
    doublearray[i]= (int) (gasdev(&NORMAL) * 241700000.0);
  }
}  
@

/*
**  Function:   double_tenpct
**  Purpose :   Used to generate DOUBLE attribute for TENPCT
**    distribution is uniform in range +- 10**9, ntup/10 distinct values
**    range is 2 * 10**9 steps are 2 * 10**9/(ntup/10)    
**    strategy is generate integer vector of length ntup, containing 
**    values 1- (ntup/10) generate values for vector
**    subtract 10**9 to scale values permute integer vector
**
** Parameters:
**
** Status    : Tested 
*/

@c
double_tenpct(doublearray,ntup) int doublearray[]; int ntup;
{
int i,j=0,interval;

  interval= (int) (2 * pow(10.0, 9.0) ) / ( ntup / 10);
  for(i=0;i<ntup;i++) {
    j++;
    if(j > (ntup/10) ) j=1; /*reset counter if necessary */
    doublearray[i] = j * interval - pow(10.0, 9.0);
  }
  permute(doublearray, ntup);
}
@

/*
**  Function:   decim_tenpct
**  Purpose :   Used to generate DECIM attribute for TENPCT
**    distribution is uniform in range +- 10**9, 10 distinct values
**    range is 2 * 10**9 steps are 2 * 10**8
*/

@c
decim_tenpct(decimarray,ntup) int decimarray[]; int ntup;
{
int i,j=0,interval;
  
  interval = (int) 2 * pow(10.0, 8.0);
  for(i=0;i<ntup;i++) {
  j++;
  if(j > 10 ) j=0; 
    decimarray[i] = j * interval - pow(10.0, 9.0);
  }
  permute(decimarray, ntup);
}  
@

/*
**  Function:   tenpct_name
**  Purpose :   Used to generate NAME attribute for HUNDRED
**    attribute has 100 unique values, 20 chars long
**    uses characters 0-9, A-Z (ASCII 48-57, 65-90)    
**    strategy is to generate unique values by varying rightmost values
**    systematically then permuting an index vector
*/

@c
tenpct_name(namearray,  ntup) int ntup; char **namearray;
{
  int i, j,k=0, l,p, br,val,nooftup,noofval,  nameval[20];
  int *indexarray;
  char namestr[20], tmpc[21];
    
  indexarray= (int *)  MEM_get (tampon, (sizeof(int) * ntup ) );
  for (i=0;i<ntup;i++) indexarray[i] = i;
  permute(indexarray, ntup);
  k=0;
  noofval = 10; /* nearestint( (float) ntup / 10.0); */
  nooftup= nearestint ( (float) ntup/ (float) noofval ); /* ntup not multiple of 100 */
  /* generate integer values to initialize */
    
  for(i = 0; i < 20; i++) {
    if(i==0) val = (int) (36.0 * ran1(&INIT) ) + 48;
        else val = (int) (36.0 * ran1(&NORMAL) ) + 48;    
    if (val > 57) val = val + 7;
    nameval[i] = val;
  }
  for(i=0;i<noofval;i++) {
    for(j=0;j<20;j++) namestr[j] = (char) nameval[j];
    br=0;
    for(j=0;j<nooftup;j++) {
         k++;
      if(k > (ntup)  ) { br=1; break; }
      for(p=0;p<20;p++) tmpc[p]= namestr[p];
      tmpc[20] = '\0';
      strcpy(namearray[ indexarray[k-1] ], tmpc);
    }
   /* if(br) { free(indexarray); return; } */
      if(br) {  MEM_unget (tampon, (char*) indexarray); return; }
    nameval[19]=nameval[19] +1;
    for(j = 19; j >= 0; j--) {
      if ( nameval[j] > 90 ) {    
        nameval[j] = 48;
        nameval[j-1]++;
      }
      if( nameval[0] > 90) {    	     
        for(l = 0; l > 20; l++) { nameval[l] = 48; }
      }
      if( (nameval[j] > 57) && (nameval[j] < 65)) 
        nameval[j] = 65;
    }
  }
 /* free(indexarray); */
    MEM_unget (tampon, (char*) indexarray);
}
@


/*
**  Function:   tenpct_address
**  Purpose :   Used to generate ADDRESS attribute for TENPCT
**    attribute is between 2-80 chars long, average 20
**    approximately ntup/10 distinct values
**    The distribution of length of values is a Poisson distribution
**    calculated using NAG routines, with a check to exclude
**    values above 80 (which are rare).
**    The mean of the target distribution is 20, but the distribution
**    is calculated with a mean of 18 and all values are scaled by +2
**    to eliminate values of 0, 1 without changing the distribution.
**    uses characters 0-9, A-Z (ASCII 48-57, 65-90)
**    strategy is to generate values by varying leftmost values 
**    systematically selecting right no. chars, then permuting an index vector
*/

@c
tenpct_address(addressarray,  ntup) int ntup; char **addressarray;
{
  int  nameval[80], i, p,j, br,k=0,l,nooftup, nchar, val;
  char namestr[80];
  char tmpc[81];
  int nr;  
  int *indexarray;

  indexarray= (int *)  MEM_get (tampon, sizeof(int) * ntup);
  
  for (i=0;i<ntup;i++) indexarray[i] = i;
  permute(indexarray, ntup);
  nooftup = 1 + ntup/100;  /*no. occurences of each value*/

  for(i = 0; i < 80; i++) {
    if(i==0) val = (int) (36.0 * ran1(&INIT) ) + 48;
        else val = (int) (36.0 * ran1(&NORMAL) ) + 48;  
    if (val > 57) val = val + 7;
    nameval[i] = val;
  }
  for(i=0;i<ntup;i++) {
    if(i==0) nchar=  (int) ( 2 + poidev(18.0, &INIT)   );
        else nchar=  (int) ( 2 + poidev(18.0, &NORMAL) );
    if(nchar > 80) nchar=80;
    for(j=0;j<nchar;j++) namestr[j]= (char) nameval[j];
    for(j=nchar+1;j<80;j++) namestr[j] = '\0';
    br=0;
    for(j=0;j<nooftup;j++) {
      k++;
      if(k > (ntup)  ) { br=1; break; }
      for(p=0;p<80;p++) tmpc[p]= namestr[p];
      tmpc[80] = '\0';
      strcpy(addressarray [ indexarray[k-1] ], tmpc);
    }
    
 /* if(br) { free(indexarray); return; } */
      if(br) {  MEM_unget (tampon, (char*) indexarray); return; }
    nameval[0]=nameval[0] + 1; /*increment leftmost */
    for(j=0;j<80;j++) {
      if(nameval[j] > 90) {
        nameval[j] = 48;
        if(j < 79) nameval[j+1] = nameval[j+1] +1;
      }
      if( nameval[79] > 90 ) {
        for(l=0;l<80;l++)
          nameval[l] = 48;
      }
      if( (nameval[j] > 57) && (nameval[j] < 65)) 
        nameval[j] = 65;
    }
  }
 /* free(indexarray) */

 MEM_unget (tampon, (char*) indexarray);
}
@



/*
**  Function: permute
**  Purpose : used to permute vector of integers - general purpose routine
**            Doen't permute the very first element.
*/

@c
permute(intarray, ntup) int ntup; int intarray[];
{
  int mtup, temp, i, swap;

  mtup = ntup - 1;
  for(i = 1;  i < ntup; i++) {
    if(i==0) swap = (int) ( ran1(&INIT)  * mtup ) + 1;
        else swap = (int) ( ran1(&NORMAL)  * mtup ) + 1;
    temp = intarray[swap];
    intarray[swap] = intarray[i];
    intarray[i] = temp;
  }
}
@

@c
nearestint (rvar) float rvar;
{
     int ivar;

     ivar = (int) rvar;
     if ( (float) (ivar+0.5) >  rvar)
                return (ivar);
     else
                return (ivar+1);
}
@
