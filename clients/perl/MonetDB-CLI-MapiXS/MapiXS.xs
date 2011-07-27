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

/*

  Author: Steffen Goeldner <sgoeldner@cpan.org>

*/

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "MapiXS.h"

typedef Mapi    MonetDB__CLI__MapiXS__Cxn;
typedef MapiHdl MonetDB__CLI__MapiXS__Req;

MODULE = MonetDB::CLI::MapiXS  PACKAGE = MonetDB::CLI::MapiXS

PROTOTYPES: DISABLE

MonetDB::CLI::MapiXS::Cxn
connect( CLASS, host, port, username, password, lang )
    const char* CLASS
    const char* host
    int         port
    const char* username
    const char* password
    const char* lang
  CODE:
    RETVAL = mapi_connect( host, port, username, password, lang );
    if ( RETVAL == NULL ) {
      croak("Handle is undefined");
    }
    if ( RETVAL->error ) {
      croak( RETVAL->errorstr );
    }
  OUTPUT:
    RETVAL


MODULE = MonetDB::CLI::MapiXS  PACKAGE = MonetDB::CLI::MapiXS::Cxn

MonetDB::CLI::MapiXS::Req
query( self, statement )
    MonetDB::CLI::MapiXS::Cxn self
    const char* statement
    SV* cxn = ST(0);
  CODE:
    RETVAL = mapi_query( self, statement );
    if ( RETVAL == NULL ) {
      croak("Handle is undefined (%s)", self->errorstr );
    }
    if ( RETVAL->result && RETVAL->result->errorstr ) {
      croak( RETVAL->result->errorstr );
    }
    if ( self->error ) {
      croak( self->errorstr );
    }
  OUTPUT:
    RETVAL
  CLEANUP:
    SvPVX   ( SvRV( ST(0) ) ) = (char*)cxn;
    SvROK_on( SvRV( ST(0) ) );
    SvREFCNT_inc( cxn );

MonetDB::CLI::MapiXS::Req
new_handle( self )
    MonetDB::CLI::MapiXS::Cxn self
    SV* cxn = ST(0);
  CODE:
    RETVAL = mapi_new_handle( self );
    if ( RETVAL == NULL ) {
      croak("Handle is undefined (%s)", self->errorstr );
    }
    if ( RETVAL->result && RETVAL->result->errorstr ) {
      croak( RETVAL->result->errorstr );
    }
    if ( self->error ) {
      croak( self->errorstr );
    }
  OUTPUT:
    RETVAL
  CLEANUP:
    SvPVX   ( SvRV( ST(0) ) ) = (char*)cxn;
    SvROK_on( SvRV( ST(0) ) );
    SvREFCNT_inc( cxn );

void
DESTROY( self )
    MonetDB::CLI::MapiXS::Cxn self
  CODE:
    mapi_destroy( self );
    if ( self->error ) {
      croak( self->errorstr );
    }


MODULE = MonetDB::CLI::MapiXS  PACKAGE = MonetDB::CLI::MapiXS::Req

void
query( self, statement )
    MonetDB::CLI::MapiXS::Req self
    const char* statement
  CODE:
    mapi_query_handle( self, statement );
    if ( self->result && self->result->errorstr ) {
      croak( self->result->errorstr );
    }
    if ( self->mid->error ) {
      croak( self->mid->errorstr );
    }

int
querytype( self )
    MonetDB::CLI::MapiXS::Req self
  CODE:
    RETVAL = mapi_get_querytype( self );
    if ( self->mid->error ) {
      croak( self->mid->errorstr );
    }
  OUTPUT:
    RETVAL

int
id( self )
    MonetDB::CLI::MapiXS::Req self
  CODE:
    RETVAL = mapi_get_tableid( self );
    if ( self->mid->error ) {
      croak( self->mid->errorstr );
    }
  OUTPUT:
    RETVAL

int
rows_affected( self )
    MonetDB::CLI::MapiXS::Req self
  CODE:
    RETVAL = mapi_rows_affected( self );
    if ( self->mid->error ) {
      croak( self->mid->errorstr );
    }
  OUTPUT:
    RETVAL

int
columncount( self )
    MonetDB::CLI::MapiXS::Req self
  CODE:
    RETVAL = mapi_get_field_count( self );
    if ( self->mid->error ) {
      croak( self->mid->errorstr );
    }
  OUTPUT:
    RETVAL

char*
name( self, fnr )
    MonetDB::CLI::MapiXS::Req self
    int fnr
  CODE:
    RETVAL = mapi_get_name( self, fnr );
    if ( self->mid->error ) {
      croak( self->mid->errorstr );
    }
  OUTPUT:
    RETVAL

char*
type( self, fnr )
    MonetDB::CLI::MapiXS::Req self
    int fnr
  CODE:
    RETVAL = mapi_get_type( self, fnr );
    if ( self->mid->error ) {
      croak( self->mid->errorstr );
    }
  OUTPUT:
    RETVAL

int
length( self, fnr )
    MonetDB::CLI::MapiXS::Req self
    int fnr
  CODE:
    RETVAL = mapi_get_len( self, fnr );
    if ( self->mid->error ) {
      croak( self->mid->errorstr );
    }
  OUTPUT:
    RETVAL

int
fetch( self )
    MonetDB::CLI::MapiXS::Req self
  CODE:
    RETVAL = mapi_fetch_row( self );
    if ( self->mid->error ) {
      croak( self->mid->errorstr );
    }
  OUTPUT:
    RETVAL

char*
field( self, fnr )
    MonetDB::CLI::MapiXS::Req self
    int fnr
  CODE:
    RETVAL = mapi_fetch_field( self, fnr );
    if ( self->mid->error ) {
      croak( self->mid->errorstr );
    }
  OUTPUT:
    RETVAL

void
finish( self )
    MonetDB::CLI::MapiXS::Req self
  CODE:
    mapi_finish( self );
    if ( self->mid->error ) {
      croak( self->mid->errorstr );
    }

void
DESTROY( self )
    MonetDB::CLI::MapiXS::Req self
  CODE:
    if ( mapi_close_handle( self ) ) {
      if ( self->mid->error ) {
        croak( self->mid->errorstr );
      }
    }
