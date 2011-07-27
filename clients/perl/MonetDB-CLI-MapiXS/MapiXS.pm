package MonetDB::CLI::MapiXS;

our $VERSION = '0.03';

use XSLoader();

XSLoader::load __PACKAGE__;

=head1 NAME

MonetDB::CLI::MapiXS - MonetDB::CLI implementation, using libMapi

=head1 DESCRIPTION

MonetDB::CLI::MapiXS is an implementation of the MonetDB call level interface
L<MonetDB::CLI>.
It uses L<libMapi> - the MonetDB Application Programming Interface.
Normally, you don't use this module directly, but let L<MonetDB::CLI>
choose an implementation module.

=head1 AUTHORS

Steffen Goeldner E<lt>sgoeldner@cpan.orgE<gt>.

=head1 COPYRIGHT AND LICENCE

The contents of this file are subject to the MonetDB Public License
Version 1.1 (the "License"); you may not use this file except in
compliance with the License. You may obtain a copy of the License at
http://www.monetdb.org/Legal/MonetDBLicense

Software distributed under the License is distributed on an "AS IS"
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
License for the specific language governing rights and limitations
under the License.

The Original Code is the MonetDB Database System.

The Initial Developer of the Original Code is CWI.
Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
Copyright August 2008-2011 MonetDB B.V.
All Rights Reserved.

=head1 SEE ALSO

=head2 MonetDB

  Homepage    : http://www.monetdb.org/

=head2 Perl modules

L<MonetDB::CLI>, L<libMapi>

=cut
