# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

use Test::More tests => 5;

require DBI;
pass 'DBI required';

import DBI;
pass 'DBI imported';

$switch = DBI->internal;
is ref $switch,'DBI::dr','switch';

$drh = DBI->install_driver('monetdb');
is ref $drh,'DBI::dr','drh';

ok $drh->{Version},'Version: ' . $drh->{Version};
