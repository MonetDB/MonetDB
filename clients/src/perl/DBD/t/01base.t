
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
