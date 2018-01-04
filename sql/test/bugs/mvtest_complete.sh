#!/bin/sh

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

# rename all entries and update CVS
echo "$1.sql"
mv $1.sql $2.sql
cvs rm $1.sql
cvs add $2.sql

cd Tests
sh mvtest.sh $1 $2
