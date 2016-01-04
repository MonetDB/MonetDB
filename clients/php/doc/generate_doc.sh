#!/usr/bin/env bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

#
#
# Generates (html) documentation from source code comments.
#
# Requires: phpdoc (http://www.phpdoc.org/)
#
# Usage: generate_doc.sh <source dir> <doc dir>
#
# By default <doc dir> is located within the source tree 'doc/' directory
#

source_code=$1

current_dir=$(pwd)
doc_dir=$current_dir/$2

title="MonetDB5 PHP API"

phpdoc -ti $title -o "HTML:frames:phpedit" -f $source_code -t $doc_dir
