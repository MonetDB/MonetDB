#!/usr/bin/env bash

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

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
