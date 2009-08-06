#!/usr/bin/env bash
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