#!/bin/sh

# we're not interested in the repository
shift

for file; do
    if test ! -f "$file"; then
	# you're always allowed to remove files
	continue
    fi

    case "$file" in
    *.stable.out* | *.stable.err*)
	# you're never allowed to commit a test that failed with a signal
	if grep -q '^!Mtimeout: signal' "$file" || grep -q '^!ERROR: BATSIGcrash' "$file"; then
	    echo "Pre-commit check failed:"
	    echo "You are not allowed to commit this change to $file"
	    echo "You are trying to approve a test which failed with a signal."
	    echo "This is never acceptable."
	    exit 1
	fi
	;;
#     *.py | *.py.in)
# 	# TABs and trailing white space are not allowed in Python source
# 	if grep -q $'\t\\| $' "$file"; then
# 	    echo "Pre-commit check failed:"
# 	    echo "The file \"$file\" contains TABs and/or trailing white space."
# 	    echo "Change TABs to spaces and remove any trailing white space, then try again."
# 	    exit 1
# 	fi
# 	;;
    esac
done
exit 0
