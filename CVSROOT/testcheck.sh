#!/bin/sh

# we're not interested in the repository
shift

for file; do
    case "$file" in
	# we're currently only checking test output
	*.stable.out*|*.stable.err*)
	# you're always allowed to remove test output
	if test -f "$file"; then
	    # you're never allowed to commit a test that failed with a signal
	    if grep -q '^!Mtimeout: signal' "$file"; then
		echo "You are not allowed to commit this change to $file"
		echo "You are trying to approve a test which failed with a signal."
		echo "This is never acceptable."
		exit 1
	    fi
	fi
	;;
    esac
done
exit 0
