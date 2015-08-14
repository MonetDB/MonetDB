shift
# this rewrites libtool linker commands into ar commands 
# because windows gcc cannot have dangling references

if [ "$1" = "--mode=link" ]; then
	shift
	shift
	i=1
	out=1
	j=$#
	call="ar rvs"
	while [ $i -le $j ]
	do
		case "$1" in
		-rpath) ;;
		-version-info) ;;
		-D*) ;;
		-o) out=1 ;; 
		*)
		call="$call $1"
		;;
		esac
		shift
		i=`expr $i + 1`
	done
	echo $call
	$call
	
else
	shift
	echo "$@"
	"$@"
fi
