shift
# this rewrites libtool linker commands into ar commands 
# because windows gcc cannot have dangling references
if [ "$1" = "--mode=link" ]; then
	shift
	shift
	i=1
	out=1
	j=$#
	call="ar rvsT"
	while [ $i -le $j ]
	do
		case "$1" in
		-rpath) 
		shift # cough, cough
		# TODO
		;;
		-version-info) ;;
		1) ;;
		-module) ;;
		-avoid-version) ;;
		-m64) ;;
		-O3) ;;
		/monetdb5) ;; # TODO: find nicer way of skipping these.
		-D*) ;;
		-I*) ;;
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
elif [ "$1" = "cp" ]; then
	"$@"
else
	shift
	echo "$@"
	"$@"
fi
