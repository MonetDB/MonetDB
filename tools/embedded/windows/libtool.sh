shift
if [ "$1" = "--mode=link" ]; then
	shift
	shift
	j=$#
	i=1
	while [ $i -le $j ]
	do
		case "$1" in
		-o) touch $2 ;; 
		*) ;;
		esac
		shift
		i=`expr $i + 1`
	done
elif [ "$1" = "cp" ]; then
	"$@"
else
	shift
	echo "$@"
	"$@"
fi
