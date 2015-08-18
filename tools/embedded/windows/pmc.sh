if [ $# -eq 1 ]; then
	NN=`echo $1 | sed 's/.in$//'`
	echo $1
	# sedscript created using 
	# grep -o "@[[:alnum:]_]*@" Makefile.in | sort -f | uniq | sed -e "s/^/s|/" -e "s/$/||/" > sedscript
	sed -f tools/embedded/windows/sedscript $1 > $NN
else
	find . -name "*.in" -type f -exec sh $0 {} \;
fi
touch config.status
cp tools/embedded/windows/sql_parser.tab.* sql/server/
cp tools/embedded/windows/monetdb_config.h.in .

# TODO: add source paths to sedscript
