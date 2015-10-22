if [ $# -eq 1 ]; then
	NN=`echo $1 | sed 's/.in$//'`
	echo $1
	# sedscript.in created using 
	# grep -o "@[[:alnum:]_]*@" Makefile.in | sort -f | uniq | sed -e "s/^/s|/" -e "s/$/||/" > sedscript.tpl
	sed -f src/tools/embedded/windows/sedscript $1 > $NN
else
	touch src/config.status
	find src -name "*.in" -type f -exec sh $0 {} \;
fi
