#/bin/bash

# This script can be used to assess the performance of the ordered index.
# It presupposes that you have installed a TPC-H version

if [ "x"$1 = "x" ]
then
	echo "usage: dbname [index [only],default,drop] "
	exit
fi

db=$1

VERSION=`mserver5 --version|grep server|sed -e "s/(.*//" -e "s/\"//g"`

TAG=`date +%y%m%d`
TIMEFORMAT="%R"
RUNLENGTH=5

if [ "x$2" = "x" ]
then
	cmd="default"
else
	cmd=$2
fi

monetdb stop $db 2>&1 >/dev/null

#fixed query load
declare -a arr=(
"trace SELECT count(*) FROM sys.orders;"
"trace SELECT count(*) FROM sys.orders WHERE o_totalprice BETWEEN 38451.0 AND 38452.0;"
"trace SELECT count(*) FROM sys.orders WHERE o_totalprice = 38451.38;"
"trace SELECT count(*) FROM sys.orders WHERE o_totalprice >= 555285.16;"
"trace SELECT count(*) FROM sys.orders WHERE o_totalprice < 858.0;"
"trace SELECT count(*) FROM sys.partsupp as I, sys.supplier as S, sys.part as P WHERE I.ps_partkey = P.p_partkey AND I.ps_suppkey = S.s_suppkey;"
"trace SELECT count(*) FROM sys.partsupp as I, sys.supplier as S, sys.part as P WHERE I.ps_partkey = P.p_partkey AND I.ps_suppkey = S.s_suppkey AND  p_retailprice BETWEEN 1214.0 AND 1215.0;"
"trace SELECT count(*) FROM sys.partsupp as I, sys.supplier as S, sys.part as P WHERE I.ps_partkey = P.p_partkey AND I.ps_suppkey = S.s_suppkey AND  s_acctbal BETWEEN 1432.0 AND 1433.0;"
"trace SELECT count(*) FROM sys.partsupp as I, sys.supplier as S, sys.part as P WHERE I.ps_partkey = P.p_partkey AND I.ps_suppkey = S.s_suppkey AND ps_supplycost BETWEEN 915.0 AND 916;"
"trace SELECT max(o_totalprice) FROM sys.orders;"
"trace SELECT max(o_totalprice) FROM sys.orders WHERE o_totalprice BETWEEN 38451.0 AND 38452.0;"
"trace SELECT max(o_totalprice) FROM sys.orders WHERE o_totalprice = 38451.38;"
"trace SELECT max(o_totalprice) FROM sys.orders WHERE o_totalprice >= 555285.16;"
"trace SELECT max(o_totalprice) FROM sys.orders WHERE o_totalprice < 858.0;"
"trace SELECT max(p_retailprice) FROM sys.partsupp as I, sys.supplier as S, sys.part as P WHERE I.ps_partkey = P.p_partkey AND I.ps_suppkey = S.s_suppkey;"
"trace SELECT max(p_retailprice) FROM sys.partsupp as I, sys.supplier as S, sys.part as P WHERE I.ps_partkey = P.p_partkey AND I.ps_suppkey = S.s_suppkey AND  p_retailprice BETWEEN 1214.0 AND 1215.0;"
"trace SELECT max(p_retailprice) FROM sys.partsupp as I, sys.supplier as S, sys.part as P WHERE I.ps_partkey = P.p_partkey AND I.ps_suppkey = S.s_suppkey AND  s_acctbal BETWEEN 1432.0 AND 1433.0;"
"trace SELECT max(p_retailprice) FROM sys.partsupp as I, sys.supplier as S, sys.part as P WHERE I.ps_partkey = P.p_partkey AND I.ps_suppkey = S.s_suppkey AND ps_supplycost BETWEEN 915.0 AND 916;"
)

#database initialization
declare -a index=(
"CALL createorderindex('sys','orders','o_totalprice');"
"CALL createorderindex('sys','part','p_retailprice');"
"CALL createorderindex('sys','partsupp','ps_supplycost');"
"CALL createorderindex('sys','supplier','s_acctbal');"
)

#database drop indices
declare -a drop=(
"CALL droporderindex('sys','orders','o_totalprice');"
"CALL droporderindex('sys','part','p_retailprice');"
"CALL droporderindex('sys','partsupp','ps_supplycost');"
"CALL droporderindex('sys','supplier','s_acctbal');"
)

files="filenames = \""


# create the index upon need
if [ "x${cmd}" = "xindex" ] 
then
	DATA=`pwd`/${db}-${cmd}-${TAG}
	mkdir -p $DATA
	rm ${DATA}/*
	for (( QID=0; QID<${#index[*]}; QID++ ))
	do
		qry=${index[$QID]}
		echo "query $qry"
		t=`time (echo "${qry}" | mclient -d $db -ei  |tail -1 |sed -e "s/ //g"  -e "s/.*tuple//" -e "s/(//" -e "s/)//" -e "s/\.\(.*\)ms/.\1/"  -e "s/\.\(.*\)s/000.\1/") 2>&1`
		echo "${qry}	$t" >>${DATA}/indices
	done
	if [ "x$3" = xonly ]
	then
		exit
	fi
fi


# drop the index 
if [ "x${cmd}" = "xdrop" ] 
then
	DATA=`pwd`/${db}-${cmd}-${TAG}
	mkdir -p $DATA
	for (( QID=0; QID<${#index[*]}; QID++ ))
	do
		qry=${drop[$QID]}
		echo "query $qry"
		t=`time (echo "${qry}" | mclient -d $db -ei  |tail -1 |sed -e "s/ //g"  -e "s/.*tuple//" -e "s/(//" -e "s/)//" -e "s/\.\(.*\)ms/.\1/"  -e "s/\.\(.*\)s/000.\1/") 2>&1`
		echo "${qry}	$t" >>${DATA}/dropindices
	done
	exit
fi

DATA=`pwd`/${db}-${cmd}-${TAG}
mkdir -p $DATA
rm ${DATA}/*
for (( QID=0; QID<${#arr[*]}; QID++ ))
do
	qry=${arr[$QID]}
	echo "${QID}	${qry}" >>${DATA}/workload
	monetdb stop $db 2>&1 >/dev/null
	#echo "clearing memory";
	#echo 3| sudo /usr/bin/tee /proc/sys/vm/drop_caches
	monetdb start $db 2>&1 >/dev/null

	row="${QID},${cmd},"`date +%Y%m%d:%H%M%S`
	mrow=""
	#mclient -d ${db} -s "EXPLAIN ${qry}" >${DATA}/Q${QID}.explain
	for (( i=0; i< ${RUNLENGTH}; i++ ))
	do
		echo "${qry}" | mclient -d $db -ei   2>&1 >${DATA}/Q${QID}.out
		t=`cat ${DATA}/Q${QID}.out |tail -1 |sed -e "s/ //g"  -e "s/.*tuples//" -e "s/(//" -e "s/)//" -e "s/\.\(.*\)ms/.\1/"  -e "s/\.\(.*\)s/000.\1/"`

		#t=`(mclient -d $db -i"ms" -s "${qry}" |tail -1 | sed -e "s/.*(//" -e "s/ms.*//" ) `
		row=${row}","$t
		echo "$t" >>${DATA}/Q${QID}.data
	done
	echo ${row}",\""${qry}"\""
	echo ${row}",\""${qry}"\"">> ${DATA}/summary
	files="${files}Q${QID}.data "
done

files="${files}\""

# create the gnuplot script file

echo "set title \"${TAG} ${db} ${cmd}\"
set logscale y
set terminal pdfcairo noenhanced font 'verdana,10' color solid size 7,9 
set output 'plot.pdf'
set key outside
$files 
plot \\" >>${DATA}/plot.gpl
for (( QID=0; QID<${#arr[*]}; QID++ ))
do
	echo "'Q${QID}.data' with linespoints,\\"  >>${DATA}/plot.gpl
done
echo "" >>${DATA}/plot.gpl
cd ${DATA}; gnuplot plot.gpl
