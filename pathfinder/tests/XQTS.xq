(
<h>
#!/bin/sh

if [ ! "$3" ] ; then
	echo 'Usage: '"$0"' {text{"<TSTSRCBASE> <XQTS_DIR> <XQTS_SRC>"}}'
	exit 1
fi
	
TSTSRCBASE="$1"
XQTS_DIR="$2"
XQTS_SRC="$3"
XQTS_DST="$TSTSRCBASE/$XQTS_DIR"
DOCS_DIR="$XQTS_DIR/doc"
DOCS_DST="$XQTS_DST/doc"
MODS_DIR="$XQTS_DIR/mod"
MODS_DST="$XQTS_DST/mod"

mkdir -p "$DOCS_DST"
cp "$XQTS_SRC/TestSources"/*.xml "$DOCS_DST"
mkdir -p "$MODS_DST"
for i in "$XQTS_SRC/TestSources"/*-lib.xq ; do
	cat "$i" \
	 | perl -pe 's|(module namespace .*[^ ]) *= *([^ ].*;)|$1 = $2|' \
	 | perl -pe 's!((xs|xdt):[a-zA-Z0-9]*)\(([^\(\)]*(\([^\(\)]*(\([^\(\)]*\)[^\(\)]*)*\)[^\(\)]*)*)\)!($3 cast as $1)!g' \
	 | perl -pe 's!((xs|xdt):[a-zA-Z0-9]*)\(([^\(\)]*(\([^\(\)]*(\([^\(\)]*\)[^\(\)]*)*\)[^\(\)]*)*)\)!($3 cast as $1)!g' \
	 | perl -pe 's!((xs|xdt):[a-zA-Z0-9]*)\(([^\(\)]*(\([^\(\)]*(\([^\(\)]*\)[^\(\)]*)*\)[^\(\)]*)*)\)!($3 cast as $1)!g' \
	 > "{text{"$MODS_DST/${i##*/}"}}"
done
ln -s "moduleDefs-lib.xq" "$MODS_DST/module-defs.xq"
ln -s "modulesdiffns-lib.xq" "$MODS_DST/modulesdiffns.xq"

(
cd "$XQTS_SRC/Queries/XQuery"
perl -i -p -e 's|^declare variable \$input-context external;|declare variable \$input-context1 external;|' \
	OptionalFeatureErrors/CombinedErrorCodes/combined-errors-1.xq
perl -i -p -e 's|^declare variable \$input-context1 external;|declare variable \$input-context external;|' \
	Functions/QNameFunc/QNameConstructFunc/ExpandedQNameConstructFunc/qName-1.xq \
	Functions/QNameFunc/ResolveQNameConstructFunc/fn-resolve-qname-[12].xq \
	TrivialEmbedding/trivial-[1234].xq
)
</h>
,
for $tst in doc("XQTSCatalog.xml")//*:test-case
(:[for $et in *:expected-error/text() where (starts-with($et,"XQST0046")) return $et]:)
(:[not(*:expected-error)]:)
for $yes in doc("/net/corona.ins.cwi.nl/export/scratch0/manegold/Monet/Testing/Stable/pathfinder/tests/XQTS.supported.xml")//yes 
where (<x>{fn:data($tst/@FilePath)}{fn:data($tst/@name)}.xq</x> = <x>{fn:data($yes/@FilePathName)}</x>) return
if ($tst/*:output-file or $tst/*:expected-error) then
<t>

TSTDIR="{fn:data($tst/@FilePath)}"
TSTDIR="${text{"{"}}TSTDIR%/{text{"}"}}"
TSTNME="{fn:data($tst/@name)}"
TSTFLE="{fn:data($tst/*:query/@name)}.xq"
mkdir -p "$XQTS_DST/$TSTDIR/Tests"
echo "$TSTNME" >> "$XQTS_DST/$TSTDIR/Tests/All"
cat "$XQTS_SRC/Queries/XQuery/$TSTDIR/$TSTFLE" \
{
for $i in $tst/*:input-file return
<x>
  | perl -pe 's|^declare variable (\${fn:data($i/@variable)}) external;|let $1 := doc("\$TSTSRCBASE/'"$DOCS_DIR"'/{$i/text()}.xml") return|' \
</x>
}
{
if ($tst/*:input-query) then
<q>
  | perl -pe 's|^(declare variable \${fn:data($tst/*:input-query/@variable)} .*)external;|$1:= (\n'"` \
	cat "$XQTS_SRC/Queries/XQuery/$TSTDIR/{fn:data($tst/*:input-query/@name)}.xq" \
	 | sed -e 's|\\$|\\\\$|g' \
	`"'\n);|' \
</q>
else
<q/>
}
  | perl -pe 's|^(import module namespace .*[^ ]) *= *([^ ].*;)|$1 = $2|' \
  | perl -pe 's|^(import module namespace .* = [^ ]+)( +at .*)? *;|$1 at;|' \
{
if ($tst/*:module) then
for $m in $tst/*:module return
<m>
  | perl -pe 's|^(import module namespace .* at.*);|$1 "\$TSTSRCBASE/'"$MODS_DIR"'/{$m/text()}.xq",;|' \
</m>
else
<m/>
}
  | perl -pe 's|^(import module namespace .* at.*),;|$1;|' \
  | perl -pe 's!((xs|xdt):[a-zA-Z0-9]*)\(([^\(\)]*(\([^\(\)]*(\([^\(\)]*\)[^\(\)]*)*\)[^\(\)]*)*)\)!($3 cast as $1)!g' \
  | perl -pe 's!((xs|xdt):[a-zA-Z0-9]*)\(([^\(\)]*(\([^\(\)]*(\([^\(\)]*\)[^\(\)]*)*\)[^\(\)]*)*)\)!($3 cast as $1)!g' \
  | perl -pe 's!((xs|xdt):[a-zA-Z0-9]*)\(([^\(\)]*(\([^\(\)]*(\([^\(\)]*\)[^\(\)]*)*\)[^\(\)]*)*)\)!($3 cast as $1)!g' \
  > "$XQTS_DST/$TSTDIR/Tests/$TSTNME.xq.in"
(
echo 'stdout of test '\'"$TSTNME"'` in directory '\'"$XQTS_DIR/$TSTDIR"'` itself:'
echo ''
echo 'Ready.'
echo ''
echo 'Over..'
echo ''
{
if ($tst/*:output-file) then
for $o at $p in $tst/*:output-file return
if ($p = 1) then
<o>
echo -e '{text{"<?xml"}} version="1.0" encoding="utf-8"{text{"?>"}}\n<XQueryResult>'
cat "$XQTS_SRC/ExpectedTestResults/$TSTDIR/{$o/text()}"
echo -e '\n</XQueryResult>'
</o>
else
<o/>
else
<o/>
}
) > "$XQTS_DST/$TSTDIR/Tests/$TSTNME.stable.out"
(
echo 'stderr of test '\'"$TSTNME"'` in directory '\'"$XQTS_DIR/$TSTDIR"'` itself:'
echo ''
{
if ($tst/*:expected-error) then
for $e in $tst/*:expected-error
let $mxq := doc("XQTS.Errors.xml")/MXQerrors/MXQerror[@id = $e/text()] return
if ($mxq) then
<e>
echo '# expecting {fn:data($tst/@scenario)} "{$e/text()}" (cf., http://www.w3.org/TR/xquery/#ERR{$e/text()} ) !'
{
for $msg in $mxq return
<E>
echo '{$msg/text()}'
</E>
}
</e>
else
<e>
echo '! expecting {fn:data($tst/@scenario)} "{$e/text()}" (cf., http://www.w3.org/TR/xquery/#ERR{$e/text()} ) !'
</e>
else
<e/>
}
) > "$XQTS_DST/$TSTDIR/Tests/$TSTNME.stable.err"
</t>
else
<t>
echo 'not(*:output-file) and not(*:expected-error): {fn:data($tst/@FilePath)}{fn:data($tst/@name)}"
</t>
)
