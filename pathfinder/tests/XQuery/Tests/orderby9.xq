let $doc := <foo>
                <a><b>42</b><b>foo</b></a>
                <a><b>200</b></a>
                <a/>
                <a><b>1</b></a>
            </foo>
for $a in $doc/a
where count($a/b) lt 2
order by zero-or-one($a/b)
return $a
