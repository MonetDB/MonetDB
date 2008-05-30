let $d := doc('dblp.xml')
for $a1 in $d//article,
$a2 in $d//article,
$a3 in $d//article,
$a4 in $d//article
where
$a1/author/text() = $a2/author/text() and
$a2/author/text() = $a3/author/text() and
$a3/author/text() = $a4/author/text() and
$a1/journal/text() = 'ACM Trans. Database Syst.' and
$a2/journal/text() = 'Bioinformatics' and
$a3/journal/text() = 'IEEE Trans. Knowl. Data Eng.' and
$a4/journal/text() = 'Nucleic Acids Research'
return $a3
