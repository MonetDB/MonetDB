
create table test (lhs varchar(16), rhs varchar(16));
COPY 113 RECORDS into test from STDIN using delimiters ',','\n', '"';
"S","TOP"
"PP S@","S"
"IN NP","PP"
"In_","IN"
"NP NP@","NP"
"DT NP@","NP"
"an_","DT"
"NNP NP@","NP@"
"Oct._","NNP"
"CD NN","NP@"
"19_","CD"
"review_","NN"
"PP PRN","NP@"
"IN NP","PP"
"of_","IN"
"`` NP@","NP"
"``_","``"
"NP NP@","NP@"
"DT NN","NP"
"The_","DT"
"Misanthrope_","NN"
"'' PP","NP@"
"''_","''"
"IN NP","PP"
"at_","IN"
"NP NP@","NP"
"NNP POS","NP"
"Chicago_","NNP"
"'s_","POS"
"NNP NNP","NP@"
"Goodman_","NNP"
"Theatre_","NNP"
"-LRB- PRN@","PRN"
"-LRB-_","-LRB-"
"`` PRN@","PRN@"
"``_","``"
"S PRN@","PRN@"
"NP VP","S"
"VBN NNS","NP"
"Revitalized_","VBN"
"Classics_","NNS"
"VBP VP@","VP"
"Take_","VBP"
"NP PP","VP@"
"DT NN","NP"
"the_","DT"
"Stage_","NN"
"IN NP","PP"
"in_","IN"
"NNP NNP","NP"
"Windy_","NNP"
"City_","NNP"
", PRN@","PRN@"
",_",","
"'' PRN@","PRN@"
"''_","''"
"NP -RRB-","PRN@"
"NN NP@","NP"
"Leisure_","NN"
"CC NNS","NP@"
"&_","CC"
"Arts_","NNS"
"-RRB-_","-RRB-"
", S@","S@"
",_",","
"NP S@","S@"
"NP NP@","NP"
"NP PP","NP"
"DT NN","NP"
"the_","DT"
"role_","NN"
"IN NP","PP"
"of_","IN"
"NNP","NP"
"Celimene_","NNP"
", NP@","NP@"
",_",","
"VP ,","NP@"
"VBN PP","VP"
"played_","VBN"
"IN NP","PP"
"by_","IN"
"NNP NNP","NP"
"Kim_","NNP"
"Cattrall_","NNP"
",_",","
"VP .","S@"
"VBD VP","VP"
"was_","VBD"
"ADVP VP@","VP"
"RB","ADVP"
"mistakenly_","RB"
"VBN PP","VP@"
"attributed_","VBN"
"TO NP","PP"
"to_","TO"
"NNP NNP","NP"
"Christina_","NNP"
"Haag_","NNP"
"._","."
"S","TOP"
"NP S@","S"
"NNP NNP","NP"
"Ms._","NNP"
"Haag_","NNP"
"VP .","S@"
"VBZ NP","VP"
"plays_","VBZ"
"NNP","NP"
"Elianti_","NNP"
"._","."
"S","TOP"
"NP S@","S"


select rhs,
(lhs like 'A%' OR lhs like 'B%' OR lhs like 'C%' OR lhs like 'D%' OR lhs
like 'E%' OR lhs like 'F%' OR lhs like 'G%' OR lhs like 'H%' OR lhs like
'I%' OR lhs like 'J%' OR lhs like 'K%' OR lhs like 'L%' OR lhs like 'M%' OR
lhs like 'N%' OR lhs like 'O%' OR lhs like 'P%' OR lhs like 'Q%' OR lhs
like 'R%' OR lhs like 'S%' OR lhs like 'T%' OR lhs like 'U%' OR lhs like
'V%' OR lhs like 'W%' OR lhs like 'X%' OR lhs like 'Y%' OR lhs like 'Z%'),
(lower(lhs) like 'a%'),
(lower(lhs) like 'in%'),
(lower(lhs) like 'un%'),
(lower(lhs) like 'non%'),
((lower(lhs) like '%ise_') OR (lower(lhs) like '%ize_')),
(lower(lhs) like '%ly_'),
(lower(lhs) like '%s_'),
(lower(lhs) like '%ed_'),
(lower(lhs) like '%ing_'),
(lower(lhs) like '%-%_'),
(lower(lhs) like '%-_'),
(lower(lhs) like '%.%_'),
(lower(lhs) like '%._')
from test where lhs like '%!_' escape '!';

select rhs,
(lhs like 'A%' OR lhs like 'B%' OR lhs like 'C%' OR lhs like 'D%' OR
lhs like 'E%' OR lhs like 'F%' OR lhs like 'G%' OR lhs like 'H%' OR lhs
like 'I%' OR lhs like 'J%' OR lhs like 'K%' OR lhs like 'L%' OR lhs like
'M%' OR lhs like 'N%' OR lhs like 'O%' OR lhs like 'P%' OR lhs like 'Q%' OR
lhs like 'R%' OR lhs like 'S%' OR lhs like 'T%' OR lhs like 'U%' OR lhs
like 'V%' OR lhs like 'W%' OR lhs like 'X%' OR lhs like 'Y%' OR lhs like
'Z%'),
(lower(lhs) like 'a%'),
(lower(lhs) like 'in%'),
(lower(lhs) like 'un%'),
(lower(lhs) like 'non%'),
((lower(lhs) like '%ise!_') OR (lower(lhs) like '%ize!_')),
(lower(lhs) like '%ly!_'),
(lower(lhs) like '%s!_'),
(lower(lhs) like '%ed!_'),
(lower(lhs) like '%ing!_'),
(lower(lhs) like '%-%!_'),
(lower(lhs) like '%-!_'),
(lower(lhs) like '%.%!_'),
(lower(lhs) like '%.!_')
from test where lhs like '%!_' escape '!';
