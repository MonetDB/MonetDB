{--	Xmark syntax changes from: http://monetdb.cwi.nl/xml/Assets/queries.txt
	In general, case sensitive changes for all keywords (FOR to for...e.t.c
	Other changes commented in the following queries 
 --}

{-- Q1 --}
for    $b in document("auction.xml")/site/people/person[@id="person0"]
return $b/name/text()
--

{-- Q2 expressions inside tags must be enclosed with {} --}
for $b in document("auction.xml")/site/open_auctions/open_auction
return <increase>{$b/bidder[1]/increase/text()}</increase>
--

{-- Q3 attribute assignments always in "" or "{expression}" --}
for    $b in document("auction.xml")/site/open_auctions/open_auction
where  $b/bidder[1]/increase/text() * 2 <= $b/bidder[last()]/increase/text()
return <increase first="{$b/bidder[1]/increase/text()}"
                 last="{$b/bidder[last()]/increase/text()}"/>
--

{-- Q4 "BEFORE" changes to "<<" --}
for    $b in document("auction.xml")/site/open_auctions/open_auction
where  $b/bidder/personref[id="person18829"] <<
       $b/bidder/personref[id="person10487"]
return <history>{$b/reserve/text()}</history>

--

{-- Q5 --}
count  (for    $i in document("auction.xml")/site/closed_auctions/closed_auction
        where  $i/price/text() >= 40
        return $i/price)
--

{-- Q6 --}
 for  $b in document("auction.xml")/site/regions
 return count($b//item)
--

{-- Q7 --}
for $p in document("auction.xml")/site
let $c1 := count($p//description),
    $c2 := count($p//mail),
    $c3 := count($p//email),
    $sum := $c1 + $c2 + $c3
return $sum
--

{-- Q8 --}
for    $p in document("auction.xml")/site/people/person
let    $a := for $t in document("auction.xml")/site/closed_auctions/closed_auction
             where $t/buyer/@person = $p/@id
             return $t
return <item person="{$p/name/text()}"> {count ($a)} </item>
--

{-- Q9--}
for    $p in document("auction.xml")/site/people/person
let    $a := for $t in document("auction.xml")/site/closed_auctions/closed_auction
             let $n := for $t2 in document("auction.xml")/site/regions/europe/item
                       where  $t/itemref/@item = $t2/@id
                       return $t2
             where $p/@id = $t/buyer/@person
             return <item> {$n/name/text()} </item>
return <person name="{$p/name/text()}"> {$a} </person>
--

{-- Q10  "DISTINCT" changes to distinct-values(expression) --}
for $i in distinct-values(
          document("auction.xml")/site/people/person/profile/interest/@category)
let $p := for    $t in document("auction.xml")/site/people/person
          where  $t/profile/interest/@category = $i
            return <personne>
                <statistiques>
                        <sexe> {$t/gender/text()} </sexe>,
                        <age> {$t/age/text()} </age>,
                        <education> {$t/education/text()}</education>,
                        <revenu> {$t/income/text()} </revenu>
                </statistiques>,
                <coordonnees>
                        <nom> $t/name/text() </nom>,
                        <rue> $t/street/text() </rue>,
                        <ville> $t/city/text() </ville>,
                        <pays> $t/country/text() </pays>,
                        <reseau>
                                <courrier> $t/email/text() </courrier>,
                                <pagePerso> $t/homepage/text()</pagePerso>
                        </reseau>,
                </coordonnees>
                <cartePaiement> $t/creditcard/text()</cartePaiement>
              </personne>
return <categorie>
        <id> $i </id>,
        $p
      </categorie>
--

{-- Q11 --}
for $p in document("auction.xml")/site/people/person
let $l := for $i in document("auction.xml")/site/open_auctions/open_auction/initial
          where $p/profile/@income > (5000 * $i/text())
          return $i
return <items>
         <name> {$p/name/text()} </name>
         <number> {count ($l)} </number>
       </items>
--

{-- Q12 --}
for $p in document("auction.xml")/site/people/person
let $l := for $i in document("auction.xml")/site/open_auctions/open_auction/initial
          where $p/profile/@income > (5000 * $i/text())
          return $i
where $p/profile/@income > 50000
return <items>
         <name> {$p/name/text()} </name>
         <number> {count ($l)} </number>
       </items>

--

{-- Q13 --}
for $i in document("auction.xml")/site/regions/australia/item
return <item> <name> {$i/name/text()} </name> {$i/description} </item>
--

{-- Q14 --}
for $i in document("auction.xml")/site//item
where contains ($i/description,"gold")
return $i/name/text()
--

{-- Q15 --}
for $a in document("auction.xml")/site/closed_auctions/closed_auction/annotation/
		description/parlist/listitem/parlist/listitem/text/emph/keyword/text()
return <text> {$a} </text>
--

{-- Q16 --}
for $a in document("auction.xml")/site/closed_auctions/closed_auction
where  {-- NOT left out --} empty($a/annotation/description/parlist/listitem/parlist/
		listitem/text/emph/keyword/text())
return <person id="{$a/seller/@person}" />
--

{-- Q17 --}
for  $p in document("auction.xml")/site/people/person
where  empty($p/homepage/text())
return <person> {$p/name/text()} </person>
--

{-- Q18 --}
define function convert($v)
{
   {-- "return" only allowed in flwr expressions --}
   let $c := 2.20371 * $v
   return $c
}
for    $i in document("auction.xml")/site/open_auctions/open_auction
return ($i/reserve/text())
--

{-- Q19 -- "SORTBY" changes to "order by" --}
for    $b in document("auction.xml")/site/regions//auction
       order by $b/items/item/item_number/@id
return <item>
         <name> {$b//name/text()} </name>
         <location> {$b/location/text()} </location>
       </item>
--

{-- Q20 --}
<result>
 <preferred>
   {count (document("auction.xml")/site/people/person/profile[@income >= 100000])}
 </preferred>,
 <standard>
  {count (document("auction.xml")/site/people/person/profile[@income < 100000
                                                        and @income >= 30000])}
 </standard>,
 <challenge>
  {count (document("auction.xml")/site/people/person/profile[@income < 30000])}
 </challenge>,
 <na>
  {count (for    $p in document("auction.xml")/site/people/person
         where  empty($p/@income)
         return $p)}
 </na>
</result>
