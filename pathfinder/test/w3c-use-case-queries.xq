<bib>
 {
  for $b in document("http://www.bn.com/bib.xml")/bib/book
  where $b/publisher = "Addison-Wesley" and $b/@year > 1991
  return
    <book year="{ $b/@year }">
     { $b/title }
    </book>
 }
</bib> 
--

<results>
  {
    for $b in document("http://www.bn.com/bib.xml")/bib/book,
        $t in $b/title,
        $a in $b/author
    return
        <result>
            { $t }    
            { $a }
        </result>
  }
</results>

--

<results>
{
    for $b in document("http://www.bn.com/bib.xml")/bib/book
    return
        <result>
            { $b/title }
            { $b/author  }
        </result>
}
</results> 
--

<results>
  {
    for $a in distinct-values(document("http://www.bn.com/bib.xml")//author)
    return
        <result>
            { $a }
            {
                for $b in document("http://www.bn.com/bib.xml")/bib/book
                where some $ba in $b/author satisfies deep-equal($ba,$a)
                return $b/title
            }
        </result>
  }
</results> 
--

<books-with-prices>
  {
    for $b in document("http://www.bn.com/bib.xml")//book,
        $a in document("http://www.amazon.com/reviews.xml")//entry
    where $b/title = $a/title
    return
        <book-with-prices>
            { $b/title }
            <price-amazon>{ $a/price/text() }</price-amazon>
            <price-bn>{ $b/price/text() }</price-bn>
        </book-with-prices>
  }
</books-with-prices>
--

<bib>
  {
    for $b in document("http://www.bn.com/bib.xml")//book
    where count($b/author) > 0
    return
        <book>
            { $b/title }
            {
                for $a in $b/author[position()<=2]  
                return $a
            }
            {
                if (count($b/author) > 2)
                 then <et-al/>
                 else ()
            }
        </book>
  }
</bib>
--

<bib>
  {
    for $b in document("http://www.bn.com/bib.xml")//book
    where $b/publisher = "Addison-Wesley" and $b/@year > 1991
    order by $b/title
    return
        <book>
            { $b/@year }
            { $b/title }
        </book>
  }
</bib> 
--

for $b in document("http://www.bn.com/bib.xml")//book
let $e := $b/*[contains(string(.), "Suciu") 
               and ends-with(local-name(.), "or")]
where exists($e)
return
    <book>
        { $b/title }
        { $e }
    </book> 
--

<results>
  {
    for $t in document("books.xml")//(chapter | section)/title
    where contains($t/text(), "XML")
    return $t
  }
</results> 
--

<results>
  {
    let $doc := document("prices.xml")
    for $t in distinct-values($doc//book/title)
    let $p := for $x in $doc//book[title = $t]/price
              return decimal($x)
    return
      <minprice title="{ $t/text() }">
        <price>{ min($p) }</price>
      </minprice>
  }
</results> 
--

<bib>
{
        for $b in document("http://www.bn.com/bib.xml")//book[author]
        return
            <book>
                { $b/title }
                { $b/author }
            </book>
}
{
        for $b in document("http://www.bn.com/bib.xml")//book[editor]
        return
          <reference>
            { $b/title }
            {$b/editor/affiliation}
          </reference>
}
</bib>  
--

<bib>
{
    for $book1 in document("http://www.bn.com/bib.xml")//book,
        $book2 in document("http://www.bn.com/bib.xml")//book
    let $aut1 := for $a in $book1/author 
                 order by $a/last, $a/first
                 return $a
    let $aut2 := for $a in $book2/author 
                 order by $a/last, $a/first
                 return $a
    where $book1 << $book2
    and not($book1/title = $book2/title)
    and sequence-deep-equal($aut1, $aut2) 
    return
        <book-pair>
            { $book1/title }
            { $book2/title }
        </book-pair>
}
</bib> 
--

define function toc($e as element )
  as element*
{
  let $n := local-name( $e )
  return
    if ($n = "section")
    then <section>
             { $e/@* }
             { toc($e/*) }
           </section>
    else if ($n = "title")
    then $e
    else ()
}

<toc>
  {
    toc( document("book.xml")/book )
  }
</toc> 
--

<figlist>
  {
    for $f in document("book.xml")//figure
    return
        <figure>
            { $f/@* }
            { $f/title }
        </figure>
  }
</figlist> 
--

<section_count>{ count(document("book.xml")//section) }</section_count>, 
<figure_count>{ count(document("book.xml")//figure) }</figure_count> 
--

<top_section_count>
 { 
   count(document("book.xml")/book/section) 
 }
</top_section_count>
--

<section_list>
  {
    for $s in document("book.xml")//section
    let $f := $s/figure
    return
        <section title="{ $s/title/text() }" figcount="{ count($f) }"/>
  }
</section_list> 
--

define function section_summary($s as element) as element
{
    <section>
        { $s/@* }
        { $s/title }
        <figcount>{ count($s/figure) }</figcount>
        {
            for $ss in $s/section
            return section_summary($ss)
        }
    </section>
}

<toc>
  {
    for $s in document("book.xml")/book/section
    return section_summary($s)
  }
</toc> 
--

for $s in document("report1.xml")//section[section.title = "Procedure"]
return ($s//incision)[2]/instrument
--

for $s in document("report1.xml")//section[section.title = "Procedure"]
return ($s//instrument)[position()<=2]
--

let $i2 := (document("report1.xml")//incision)[2]
for $a in (document("report1.xml")//action)[. >> $i2][position()<=2]
return $a//instrument 
--

for $p in document("report1.xml")//section[section.title = "Procedure"]
where not(some $a in $p//anesthesia satisfies
        $a << ($p//incision)[1] )
return $p 
--

define function precedes($a as node, $b as node) as boolean 
{
    $a << $b
      and
    empty($a//node() intersect $b)
}


define function follows($a as node, $b as node) as boolean 
{
    $a >> $b
      and
    empty($b//node() intersect $a)
}

<critical_sequence>
 {
  let $proc := document("report1.xml")//section[section.title="Procedure"][1]
  for $n in $proc//node()
  where follows($n, ($proc//incision)[1])
    and precedes($n, ($proc//incision)[2])
  return $n
 }
</critical_sequence> 
--

<critical_sequence>
 {
  let $proc := document("report1.xml")//section[section.title="Procedure"][1],
      $i1 :=  ($proc//incision)[1],
      $i2 :=  ($proc//incision)[2]
  for $n in $proc//node() except $i1//node()
  where $n >> $i1 and $n << $i2
  return $n 
 }
</critical_sequence> 
--

<result>
  {
    for $i in document("items.xml")//item_tuple
    where $i/start_date <= current-date()
      and $i/end_date >= current-date() 
      and contains($i/description, "Bicycle")
    order by $i/itemno
    return
        <item_tuple>
            { $i/itemno }
            { $i/description }
        </item_tuple>
  }
</result>
--

<result>
  {
    for $i in document("items.xml")//item_tuple
    let $b := document("bids.xml")//bid_tuple[itemno = $i/itemno]
    where contains($i/description, "Bicycle")
    order by $i/itemno
    return
        <item_tuple>
            { $i/itemno }
            { $i/description }
            <high_bid>{ max(for $z in $b/bid return decimal($z)) }</high_bid>
        </item_tuple>
  }
</result> 
--

<result>
  {
    for $u in document("users.xml")//user_tuple
    for $i in document("items.xml")//item_tuple
    where $u/rating > "C" 
       and $i/reserve_price > 1000 
       and $i/offered_by = $u/userid
    return
        <warning>
            { $u/name }
            { $u/rating }
            { $i/description }
            { $i/reserve_price }
        </warning>
  }
</result>
--

<result>
  {
    for $i in document("items.xml")//item_tuple
    where empty(document("bids.xml")//bid_tuple[itemno = $i/itemno])
    return
        <no_bid_item>
            { $i/itemno }
            { $i/description }
        </no_bid_item>
  }
</result> 
--

<result>
  {
    for $seller in document("users.xml")//user_tuple,
        $buyer in  document("users.xml")//user_tuple,
        $item in  document("items.xml")//item_tuple,
        $highbid in  document("bids.xml")//bid_tuple
    where $seller/name = "Tom Jones"
      and $seller/userid  = $item/offered_by
      and contains($item/description , "Bicycle")
      and $item/itemno  = $highbid/itemno
      and $highbid/userid  = $buyer/userid
      and $highbid/bid = max(
                             for $x in document("bids.xml")//bid_tuple
                                  [itemno = $item/itemno]/bid 
                             return decimal($x)
                         )
    order by ($item/itemno)
    return
        <jones_bike>
            { $item/itemno }
            { $item/description }
            <high_bid>{ $highbid/bid }</high_bid>
            <high_bidder>{ $buyer/name }</high_bidder>
        </jones_bike>
  }
</result> 
--

<result>
  {
    for $seller in unordered(document("users.xml")//user_tuple),
        $buyer in  unordered(document("users.xml")//user_tuple),
        $item in  unordered(document("items.xml")//item_tuple),
        $highbid in  document("bids.xml")//bid_tuple
    where $seller/name = "Tom Jones"
      and $seller/userid  = $item/offered_by
      and contains($item/description , "Bicycle")
      and $item/itemno  = $highbid/itemno
      and $highbid/userid  = $buyer/userid
      and $highbid/bid = max(for $x in unordered(document("bids.xml")//bid_tuple)
                                [itemno = $item/itemno]/bid 
                             return decimal(data($x)))
    order by $item/itemno
    return
        <jones_bike>
            { $item/itemno }
            { $item/description }
            <high_bid>{ $highbid/bid }</high_bid>
            <high_bidder>{ $buyer/name }</high_bidder>
        </jones_bike>
  }
</result> 
--

<result>
  {
    for $item in document("items.xml")//item_tuple
    let $b := document("bids.xml")//bid_tuple[itemno = $item/itemno]
    let $z := max(for $x in $b/bid return decimal($x))
    where $item/reserve_price * 2 < $z
    return
        <successful_item>
            { $item/itemno }
            { $item/description }
            { $item/reserve_price }
            <high_bid>{$z }</high_bid>
         </successful_item>
  }
</result> 
--

let $allbikes := document("items.xml")//item_tuple
                    [contains(description, "Bicycle") 
                     or contains(description, "Tricycle")]
let $bikebids := document("bids.xml")//bid_tuple[itemno = $allbikes/itemno]
return
    <high_bid>
      { 
        max(for $x in $bikebids/bid return decimal($x)) 
      }
    </high_bid> 
--

let $item := document("items.xml")//item_tuple
  [end_date >= date("1999-03-01") and end_date <= date("1999-03-31")]
return
    <item_count>
      { 
        count($item) 
      }
    </item_count>
--

<result>
  {
    let $end_dates := document("items.xml")//item_tuple/end_date
    for $m in distinct-values(for $e in $end_dates 
                              return get-month-from-date($e))
    let $item := document("items.xml")
        //item_tuple[get-year-from-date(end_date) = 1999 
                     and get-month-from-date(end_date) = $m]
    order by $m
    return
        <monthly_result>
            <month>{ $m }</month>
            <item_count>{ count($item) }</item_count>
        </monthly_result>
  }
</result>
--

<result>
 {
    for $highbid in document("bids.xml")//bid_tuple,
        $user in document("users.xml")//user_tuple
    where $user/userid = $highbid/userid 
      and $highbid/bid = max(for $x in document("bids.xml")//bid_tuple
                              [itemno=$highbid/itemno]/bid return decimal($x))
    order by $highbid/itemno
    return
        <high_bid>
            { $highbid/itemno }
            { $highbid/bid }
            <bidder>{ $user/name/text() }</bidder>
        </high_bid>
  }
</result> 
--

let $highbid := max(for $x in document("bids.xml")//bid_tuple/bid 
                    return decimal($x))
return
    <result>
     {
        for $item in document("items.xml")//item_tuple,
            $b in document("bids.xml")//bid_tuple[itemno = $item/itemno]
        where $b/bid = $highbid
        return
            <expensive_item>
                { $item/itemno }
                { $item/description }
                <high_bid>{ $highbid }</high_bid>
            </expensive_item>
     }
    </result> 
--

define function bid_summary ()
{
    for $i in distinct-values(document("bids.xml")//itemno)
    let $b := document("bids.xml")//bid_tuple[itemno = $i]
    return
        <bid_count>
            { $i }
            <nbids>{ count($b) }</nbids>
        </bid_count>
}

<result>
 {
    let $bid_counts := bid_summary(),
        $maxbids := max(for $x in $bid_counts/nbids return decimal($x)),
        $maxitemnos := $bid_counts[nbids = $maxbids]
    for $item in document("items.xml")//item_tuple,
        $bc in $bid_counts
    where $bc/nbids =  $maxbids and $item/itemno = $bc/itemno
    return
        <popular_item>
            { $item/itemno }
            { $item/description }
            <bid_count>{ $bc/nbids/text() }</bid_count>
        </popular_item>
 }
</result> 
--

<result>
 {
    for $uid in distinct-values(document("bids.xml")//userid),
        $u in document("users.xml")//user_tuple[userid = $uid]
    let $b := document("bids.xml")//bid_tuple[userid = $uid]
    order by $u/userid
    return
        <bidder>
            { $u/userid }
            { $u/name }
            <bidcount>{ count($b) }</bidcount>
            <avgbid>{ avg(for $x in $b/bid return decimal($x)) }</avgbid>
        </bidder>
  }
</result> 
--

<result>
 {
    for $i in distinct-values(document("bids.xml")//itemno)
    let $b := document("bids.xml")//bid_tuple[itemno = $i]
    let $avgbid := decimal(avg(for $x in $b/bid return decimal($x)))
    where count($b) >= 3
    order by $avgbid descending
    return
        <popular_item>
            { $i }
            <avgbid>{ $avgbid }</avgbid>
        </popular_item>
  }
</result> 
--

<result>
  {
    for $u in document("users.xml")//user_tuple
    let $b := document("bids.xml")//bid_tuple[userid=$u/userid and bid>=100]
    where count($b) > 1
    return
        <big_spender>{ $u/name/text() }</big_spender>
  }
</result>

--

<result>
  {
    for $u in document("users.xml")//user_tuple
    let $b := document("bids.xml")//bid_tuple[userid = $u/userid]
    order by $u/userid
    return
        <user>
            { $u/userid }
            { $u/name }
            {
                if (empty($b))
                  then <status>inactive</status>
                  else <status>active</status>
            }
        </user>
  }
</result>

--

<frequent_bidder>
  {
    for $u in document("users.xml")//user_tuple
    where 
      every $item in document("items.xml")//item_tuple satisfies 
        some $b in document("bids.xml")//bid_tuple satisfies 
          ($item/itemno = $b/itemno and $u/userid = $b/userid)
    return
        $u/name
  }
</frequent_bidder>
--

<result>
  {
    for $u in document("users.xml")//user_tuple
    order by $u/name
    return
        <user>
            { $u/name }
            {
                for $b in distinct-values(document("bids.xml")//bid_tuple
                                             [userid = $u/userid]/itemno)
                for $i in document("items.xml")//item_tuple[itemno = $b]
                let $descr := $i/description/text()
                order by $descr
                return
                    <bid_on_item>{ $descr }</bid_on_item>
            }
        </user>
  }
</result>

--

<result>
  { 
    input()//report//para 
  }
</result>
--

<result>
  { 
    input()//intro/para 
  }
</result>
--

<result>
  {
    for $c in input()//chapter
    where empty($c/intro)
    return $c/section/intro/para
  }
</result> 
--

<result>
  {
    (((input()//chapter)[2]//section)[3]//para)[2]
  }
</result> 
--

<result>
  {
    input()//para[@security = "c"]
  }
</result> 
--

<result>
  {
    for $s in input()//section/@shorttitle
    return <stitle>{ $s }</stitle>
  }
</result> 
--

<result>
  {
    for $i in input()//intro/para[1]
    return
        <first_letter>{ substring(string($i), 1, 1) }</first_letter>
  }
</result> 
--

<result>
  {
    input()//section[contains(string(.//title), "is SGML")]
  }
</result> 
--

<result>
  {
    input()//section[contains(.//title/text(), "is SGML")]
  }
</result> 
--

<result>
  {
    for $id in input()//xref/@xrefid
    return input()//topic[@topicid = $id]
  }
</result> 
--

<result>
  {
    let $x := input()//xref[@xrefid = "top4"],
        $t := input()//title[. << $x]
    return $t[last()]
  }
</result> 
--

input()//news_item/title[contains(./text(), "Foobar Corporation")] 
--

define function partners($company as xs:string) as element*
{
    let $c := document("company-data.xml")//company[name = $company]
    return $c//partner
}

let $foobar_partners := partners("Foobar Corporation")

for $item in input()//news_item
where
  some $t in $item//title satisfies
    (contains($t/text(), "Foobar Corporation")
    and some $partner in $foobar_partners satisfies
      contains($t/text(), $partner/text()))
  or some $par in $item//par satisfies
   (contains(string($par), "Foobar Corporation")
     and some $partner in $foobar_partners satisfies
        contains(string($par), $partner/text()))
return
    <news_item>
        { $item/title }
        { $item/date }
    </news_item> 
--

define function partners($company as xs:string) as element*
{
    let $c := document("company-data.xml")//company[name = $company]
    return $c//partner
}

for $item in input()//news_item,
    $c in document("company-data.xml")//company
let $partners := partners($c/name)
where contains(string($item), $c/name)
  and some $p in $partners satisfies
    contains(string($item), $p) and $item/news_agent != $c/name
return
    $item 
--

for $item in input()//news_item
where contains(string($item/content), "Gorilla Corporation")
return
    <item_summary>
        { $item/title/text() }.
        { $item/date/text() }.
        { string(($item//par)[1]) }
    </item_summary> 
--

<Q1>
  {
    for $n in distinct-values(
                  for $i in (input()//* | input()//@*)
                  return namespace-uri($i) 
               )
    return  <ns>{$n}</ns>
  }
</Q1> 
--

declare namespace music = "http://www.example.org/music/records"

<Q2>
  {
    input()//music:title
  }
</Q2> 
--

declare namespace dt = "http://www.w3.org/2001/XMLSchema"

<Q3>
  {
    input()//*[@dt:*]
  }
</Q3> 
--

declare namespace xlink = "http://www.w3.org/1999/xlink"

<Q4>
  {
    for $hr in input()//@xlink:href
    return <ns>{ $hr }</ns>
  }
</Q4>
--

declare namespace music = "http://www.example.org/music/records"

<Q5>
  {
     input()//music:record[music:remark/@xml:lang = "de"]
  }
</Q5> 
--

declare namespace ma = "http://www.example.com/AuctionWatch"
declare namespace anyzone = "http://www.example.com/auctioneers#anyzone"

<Q6>
  {
    input()//ma:Auction[@anyzone:ID]/ma:Schedule/ma:Close
  }
</Q6>
--

declare namespace ma = "http://www.example.com/AuctionWatch"

<Q7>
  {
    for $a in document("data/ns-data.xml")//ma:Auction
    let $seller_id := $a/ma:Trading_Partners/ma:Seller/*:ID,
        $buyer_id := $a/ma:Trading_Partners/ma:High_Bidder/*:ID
    where namespace-uri($seller_id) = namespace-uri($buyer_id)
    return
        $a/ma:AuctionHomepage
  }
</Q7> 
--

declare namespace ma = "http://www.example.com/AuctionWatch"

<Q8>
  {
    for $s in input()//ma:Trading_Partners/(ma:Seller | ma:High_Bidder)
    where $s/*:NegativeComments = 0
    return $s
  }
</Q8>
--

define function one_level ($p as element) as element
{
    <part partid="{ $p/@partid }"
          name="{ $p/@name }" >
        {
            for $s in document("partlist.xml")//part
            where $s/@partof = $p/@partid
            return one_level($s)
        }
    </part>
}

<parttree>
  {
    for $p in document("partlist.xml")//part[empty(@partof)]
    return one_level($p)
  }
</parttree>
--

import schema "ipo.xsd"
declare namespace ipo="http://www.example.com/IPO"
                        
count( 
  document("ipo.xml")//ipo:shipTo[. instance of element of type ipo:UKAddress]
)

--

import schema "ipo.xsd"
import schema "zips.xsd"
declare namespace ipo="http://www.example.com/IPO"
declare namespace zips="http://www.example.com/zips"

define function zip-ok($a as element of type ipo:USAddress)
  as xs:boolean
{ 
  some $i in document("zips.xml")/zips:zips/zips:row
  satisfies $i/zips:city = $a/ipo:city
        and $i/zips:state = $a/ipo:state
        and $i/zips:zip = $a/ipo:zip
} 
--

import schema "ipo.xsd"
import schema "postals.xsd"
declare namespace ipo="http://www.example.com/IPO"
declare namespace pst="http://www.example.com/postals"

define function postal-ok($a as element of type ipo:UKAddress)
  as xs:boolean
{
  some $i in document("postals.xml")/pst:postals/pst:row
  satisfies $i/pst:city = $a/ipo:city
       and xf:starts-with($a/ipo:postcode, $i/pst:prefix)
} 
--

import schema "ipo.xsd"
import schema "postals.xsd"
import schema "zips.xsd"
declare namespace ipo="http://www.example.com/IPO"
declare namespace pst="http://www.example.com/postals"
declare namespace zips="http://www.example.com/zips"

define function postal-ok($a as element of type ipo:UKAddress)
  as xs:boolean
{
  some $i in document("postals.xml")/pst:postals/pst:row
  satisfies $i/pst:city = $a/ipo:city
       and starts-with($a/ipo:postcode, $i/pst:prefix)
} 

define function zip-ok($a as element of type ipo:USAddress)
  as xs:boolean
{ 
  some $i in document("zips.xml")/zips:zips/zips:row
  satisfies $i/zips:city = $a/ipo:city
        and $i/zips:state = $a/ipo:state
        and $i/zips:zip = $a/ipo:zip
}

define function address-ok($a as element of type ipo:Address)
 as xs:boolean
{
  typeswitch ($a)
      case $zip as element of type ipo:USAddress
           return zip-ok($zip)
      case $postal as element of type ipo:UKAddress 
           return postal-ok($postal) 
      default return false()
}

for $p in document("ipo.xml")//ipo:purchaseOrder
where not( address-ok($p/ipo:shipTo) and address-ok($p/ipo:billTo))
return $p 
--

import schema "ipo.xsd"
declare namespace ipo="http://www.example.com/IPO"

define function names-match( $s as element ipo:shipTo context ipo:purchaseOrder, 
                             $b as element ipo:billTo context ipo:purchaseOrder )
  as xs:boolean
{
     $s/ipo:name = $b/ipo:name
}
 
for $p in document("ipo.xml")//ipo:purchaseOrder
where not( names-match( $p/ipo:shipTo, $p/ipo:billTo ) )
return $p                        
--

import schema "ipo.xsd"
declare namespace ipo="http://www.example.com/IPO"

for $p in document("ipo.xml")//ipo:purchaseOrder,
    $s in $p/ipo:shipTo
where not( $s instance of element of type ipo:USAddress)
  and exists( $p//ipo:USPrice )
return $p 
--

import schema "ipo.xsd"
declare namespace ipo="http://www.example.com/IPO"

define function comment-text($c as element ipo:comment)
  as xs:string
{
    string( $c )
}

for $p in document("ipo.xml")//ipo:purchaseOrder,
    $t in comment-text( $p//ipo:shipComment )
where $p/ipo:shipTo/ipo:name="Helen Zoe"
    and $p/ipo:orderDate = date("1999-12-01")
return $t 
--

for $p in document("ipo.xml")//ipo:purchaseOrder
where $p/ipo:shipTo/ipo:name="Helen Zoe"
  and $p/@orderDate = date("1999-12-01")
return comment-text( $p//ipo:customerComment )    
--

import schema "ipo.xsd"
declare namespace ipo="http://www.example.com/IPO"

for $p in document("ipo.xml")//ipo:purchaseOrder
where $p/ipo:shipTo/ipo:name="Helen Zoe"
  and $p/@orderDate = date("1999-12-01")
return ($p//ipo:customerComment  | $p//ipo:shipComment  | $p//ipo:comment )    
--

import schema "ipo.xsd"
declare namespace ipo="http://www.example.com/IPO"

define function comments-for-element( $e as element )
  as ipo:comment*
{
  let $c := $e/(ipo:customerComment  | ipo:shipComment | ipo:comment)
  return $c
}    
--

import schema "ipo.xsd"
declare namespace ipo="http://www.example.com/IPO"

define function deadbeat( $b as element ipo:billTo of type ipo:USAddress )
  as xs:boolean
{
   $b/ipo:name = document("http://www.usa-deadbeats.com/current")/deadbeats/row/name
} 
--

import schema "ipo.xsd"
declare namespace ipo="http://www.example.com/IPO"

define function total-price( $i as element ipo:item* )
  as xs:decimal
{
  let $subtotals := for $s in $i return $s/ipo:quantity * $s/ipo:USPrice
  return sum( $subtotals )
} 
--

import schema "ipo.xsd"
declare namespace ipo="http://www.example.com/IPO"

for $p in document("ipo.xml")//ipo:purchaseOrder
where $p/ipo:shipTo/ipo:name="Helen Zoe"
   and $p/ipo:orderDate = date("1999-12-01")
return total-price($p//ipo:item)

