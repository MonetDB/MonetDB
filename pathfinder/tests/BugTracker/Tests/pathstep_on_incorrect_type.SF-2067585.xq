let $r :=
    (for $p in doc("auctionG.xml")//person
        let $ca := doc("auctionG.xml")//closed_auction[./seller/@person=$p/@id]
            where not(empty($ca))
                order by sum($ca/price)
                    return ($p, sum($ca/price)))
    return ($r, $r/name)
