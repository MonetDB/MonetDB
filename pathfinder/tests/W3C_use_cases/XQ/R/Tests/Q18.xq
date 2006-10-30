<result>
  {
    for $u in doc("users.xml")//user_tuple
    order by $u/name
    return
        <user>
            { $u/name }
            {
                for $b in distinct-values(doc("bids.xml")//bid_tuple
                                             [userid = $u/userid]/itemno)
                for $i in doc("items.xml")//item_tuple[itemno = $b]
                let $descr := $i/description/text()
                order by $descr
                return
                    <bid_on_item>{ $descr }</bid_on_item>
            }
        </user>
  }
</result>
