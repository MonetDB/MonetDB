<result>
  {
    let $end_dates := doc("items.xml")//item_tuple/end_date
    for $m in distinct-values(for $e in $end_dates 
(:                            return month-from-date($e)):)
                              return substring($e,6,2))
    let $item := doc("items.xml")
(:
        //item_tuple[year-from-date(end_date) = 1999
                     and month-from-date(end_date) = $m]
:)
        //item_tuple[substring(exactly-one(end_date),1,4) = "1999"
                     and substring(exactly-one(end_date),6,2) = $m]
    order by $m
    return
        <monthly_result>
            <month>{ $m }</month>
            <item_count>{ count($item) }</item_count>
        </monthly_result>
  }
</result>
