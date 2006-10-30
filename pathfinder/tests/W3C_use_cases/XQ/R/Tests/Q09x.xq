<result>
  {
    let $end_dates := doc("items.xml")//item_tuple/end_date
    for $m in distinct-values(for $e in $end_dates 
                              return month-from-date($e))
    let $item := doc("items.xml")
        //item_tuple[year-from-date(end_date) = 1999 
                     and month-from-date(end_date) = $m]
    order by $m
    return
        <monthly_result>
            <month>{ $m }</month>
            <item_count>{ count($item) }</item_count>
        </monthly_result>
  }
</result>
