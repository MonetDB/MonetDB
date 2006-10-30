<result>
  {
    for $i in doc("items.xml")//item_tuple
    where $i/start_date <= "1999-01-31" (:current-date():)
      and $i/end_date >= "1999-01-31" (:current-date():)
      and contains(zero-or-one($i/description), "Bicycle")
    order by exactly-one($i/itemno)
    return
        <item_tuple>
            { $i/itemno }
            { $i/description }
        </item_tuple>
  }
</result>
