<result>
  {
    for $u in doc("users.xml")//user_tuple
    for $i in doc("items.xml")//item_tuple
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
