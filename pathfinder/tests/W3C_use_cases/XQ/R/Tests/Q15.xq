<result>
  {
    for $u in doc("users.xml")//user_tuple
    let $b := doc("bids.xml")//bid_tuple[userid=$u/userid and bid>=100]
    where count($b) > 1
    return
        <big_spender>{ $u/name/text() }</big_spender>
  }
</result>
