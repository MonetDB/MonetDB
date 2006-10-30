<result>
  {
    for $u in doc("users.xml")//user_tuple
    let $b := doc("bids.xml")//bid_tuple[userid = $u/userid]
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
