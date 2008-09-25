for $fname in distinct-values(doc("people")/people/person/leisure_activities/*)
order by $fname
return <activity> { $fname/text() } </activity>

