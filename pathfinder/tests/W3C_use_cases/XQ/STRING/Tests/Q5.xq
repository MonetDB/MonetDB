for $item in doc("string.xml")//news_item
where contains(string(zero-or-one($item/content)), "Gorilla Corporation")
return
    <item_summary>
        { concat(zero-or-one($item/title),". ") }
        { concat(zero-or-one($item/date),". ") }
        { string(zero-or-one(($item//par)[1])) }
    </item_summary>
