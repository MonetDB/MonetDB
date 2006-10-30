for $item in doc("string.xml")//news_item
where contains(string($item/content), "Gorilla Corporation")
return
    <item_summary>
        { concat($item/title,". ") }
        { concat($item/date,". ") }
        { string(($item//par)[1]) }
    </item_summary>
