for $doc in doc("symm.xml")/documents/document
  return (insert-first($doc, element published {text{$doc/@published}}),
	  insert-first($doc, text{"&#10;"}),
	  unset-attr($doc, "published"))
