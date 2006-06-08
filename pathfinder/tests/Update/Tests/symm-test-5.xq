for $doc in doc("symm.xml")/documents/document
  return (do insert element published {text{$doc/@published}} as first into $doc,
	  do insert text{"&#10;"} as first into $doc,
	  do delete $doc/@published)
