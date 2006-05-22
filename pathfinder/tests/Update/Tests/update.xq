(:
test set-attr with the following tests:
-- non-existing attr non-existing qn non-existing value
-- non-existing attr existing qn non-existing value
-- non-existing attr non-existing qn existing value
-- non-existing attr existing qn existing value
-- existing attr non-existing value
-- existing attr existing value
test unset-attr with the following tests:
-- existing attr
-- non-existing attr
-- just inserted attr
:)
for $elem in doc("testdoc.xml")/document/element
	return (set-attr($elem, "author", "Sjoerd Mullender"),
		set-attr($elem, "title", "test element"),
		set-attr($elem, "attr", text{$elem/@attribute}),
		set-attr($elem, "attribute", "attribute value"),
		set-attr($elem, "copyright", text{$elem/../@copyright}),
		unset-attr($elem, "dummy"),
		unset-attr($elem, "non-existent"),
		set-attr($elem, "to-be-removed", "nonsense"),
		unset-attr($elem, "to-be-removed"))
,
(: test set-comment with the following tests:
-- new text
:)
for $elem in doc("testdoc.xml")//comment()
	return set-comment($elem, concat(text{$elem}, "foo "))
,
(: test set-text with the following tests:
-- new text
:)
for $elem in doc("testdoc.xml")/document/element/text()
	return set-text($elem, concat("element ", text{$elem}))
,
(: test set-pi with the following tests:
-- existing instruction, new target
-- new instruction, existing target
-- new instruction, new target
:)
(set-pi(exactly-one(doc("testdoc.xml")//processing-instruction(pi0000)), "pi0000", "foo"),
 set-pi(exactly-one(doc("testdoc.xml")//processing-instruction(pi1000)), "bar", "value 1000"),
 set-pi(exactly-one(doc("testdoc.xml")//processing-instruction(pi2000)), "target", "instruction"))
,
(: test delete with the following tests:
-- delering text
-- deleting element
-- deleting comment
-- deleting processing instruction
:)
(delete(exactly-one(doc("testdoc.xml")/document/text()[1])),
 delete(exactly-one(doc("testdoc.xml")/document/element[10])),
 delete(exactly-one(doc("testdoc.xml")/document/comment()[3])),
 delete(exactly-one(doc("testdoc.xml")//processing-instruction("pi4000"))))
,
(: test insert with the following tests:
-- test multiple insert-first
-- test multiple insert-last
-- test multiple insert-before
-- test multiple insert-after
:)
for $elem in doc("testdoc.xml")/document/element[@attribute = 10]
  return (insert-first($elem, <a/>),
	  insert-first($elem, <b/>))
,
for $elem in doc("testdoc.xml")/document/element[@attribute = 20]
  return (insert-last($elem, <a/>),
	  insert-last($elem, <b/>))
,
for $elem in doc("testdoc.xml")/document/element[@attribute = 30]
  return (insert-before($elem, <a/>),
	  insert-before($elem, <b/>))
,
for $elem in doc("testdoc.xml")/document/element[@attribute = 40]
  return (insert-after($elem, <a/>),
	  insert-after($elem, <b/>))
