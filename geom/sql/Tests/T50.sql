SELECT AsText(SymDifference(shore, boundary))
FROM lakes, named_places
WHERE lakes.name = 'Blue Lake'
AND named_places.name = 'Ashton';
