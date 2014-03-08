SELECT AsText("Union"(shore, boundary))
FROM lakes, named_places
WHERE lakes.name = 'Blue Lake'
AND namedplaces.name = ‘Goose Island’;
