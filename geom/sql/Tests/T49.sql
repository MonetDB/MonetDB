-- Conformance Item T49
SELECT AsText("Union"(shore, boundary)) FROM lakes, named_places WHERE lakes.name = 'Blue Lake' AND named_places.name = 'Goose Island';
