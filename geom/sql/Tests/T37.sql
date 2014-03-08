SELECT Equals(boundary, PolyFromText('POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )',1))
FROM named_places
WHERE name = 'Goose Island';
