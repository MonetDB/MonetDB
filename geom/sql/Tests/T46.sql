SELECT Distance("position", boundary)
FROM bridges, named_places
WHERE bridges.name = 'Cam Bridge'
AND named_places.name = 'Ashton';
