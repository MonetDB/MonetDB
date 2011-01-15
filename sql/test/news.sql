CREATE TABLE news (
  id mediumint,
  content text,
  user_id int,
  "date" timestamp,
  PRIMARY KEY (id)
);

INSERT INTO news (id, content, user_id, "date") VALUES
(1, 'dwerg.net word volwassen, er is eindelijk content
en het voicemail archief begint al aardig vol te lopen.
De commentaar en rating code voor de voicemails is ook
al vrij aardig aan het vorderen. Dus dat beloofd nog
wat.\r\n<br /><br />\r\nMja, het is dat ik veel te veel
tijd heb en het helemaal verkeerd indeel dat het
allemaal nogal lang duurt, maar in principe is het nog
maar een uurtje of 6 werk om het goed te krijgen. Zoals
je kan zien heb ik bij de voicemails al wat leuke
knopjes gezet, en het aantal voicemails dat je in 1
keer ziet verhoogd naar 15. Poepoe.\r\n<br /><br
/>\r\nHet is me allemaal wat...\r\n<br /><br
/>\r\nMuziek: Air', 1, timestamp '2002-02-22 00:21:00');
