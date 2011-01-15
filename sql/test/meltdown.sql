create table a ( id integer, var1 clob, var2 clob, var3 integer, var4 clob );

INSERT INTO a (id, var1, var2, var3, var4) VALUES (1, 'aaaa',
'Bibliotheek 21 Nederlandse Bibliotheek Inhoud Introductie Hoe kunt u de
boeken lezen? De Bijbel Literatuur gesorteerd op auteur Opmerkingen en
suggesties Introductie Wat kunt u hier in deze Nederlandse Bibliotheek
verwachten? U', 690775351, 'hier');
INSERT INTO a (id, var1, var2, var3, var4) VALUES (2, 'aaab', 'God is,
en het welzijn van de mensen bevordert. Welke', 1686169245, 'worde');
INSERT INTO a (id, var1, var2, var3, var4) VALUES (3, 'aaac', 'Wie zit
er achter deze bibliotheek? Een zendeling die het als zijn taak ziet om
het evangelie te verkondigen tot aan de uiterste einden van de wereld.
Welke nieuwe boeken zijn onlangs toegevoegd? Er is een lijst met
veranderingen beschikbaar.', 723830965, '');


select * from a;
--- drop view a;

commit;
