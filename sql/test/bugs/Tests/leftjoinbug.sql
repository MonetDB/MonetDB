
START TRANSACTION;

CREATE TABLE "ljb1" (
    "id"        integer,
    "name"      varchar(500),
    "notes"     varchar(500),
    "picture"   varchar(500),
    "ljb2"    varchar(500),
    PRIMARY KEY ("id")
);

CREATE TABLE "ljb2" (
    "id"            integer,
    "ljb3"         integer,
    "name"          varchar(500),
    "dateofbirth"   varchar(500),
    "hair"          varchar(500),
    "date"          varchar(500),
    "location"      varchar(5000),
    "picture"       varchar(5000),
    "features"      varchar(5000),
    PRIMARY KEY ("id")
);

CREATE TABLE "ljb3" (
    "ljb1_id"  integer,
    "ljb3_id"      integer,
    PRIMARY KEY ("ljb1_id", "ljb3_id"),
    FOREIGN KEY ("ljb1_id") REFERENCES ljb1
);

INSERT INTO "ljb1" VALUES (0, 'Prince Albert Victor', 'one of the most famous ljb1', '', NULL);
INSERT INTO "ljb1" VALUES (1, 'Joseph Barnett', 'was not described as a Ripper ljb1 until the 1970s', '', 'Mary Kelly');
INSERT INTO "ljb1" VALUES (2, 'Alfred Napier Blanchard', 'made a false confession', '', NULL);
INSERT INTO "ljb1" VALUES (3, 'William Henry Bury', 'Police at the time investigated the matter but did not seem to consider Bury a viable ljb1', '', 'Polly Nichols');
INSERT INTO "ljb1" VALUES (4, 'Lewis Carroll', 'a very unlikely ljb1', '', NULL);
INSERT INTO "ljb1" VALUES (5, 'David Cohen', 'a poor Polish Jew, living in Whitechapel and who had "homocidal tendensies and a great hatred of women", and was confined to a lunatic asylum at the right time for the murders to stop and died shortly afterwards', '', 'Catherine Eddowes');
INSERT INTO "ljb1" VALUES (6, 'Dr. Thomas Neill Cream', 'did commit murders, but by poisoning', 'http://www.casebook.org/ljb1/cream.html', NULL);
INSERT INTO "ljb1" VALUES (7, 'Frederick Bailey Deeming', 'The only two links he may have had with the Whitechapel murders were (1) his insanity and (2) his method of killing his family', '', NULL);
INSERT INTO "ljb1" VALUES (8, 'Montague John Druitt', 'He is considered by many to be the number one ljb1 in the case, yet there is little evidence', '', NULL);
INSERT INTO "ljb1" VALUES (9, 'Fogelma', 'no outside evidence to corroborate the story told by the Empire News', '', NULL);
INSERT INTO "ljb1" VALUES (10, 'George Hutchinson (Br.)', 'Police at the time interviewed him but did not seem to consider him a ljb1', '', NULL);
INSERT INTO "ljb1" VALUES (11, 'Mrs. Mary Pearcey', 'Not very likely "Jill the Ripper"-theory, first published in 1939', '', 'Mary Kelly');
INSERT INTO "ljb1" VALUES (12, 'James Kelly', 'there are some reasons in favour of and some against ljb1ing him', '', NULL);
INSERT INTO "ljb1" VALUES (13, 'Severin Klosowski (George Chapman)', 'there are some reasons in favour of and some against ljb1ing him', '', 'Carrie Brown');
INSERT INTO "ljb1" VALUES (14, 'Aaron Kosminski', 'According to Anderson and Swanson, identified by a witness as the Ripper, but no charges were brought against him due to the witness"s reluctance to testify against "a fellow Jew." Known to have been insane.', '', NULL);
INSERT INTO "ljb1" VALUES (15, 'Jacob Levy', 'a butcher, and the cuts inflicted upon Catharine Eddowes were suggestive of a butcher', '', 'Catharine Eddowes');
INSERT INTO "ljb1" VALUES (16, 'The Lodger (Frances Tumblety)', 'a very strong ljb1', '', NULL);
INSERT INTO "ljb1" VALUES (17, 'James Maybrick', 'The mysterious emergence of the so-called Maybrick journal in 1992 however, immediately thrust him to the forefront of credible Ripper ljb1.', '', NULL);
INSERT INTO "ljb1" VALUES (18, 'Michael Ostrog', 'Mentioned for the first time as a ljb1 in 1894,in 1994 a lot of information was published making him a prime ljb1', '', NULL);
INSERT INTO "ljb1" VALUES (19, 'Dr. Alexander Pedachenko', 'may never have existed', '', NULL);
INSERT INTO "ljb1" VALUES (20, 'The Royal Conspiracy', 'a fascinating tapestry of conspiracy involving virtually every person who has ever been a Ripper ljb1 plus a few new ones', '', NULL);
INSERT INTO "ljb1" VALUES (21, 'Walter Sickert', 'a valid ljb1 since the 1990s', '', NULL);
INSERT INTO "ljb1" VALUES (22, 'James Kenneth Stephen', 'Known misogynist and lunatic but no connections with the East End', '', NULL);
INSERT INTO "ljb1" VALUES (23, 'R. D´Onston Stephenson', 'Known to have had an extraordinary interest in the murders. Wrote numerous articles and letters on the matter. Resided in the East End.', '', NULL);
INSERT INTO "ljb1" VALUES (24, 'Alois Szemeredy', 'from Buenos Aires, ljb1ed of the Jack the Ripper- and other murders', '', NULL);
INSERT INTO "ljb1" VALUES (25, 'Francis Thompson', 'At 29, Thompson was the right age to fit the Ripper descriptions, and we know he had some medical training. He was also said to carry a dissecting scalpel around with him, which he claimed he used to shave', '', NULL);
INSERT INTO "ljb1" VALUES (26, 'Frances Tumblety', 'There is a strong case to be made that he was indeed the Batty Street Lodger', '', NULL);
INSERT INTO "ljb1" VALUES (27, 'Nikolay Vasiliev', 'an elusive legend, which probably had some basis in reality, but was mostly embellished by the journalists who wrote it up', '', NULL);
INSERT INTO "ljb1" VALUES (28, 'Dr. John Williams', 'there is very little to suggest that he was Jack the Ripper', '', NULL);

INSERT INTO "ljb2" VALUES (1, 0, 'Mary Ann Walker', '1845-08-26', 'brown hair turning grey', '1888-08-31', 'Buck"s Row by Charles Cross', '', 'five front teeth missing (Rumbelow); two bottom-one topfront (Fido), her teeth are slightly discoloured. She isdescribed as having small, delicate features with highcheekbones and grey eyes. She has a small scar on herforehead from a childhood injury.  She is described byEmily Holland as "a very clean woman who always seemedto keep to herself." The doctor at the post mortemremarked on the cleanliness of her thighs.  She is alsoan alcoholic.');

INSERT INTO "ljb2" VALUES (3, 2, 'Elisabeth Stride', '1843-11-27', 'curly dark brown', '1888-09-30', 'Berner Street (Henriques Street today)', '', 'pale complexion, all the teeth in her lower left jaw were missing');
INSERT INTO "ljb2" VALUES (4, 3, 'Catherine Eddowes', '1842-04-14', 'dark auburn', '1888-09-30', 'Mitre Square', '', 'She has a tattoo in blue ink on her left forearm "TC."');
INSERT INTO "ljb2" VALUES (5, 4, 'Mary Jane Kelly', 'around 1863', 'blonde', '1888-11-09', '13 Miller"s Court', '', 'a fair complexion. "Said to have been possessed of considerable personal attractions." (McNaughten) She was last seen wearing a linsey frock and a red shawl pulled around her shoulders. She was bare headed. Detective Constable Walter Dew claimed to know Kelly well by sight and says that she was attractive and paraded around, usually in the company of two or three friends. He says she always wore a spotlessly clean white apron.Maria Harvey, a friend, says that she was "much superior to that of most persons in her position in life."');
INSERT INTO "ljb2" VALUES (6, 5, '"Fairy Fay"', 'unknown', 'unknown', '1887-12-26', 'the alleys of Commercial Road', '', 'not recorded');
INSERT INTO "ljb2" VALUES (7, 6, 'Annie Millwood', '1850', NULL, '1888-02-15', 'White"s Row, Spitalfields', '', '');
INSERT INTO "ljb2" VALUES (8, 7, 'Ada Wilson', NULL, NULL, '(survived the attack on 1888-28-03)', '19 Maidman Street', '', '');
INSERT INTO "ljb2" VALUES (9, 8, 'Emma Smith', '1843', NULL, '1888-03-03', 'just outside Taylor Brothers Mustard and Cocoa Mill which was on the north-east corner of the Wentworth/Old Montague Street crossroads', '', '');
INSERT INTO "ljb2" VALUES (10, 9, 'Martha Tabram', '1849-05-10', NULL, '1888-08-07', 'George Yard, a narrow north-south alley connecting Wentworth Street and Whitechapel High Street', '', '');
INSERT INTO "ljb2" VALUES (11, 10, 'Whitehall Mystery', NULL, NULL, '1888-10-03', 'a vault soon to become a section of the cellar of New Scotland Yard', '', 'the headless and limbless torso of a woman');
INSERT INTO "ljb2" VALUES (12, 11, 'Annie Farmer', '1848', NULL, '(survived the attack on 1888-11-20)', NULL, '', '');
INSERT INTO "ljb2" VALUES (13, 12, 'Rose Mylett', '1862', NULL, '1888-12-20', 'the yard between 184 and 186 Poplar High Street, in Clarke"s Yard', '', '');
INSERT INTO "ljb2" VALUES (14, 13, 'Elisabeth Jackson', NULL, NULL, '1889-06', 'the Thames', '', '');
INSERT INTO "ljb2" VALUES (15, 14, 'Alice MacKenzie', '1849', NULL, '1889-07-17', 'Castle Alley', '', 'as a freckle-faced woman with a penchant for both smoke and drink. Her left thumb was also injured in what was no doubt some sort of industrial accident.');
INSERT INTO "ljb2" VALUES (16, 15, 'Pinchin Street Murder, possibly Lydia Hart', NULL, NULL, '1889-09-10', 'under a railway arch in Pinchin Street', '', 'body, missing both head and legs');
INSERT INTO "ljb2" VALUES (17, 16, 'Frances Coles', '1865', NULL, '1891-02-13', 'Swallow Gardens', '', 'is often heralded as the prettiest of all the murder ljb2');
INSERT INTO "ljb2" VALUES (18, 17, 'Carrie Brown', NULL, NULL, '1891-04-24', 'the room of the East River Hotel on the Manhattan waterfront of New York, U.S.A.', '', NULL);
INSERT INTO "ljb3" VALUES (13, 17); 
COMMIT;

select ljb2.ljb3, ljb2.id, ljb2.name, ljb3.ljb1_id, ljb1.name 
from 
	ljb2
left join 
	ljb3 
		on ljb3.ljb3_id = ljb2.ljb3 
left join 
	ljb1 
		on ljb1.id = ljb3.ljb1_id;

select ljb2.ljb3, ljb2.id, ljb2.name, ljb1.id, ljb1.name 
from 
	ljb2 
left join 
	ljb3 
		on ljb3.ljb3_id = ljb2.ljb3 
left join 
	ljb1 
		on ljb1.id = ljb3.ljb1_id;

drop table ljb2;
drop table ljb3;
drop table ljb1;
