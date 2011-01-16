START TRANSACTION;

CREATE TABLE INSPECTOR (
"inspector_id" INTEGER NOT NULL PRIMARY KEY,
"title" VARCHAR(255),
"other_title" VARCHAR(255) ,
"name" VARCHAR(255) ,
"other_names" VARCHAR(255),
"surname" VARCHAR(255) NOT NULL,
"picture" CLOB
);

CREATE TABLE VICTIM (
"victim_id" INTEGER NOT NULL PRIMARY KEY,
"first_name" VARCHAR(255),
"other_names" VARCHAR(255),
"surname" VARCHAR(255) NOT NULL,
"date_of_birth" VARCHAR(255),
"length" VARCHAR(255),
"eyes" VARCHAR(255),
"hair" VARCHAR(255),
"date" VARCHAR(255),
"location" CLOB,
"picture" CLOB,
"features" CLOB
);

CREATE TABLE SUSPECT (
"suspect_id" INTEGER NOT NULL PRIMARY KEY,
"name" VARCHAR(255) NOT NULL,
"picture" CLOB,
"notes" CLOB,
"victim_id" INTEGER
);

CREATE TABLE WITNESS (
"witness_id" INTEGER NOT NULL PRIMARY KEY,
"name" VARCHAR(255) NOT NULL,
"time" TIME,
"appearence" CLOB,
"diction" CLOB
);

CREATE TABLE DOCTOR (
"doctor_id" INTEGER NOT NULL PRIMARY KEY,
"title" VARCHAR(255),
"first_name" VARCHAR(255),
"other_names" VARCHAR(255),
"surname" VARCHAR(255) NOT NULL,
"picture" CLOB
);

CREATE TABLE SCENE (
"scene_id" INTEGER NOT NULL PRIMARY KEY,
"victim_id" INTEGER NOT NULL,
"suspect_id" INTEGER
);

CREATE TABLE TIMELINE (
"event_id" INTEGER NOT NULL PRIMARY KEY,
"time" TIME,
"fact" CLOB,
"victim_id" INTEGER
);

CREATE TABLE SCENE_WITNESS (
"scene_id" INTEGER NOT NULL,
"witness_id" INTEGER NOT NULL
);

CREATE TABLE SCENE_DOCTOR (
"scene_id" INTEGER NOT NULL,
"doctor_id" INTEGER NOT NULL
);

CREATE TABLE SCENE_INSPECTOR (
"scene_id" INTEGER NOT NULL,
"inspector_id" INTEGER NOT NULL
);


ALTER TABLE SUSPECT ADD FOREIGN KEY ("victim_id")
	REFERENCES VICTIM ("victim_id");
ALTER TABLE SCENE ADD FOREIGN KEY ("victim_id")
	REFERENCES VICTIM ("victim_id");
ALTER TABLE SCENE ADD FOREIGN KEY ("suspect_id")
	REFERENCES SUSPECT ("suspect_id");
ALTER TABLE TIMELINE ADD FOREIGN KEY ("victim_id")
	REFERENCES VICTIM ("victim_id");
ALTER TABLE SCENE_WITNESS ADD PRIMARY KEY ("scene_id", "witness_id");
ALTER TABLE SCENE_WITNESS ADD FOREIGN KEY ("scene_id")
	REFERENCES SCENE ("scene_id");
ALTER TABLE SCENE_WITNESS ADD FOREIGN KEY ("witness_id")
	REFERENCES WITNESS ("witness_id");
ALTER TABLE SCENE_DOCTOR ADD PRIMARY KEY ("scene_id", "doctor_id");
ALTER TABLE SCENE_DOCTOR ADD FOREIGN KEY ("scene_id")
	REFERENCES SCENE ("scene_id");
ALTER TABLE SCENE_DOCTOR ADD FOREIGN KEY ("doctor_id")
	REFERENCES DOCTOR("doctor_id");
ALTER TABLE SCENE_INSPECTOR ADD PRIMARY KEY ("scene_id", "inspector_id");
ALTER TABLE SCENE_INSPECTOR ADD FOREIGN KEY ("scene_id")
	REFERENCES SCENE ("scene_id");
ALTER TABLE SCENE_INSPECTOR ADD FOREIGN KEY ("inspector_id")
	REFERENCES INSPECTOR("inspector_id");



INSERT INTO VICTIM VALUES (1,'Mary', 'Ann', 'Walker', '1845-08-26', '5\'12\"', 'brown', 'brown hair turning grey', '1888-08-31', 'Buck\'s Row by Charles Cross', 'http://www.casebook.org/images/victims_nichols.jpg', 'five front teeth missing (Rumbelow); two bottom-one top front (Fido), her teeth are slightly discoloured. She is described as having small, delicate features with high cheekbones and grey eyes. She has a small scar on her forehead from a childhood injury.  She is described by Emily Holland as \"a very clean woman who always seemed to keep to herself.\" The doctor at the post mortem remarked on the cleanliness of her thighs.  She is also an alcoholic.');
INSERT INTO VICTIM VALUES (2,'Annie', NULL, 'Chapman', '1841-09', '5\'', 'blue', 'dark brown, wavy', '1888-09-08', 'B29 Hanbury Street', 'http://www.casebook.org/images/victims_chapman.jpg', 'Pallid complexion, excellent teeth (possibly two missing in lower jaw), strongly built (stout), thick nose');
INSERT INTO VICTIM VALUES (3,'Elisabeth', NULL, 'Stride', '1843-11-27', '5\'5\"', 'light gray', 'curly dark brown', '1888-09-30', 'Berner Street (Henriques Street today)', 'http://www.casebook.org/images/victims_stride.jpg', 'pale complexion, all the teeth in her lower left jaw were missing');
INSERT INTO VICTIM VALUES (4,'Catherine', NULL, 'Eddowes', '1842-04-14', '5\'', 'hazel', 'dark auburn', '1888-09-30', 'Mitre Square', 'http://www.casebook.org/images/eddowes1.jpg', 'She has a tattoo in blue ink on her left forearm \"TC.\"');
INSERT INTO VICTIM VALUES (5,'Mary', 'Jane', 'Kelly', 'around 1863', '5\'7\"', 'blue', 'blonde', '1888-11-09', '13 Miller\'s Court', 'http://www.casebook.org/images/victims_kelly.jpg', 'a fair complexion. \"Said to have been possessed of considerable personal attractions.\" (McNaughten) She was last seen wearing a linsey frock and a red shawl pulled around her shoulders. She was bare headed. Detective Constable Walter Dew claimed to know Kelly well by sight and says that she was attractive and paraded around, usually in the company of two or three friends. He says she always wore a spotlessly clean white apron. Maria Harvey, a friend, says that she was \"much superior to that of most persons in her position in life.\"');
INSERT INTO VICTIM VALUES (6,NULL, NULL, 'Fairy Fay', NULL, NULL, NULL, NULL, '1887-12-26', 'the alleys of Commercial Road', 'http://www.casebook.org/images/victims_fairy.jpg', 'not recorded');
INSERT INTO VICTIM VALUES (7,'Annie', NULL, 'Millwood', '1850', NULL, NULL, NULL, '1888-02-15', 'White\'s Row, Spitalfields', 'http://www.casebook.org/images/victims_millwood.jpg', NULL);
INSERT INTO VICTIM VALUES (8,'Ada', NULL, 'Wilson', NULL, NULL, NULL, NULL, '(survived the attack on 1888-28-03)', '19 Maidman Street', 'http://www.casebook.org/images/victims_wilson.jpg', NULL);
INSERT INTO VICTIM VALUES (9,'Emma', NULL, 'Smith', '1843', NULL, NULL, NULL, '1888-03-03', 'just outside Taylor Brothers Mustard and Cocoa Mill which was on the north-east corner of the Wentworth/Old Montague Street crossroads', 'http://www.casebook.org/images/victims_smith.jpg', NULL);
INSERT INTO VICTIM VALUES (10,'Martha', NULL, 'Tabram', '1849-05-10', NULL, NULL, NULL, '1888-08-07', 'George Yard, a narrow north-south alley connecting Wentworth Street and Whitechapel High Street', 'http://www.casebook.org/images/victims_tabram.jpg', NULL);
INSERT INTO VICTIM VALUES (11,'Whitehall', NULL, 'Mystery', NULL, NULL, NULL, NULL, '1888-10-03', 'a vault soon to become a section of the cellar of New Scotland Yard', 'http://www.casebook.org/images/victims_whitehall.jpg', 'the headless and limbless torso of a woman');
INSERT INTO VICTIM VALUES (12,'Annie', NULL, 'Farmer', NULL, NULL, NULL, NULL, '(survived the attack on 1888-11-20)', NULL, 'http://www.casebook.org/images/victims_farmer.jpg', NULL);
INSERT INTO VICTIM VALUES (13,'Rose', NULL, 'Mylett', '1862', NULL, NULL, NULL, '1888-12-20', 'the yard between 184 and 186 Poplar High Street, in Clarke\'s Yard', 'http://www.casebook.org/images/victims_mylett.jpg', NULL);
INSERT INTO VICTIM VALUES (14,'Elisabeth', NULL, 'Jackson', NULL, NULL, NULL, NULL, '1889-06', 'the Thames ', 'http://www.casebook.org/images/victims_jackson.jpg', NULL);
INSERT INTO VICTIM VALUES (15,'Alice', NULL, 'MacKenzie', '1849', NULL, NULL, NULL, '1889-07-17', 'Castle Alley', 'http://www.casebook.org/images/victims_mckenzie.jpg', 'as a freckle-faced woman with a penchant for both smoke and drink. Her left thumb was also injured in what was no doubt some sort of industrial accident');
INSERT INTO VICTIM VALUES (16,NULL, NULL, 'Pinchin Street Murder, possibly Lydia Hart', NULL, NULL, NULL, NULL, '1889-09-10', 'under a railway arch in Pinchin Street', 'http://www.casebook.org/images/victims_pinchin.jpg', 'body, missing both head and legs');
INSERT INTO VICTIM VALUES (17, 'Frances', NULL, 'Coles', '1865', NULL, NULL, NULL, '1891-02-13', 'Swallow Gardens  ', 'http://www.casebook.org/images/victims_coles.jpg', ' is often heralded as the prettiest of all the murder victims');
INSERT INTO VICTIM VALUES (18, 'Carrie', NULL, 'Brown', NULL, '5\' 8\"', NULL, NULL, '1891-04-24', ' the room of the East River Hotel on the Manhattan waterfront of New York, U.S.A.', 'http://www.casebook.org/images/victims_brown.jpg', NULL);


INSERT INTO WITNESS VALUES (1, 'Patrick Mulshaw', '04:00:00', 'suspicious', 'Watchman, old man, I believe somebody is murdered down the street.');
INSERT INTO WITNESS VALUES (2, 'Emily Walter', '02:00:00', 'Foreigner aged 37, dark beard and moustache. Wearing short dark jacket, dark vest and trousers, black scarf and black felt hat.', 'Asked witness to enter the backyard of 29 Hanbury Street.');
INSERT INTO WITNESS VALUES (3, 'Elizabeth Long', '05:30:00', 'Dark complexion, brown deerstalker hat, possibly a dark overcoat. Aged over 40, somewhat taller than Chapman. A foreigner of \"shabby genteel.\"', '\"Will you?\"');
INSERT INTO WITNESS VALUES (4, 'J. Best and John Gardner', '23:00:00', '5\'5\" tall, English, black moustache, sandy eyelashes, weak, wearing a morning suit and a billycock hat.', NULL);
INSERT INTO WITNESS VALUES (5, 'William Marshall', '23:45:00', 'mall, black coat, dark trousers, middle aged, round cap with a small sailor-like peak. 5\'6\", stout, appearance of a clerk. No moustache, no gloves, with a cutaway coat.', '"You would say anything but your prayers." Spoken mildly, with an English accent, and in an educated manner.');
INSERT INTO WITNESS VALUES (6, 'Matthew Packer', '12:30:00', 'Aged 25-30, 5\'7\", long black coat buttoned up, soft felt hawker hat, broad shoulders. Maybe a young clerk, frock coat, no gloves.', 'Quiet in speaking, with a rough voice');
INSERT INTO WITNESS VALUES (7, 'P.C. William Smith', '00:30:00', 'Aged 28, cleanshaven and respectable appearance, 5\'7\", hard dark felt deerstalker hat, dark clothes. Carrying a newspaper parcel 18 x 7 inches.', NULL);
INSERT INTO WITNESS VALUES (8, 'Israel Schwartz', '00:45:00', 'First man: Aged 30, 5\'5\", brown haired, fair complexion, small brown moustache, full face, broad shoulders, dark jacket and trousers, black cap with peak. Second man: Aged 35, 5\'11\", fresh complexion, light brown hair, dark overcoat, old black hard felt hat with a wide brim, clay pipe.', '\"Lipski!\"');
INSERT INTO WITNESS VALUES (9, 'Joseph Lawende', '01:30:00', 'Aged 30, 5\'7\", fair complexion, brown moustache, salt-and-pepper coat, red neckerchief, grey peaked cloth cap. Sailor-like.', NULL);
INSERT INTO WITNESS VALUES (10, 'James Blenkinsop', '01:30:00', 'Well-dressed.', 'Have you seen a man and a woman go through here?');
INSERT INTO WITNESS VALUES (11, 'Mary Ann Cox', '23:45:00', 'Short, stout man, shabbily dressed. Billycock hat, blotchy face, carroty moustache, holding quart can of beer', NULL);
INSERT INTO WITNESS VALUES (12, 'George Hutchinson', '02:00:00', 'Aged 34-35, 5\'6\", pale complexion, dark hair, slight moustached curled at each end, long dark coat, collar cuffs of astrakhan, dark jacket underneath. Light waistcoat, thick gold chain with a red stone seal, dark trousers an\' button boots, gaiters, white buttons. White shirt, black tie fastened with a horseshoe pin. Dark hat, turned down in middle. Red kerchief. Jewish and respectable in appearance.', NULL);
INSERT INTO WITNESS VALUES (13, 'Ada Wilson herself', NULL, 'a man of about 30 years of age, 5ft 6ins in height, with a sunburnt face and a fair moustache. He was wearing a dark coat, light trousers and a wideawake hat.', NULL);
INSERT INTO WITNESS VALUES (14, 'Rose Bierman', NULL, 'a young fair man with a light coat on', NULL);
INSERT INTO WITNESS VALUES (15, '"Jumbo" Friday', NULL, NULL, NULL);
INSERT INTO WITNESS VALUES (16, 'Duncan Campnell', NULL, NULL, NULL);
INSERT INTO WITNESS VALUES (17, 'Mary Miniter', '10:45:00', 'About 32 years of age. Five feet, eight inches tall. Slim build. Long, sharp nose. Heavy moustache of light color. Foreign in appearance, possibly German. Dark-brown cutaway coat. Black trousers. Old black derby hat with dented crown. ', NULL);
INSERT INTO WITNESS VALUES (18, 'James Brown', '00:45:00', '5\'7", stout, long black diagonal coat which reached almost to his heels.', NULL);


INSERT INTO INSPECTOR VALUES (1, 'Chief', 'Constable', 'Adolphus', 'Frederick', 'Williamson', 'http://www.casebook.org/images/police_frederick_williamson.jpg');
INSERT INTO INSPECTOR VALUES (2, 'Sergeant', NULL, 'Stephen', NULL, 'White', 'http://www.casebook.org/images/police_steven_white.jpg');
INSERT INTO INSPECTOR VALUES (3, 'Sir', NULL, 'Charles', NULL, 'Warren', 'http://www.casebook.org/images/police_war.jpg');
INSERT INTO INSPECTOR VALUES (4, 'P.C.', NULL, 'Edward', NULL, 'Watkins', 'http://www.casebook.org/images/police_edward_watkins.jpg');
INSERT INTO INSPECTOR VALUES (5, 'P.C.', NULL, 'Ernest', NULL, 'Thompson', 'http://www.casebook.org/images/police_ernest_thompson.jpg');
INSERT INTO INSPECTOR VALUES (6, 'Sergeant', NULL, 'William', NULL, 'Thick', 'http://www.casebook.org/images/police_thi.jpg');
INSERT INTO INSPECTOR VALUES (7, 'Chief', 'Inspector', 'Donald', NULL, 'Swanson', 'http://www.casebook.org/images/police_sut.jpg');
INSERT INTO INSPECTOR VALUES (8, 'Inspector', NULL, 'John', NULL, 'Spratling', 'http://www.casebook.org/images/police_sag.jpg');
INSERT INTO INSPECTOR VALUES (9, 'P.C.', NULL, 'William', NULL, 'Smith', 'http://www.casebook.org/images/police_william_smith.jpg');
INSERT INTO INSPECTOR VALUES (10, 'Major', NULL, 'Henry', NULL, 'Smith', 'http://www.casebook.org/images/police_smi.jpg');
INSERT INTO INSPECTOR VALUES (11, 'Detective', 'Constable', 'Robert', NULL, 'Sagar', 'http://www.casebook.org/images/police_sag.jpg');
INSERT INTO INSPECTOR VALUES (12, 'Inspector', NULL, 'Edmund', NULL, 'Reid', 'http://www.casebook.org/images/police_edmund_reid.jpg');
INSERT INTO INSPECTOR VALUES (13, 'P.C.', NULL, 'John', NULL, 'Neil', 'http://www.casebook.org/images/police_john_neil.jpg');
INSERT INTO INSPECTOR VALUES (14, 'Chief', 'Inspector', 'Henry', NULL, 'Moore', 'http://www.casebook.org/images/police_henry_moore.jpg');
INSERT INTO INSPECTOR VALUES (15, NULL, NULL, 'James', NULL, 'Monro', 'http://www.casebook.org/images/police_mon.jpg');
INSERT INTO INSPECTOR VALUES (16, 'Sir', NULL, 'Melville', NULL, 'Macnaghten', 'http://www.casebook.org/images/police_mac.jpg');
INSERT INTO INSPECTOR VALUES (17, 'Chief', 'Inspector', 'John', NULL, 'George', 'http://www.casebook.org/images/police_lc.jpg');
INSERT INTO INSPECTOR VALUES (18, 'Inspector', NULL, 'Joseph', 'Henry', 'Helson', 'http://www.casebook.org/images/police_helson.jpg');
INSERT INTO INSPECTOR VALUES (19, 'P.C.', NULL, 'James', NULL, 'Harvey', 'http://www.casebook.org/images/police_sag.jpg');
INSERT INTO INSPECTOR VALUES (20, 'Detective', 'Sergeant', 'George', NULL, 'Godley', 'http://www.casebook.org/images/police_god.jpg');
INSERT INTO INSPECTOR VALUES (21, 'Detective', 'Constable', 'Walter', NULL, 'Dew', 'http://www.casebook.org/images/police_dew.jpg');
INSERT INTO INSPECTOR VALUES (22, 'Superintendent', NULL, 'Thomas', NULL, 'Arnold', 'http://www.casebook.org/images/police_thomas_arnold.jpg');
INSERT INTO INSPECTOR VALUES (23, 'Inspector', NULL, 'Walter', NULL, 'Andrews', 'http://www.casebook.org/images/police_walter_andrews.jpg');
INSERT INTO INSPECTOR VALUES (24, 'Sir', NULL, 'Robert', NULL, 'Anderson', 'http://www.casebook.org/images/police_and.jpg');
INSERT INTO INSPECTOR VALUES (25, 'Inspector', NULL, 'Frederick', NULL, 'Abberline', 'http://www.casebook.org/images/police_abb1.jpg');
INSERT INTO INSPECTOR VALUES (26, 'P.C.', NULL, NULL, NULL, 'Neil', 'http://www.casebook.org/images/neil.jpg');


INSERT INTO DOCTOR VALUES (1, 'Dr.', NULL, NULL, 'Llewellyn', NULL);
INSERT INTO DOCTOR VALUES (2, 'Dr.', 'George', 'Baxter', 'Phillips', NULL);
INSERT INTO DOCTOR VALUES (3, 'Dr.', 'Frederick', 'Gordon', 'Brown', NULL);
INSERT INTO DOCTOR VALUES (4, 'Dr.', 'Thomas', NULL, 'Bond', NULL);
INSERT INTO DOCTOR VALUES (5, 'Dr.', 'Timothy', 'Robert', 'Killeen', NULL);
INSERT INTO DOCTOR VALUES (6, 'Dr.', 'Matthew', NULL, 'Brownfield', NULL);
INSERT INTO DOCTOR VALUES (7, NULL, 'Cornoer', 'Wynne E.', 'Baxter', NULL);
INSERT INTO DOCTOR VALUES (8, 'Dr.', NULL, NULL, 'Jenkins', NULL);


INSERT INTO SUSPECT VALUES (1, 'Dr. John Williams', 'http://www.casebook.org/images/dr-john-williams.jpg', 'there is very little to suggest that he was Jack the Ripper', NULL);
INSERT INTO SUSPECT VALUES (2, 'Nikolay Vasiliev', 'http://www.casebook.org/images/suspect_lodge.jpg', 'an elusive legend, which probably had some basis in reality, but was mostly embellished by the journalists who wrote it up', NULL);
INSERT INTO SUSPECT VALUES (3, 'Frances Tumblety', 'http://www.casebook.org/images/suspect_tumb.jpg', 'There is a strong case to be made that he was indeed the Batty Street Lodger', NULL);
INSERT INTO SUSPECT VALUES (4, 'Francis Thompson', 'http://www.casebook.org/images/suspect_thompson.jpg', 'At 29, Thompson was the right age to fit the Ripper descriptions, and we know he had some medical training. He was also said to carry a dissecting scalpel around with him, which he claimed he used to shave', NULL);
INSERT INTO SUSPECT VALUES (5, 'Alois Szemeredy', 'http://www.casebook.org/images/suspect_szemeredy.jpg', 'from Buenos Aires, suspected of the Jack the Ripper- and other murders', NULL);
INSERT INTO SUSPECT VALUES (6, 'R. D´Onston Stephenson', 'http://www.casebook.org/images/suspect_dons.jpg', 'Known to have had an extraordinary interest in the murders. Wrote numerous articles and letters on the matter. Resided in the East End', NULL);
INSERT INTO SUSPECT VALUES (7, 'James Kenneth Stephen', 'http://www.casebook.org/images/suspect_jkstep.jpg', 'Known misogynist and lunatic but no connections with the East End', NULL);
INSERT INTO SUSPECT VALUES (8, 'Walter Sickert', 'http://www.casebook.org/images/suspect_sickert.jpg', 'a valid suspect since the 1990s', NULL);
INSERT INTO SUSPECT VALUES (9, 'The Royal Conspiracy', 'http://www.casebook.org/images/suspec\'_royal.jpg', 'a fascinating tapestry of conspiracy involving virtually every person who has ever been a Ripper suspect plus a few new ones', NULL);
INSERT INTO SUSPECT VALUES (10, 'Dr. Alexander Pedachenko', 'http://www.casebook.org/images/suspect_lodge.jpg', 'may never have existed', NULL);
INSERT INTO SUSPECT VALUES (11, 'Michael Ostrog', 'http://www.casebook.org/images/suspect_ost.jpg', 'Mentioned for the first time as a suspect in 1894,in 1994 a lot of information was published making him a prime suspect', NULL);
INSERT INTO SUSPECT VALUES (12, 'James Maybrick', 'http://www.casebook.org/images/suspect_may.jpg', 'The mysterious emergence of the so-called Maybrick journal in 1992 however, immediately thrust him to the forefront of credible Ripper suspects', NULL);
INSERT INTO SUSPECT VALUES (13, 'The Lodger (Frances Tumblety)', 'http://www.casebook.org/images/suspect_lodge.jpg', 'a very strong suspect', NULL);
INSERT INTO SUSPECT VALUES (14, 'Jacob Levy', 'http://www.casebook.org/images/suspect_lodge.jpg', 'a butcher, and the cuts inflicted upon Catharine Eddowes were suggestive of a butcher', 4);
INSERT INTO SUSPECT VALUES (15, 'Aaron Kosminski', 'http://www.casebook.org/images/suspect_kosm.jpg', 'According to Anderson and Swanson, identified by a witness as the Ripper, but no charges were brought against him due to the witness\'s reluctance to testify against \"a fellow Jew.\" Known to have been insane.', NULL);
INSERT INTO SUSPECT VALUES (16, 'Severin Klosowski (George Chapman)', 'http://www.casebook.org/images/suspect_klos.jpg', 'there are some reasons in favour of and some against suspecting him', 18);
INSERT INTO SUSPECT VALUES (17, 'James Kelly', 'http://www.casebook.org/images/suspect_jkell.jpg', 'there are some reasons in favour of and some against suspecting him', NULL);
INSERT INTO SUSPECT VALUES (18, 'Mrs. Mary Pearcey', 'http://www.casebook.org/images/suspect_jill.jpg', 'Not very likely "Jill the Ripper"-theory, first published in 1939', 5);
INSERT INTO SUSPECT VALUES (19, 'George Hutchinson (Br.)', 'http://www.casebook.org/images/suspect_lodge.jpg', 'Police at the time interviewed him but did not seem to consider him a suspect', NULL);
INSERT INTO SUSPECT VALUES (20, 'Fogelma', 'http://www.casebook.org/images/suspect_lodge.jpg', 'no outside evidence to corroborate the story told by the Empire News', NULL);
INSERT INTO SUSPECT VALUES (21, 'Montague John Druitt', 'http://www.casebook.org/images/suspect_druitt.jpg', 'He is considered by many to be the number one suspect in the case, yet there is little evidence', NULL);
INSERT INTO SUSPECT VALUES (22, 'Frederick Bailey Deeming', 'http://www.casebook.org/images/suspect_deem.jpg', 'The only two links he may have had with the Whitechapel murders were (1) his insanity and (2) his method of killing his family', NULL);
INSERT INTO SUSPECT VALUES (23, 'Dr. Thomas Neill Cream', 'http://www.casebook.org/suspects/cream.html', 'did commit murders, but by poisoning', NULL);
INSERT INTO SUSPECT VALUES (24, 'David Cohen', 'http://www.casebook.org/images/suspect_lodge.jpg', 'a poor Polish Jew, living in Whitechapel and who had "homocidal tendensies and a great hatred of women", and was confined to a lunatic asylum at the right time for the murders to stop and died shortly afterwards', NULL);
INSERT INTO SUSPECT VALUES (25, 'Lewis Carroll', 'http://www.casebook.org/images/suspect_carroll.jpg', 'a very unlikely suspect', NULL);
INSERT INTO SUSPECT VALUES (26, 'William Henry Bury', 'http://www.casebook.org/images/whbury.jpg', 'Police at the time investigated the matter but did not seem to consider Bury a viable suspect', NULL);
INSERT INTO SUSPECT VALUES (27, 'Alfred Napier Blanchard', 'http://www.casebook.org/images/suspect_lodge.jpg', 'made a false confession', NULL);
INSERT INTO SUSPECT VALUES (28, 'Joseph Barnett', 'http://www.casebook.org/images/barnett2.jpg', 'was not described as a Ripper suspect until the 1970s', 5);
INSERT INTO SUSPECT VALUES (29, 'Prince Albert Victor', 'http://www.casebook.org/images/ed1890b.jpg', 'one of the most famous suspects', NULL);


INSERT INTO SCENE VALUES (1, 1, NULL);
INSERT INTO SCENE VALUES (2, 2, NULL);
INSERT INTO SCENE VALUES (3, 3, NULL);
INSERT INTO SCENE VALUES (4, 4, NULL);
INSERT INTO SCENE VALUES (5, 5, NULL);
INSERT INTO SCENE VALUES (6, 6, NULL);
INSERT INTO SCENE VALUES (7, 7, NULL);
INSERT INTO SCENE VALUES (8, 8, NULL);
INSERT INTO SCENE VALUES (9, 9, NULL);
INSERT INTO SCENE VALUES (10, 10, NULL);
INSERT INTO SCENE VALUES (11, 11, NULL);
INSERT INTO SCENE VALUES (12, 12, NULL);
INSERT INTO SCENE VALUES (13, 13, NULL);
INSERT INTO SCENE VALUES (14, 14, NULL);
INSERT INTO SCENE VALUES (15, 15, NULL);
INSERT INTO SCENE VALUES (16, 16, NULL);
INSERT INTO SCENE VALUES (17, 17, NULL);
INSERT INTO SCENE VALUES (18, 18, 16);


INSERT INTO TIMELINE VALUES (1, '23:00:00', 'Polly is seen walking down Whitechapel Road, she is probably soliciting trade.',1);
INSERT INTO TIMELINE VALUES (2, '00:30:00', 'She is seen leaving the Frying Pan Public House at the corner of Brick Lane and Thrawl Street. She returns to the lodging house at 18 Thrawl Street.',1);
INSERT INTO TIMELINE VALUES (3, '01:30:00', 'She is told by the deputy to leave the kitchen of the lodging house because she could not produce her doss money. Polly, on leaving, asks him to save a bed for her. "Never Mind!" She says, "I\'ll soon get my doss money. See what a jolly bonnet I\'ve got now." She indicates a little black bonnet which no one had seen before.',1);
INSERT INTO TIMELINE VALUES (4, '02:30:00', 'She meets Emily Holland, who was returning from watching the Shadwell Dry Dock fire, outside of a grocer\'s shop on the corner of Whitechapel Road and Osborn Street. Polly had come down Osborn Street. Holland describes her as "very drunk and staggered against the wall." Holland calls attention to the church clock striking 2:30. Polly tells Emily that she had had her doss money three times that day and had drunk it away. She says she will return to Flower and Dean Street where she could share a bed with a man after one more attempt to find trade. "I\'ve had my doss money three times today and spent it." She says, "It won\'t be long before I\'m back." The two women talk for seven or eight minutes. Polly leaves walking east down Whitechapel Road. At the time, the services of a destitute prostitute like Polly Nichols could be had for 2 or 3 pence or a stale loaf of bread. 3 pence was the going rate as that was the price of a large glass of gin.',1);
INSERT INTO TIMELINE VALUES (5, '03:15:00', 'P.C. John Thain, 96J, passes down Buck\'s Row on his beat. He sees nothing unusual. At approximately the same time Sgt. Kerby passes down Bucks Row and reports the same.',1);
INSERT INTO TIMELINE VALUES (6, '03:45:00', 'Polly Nichols\' body is discovered in Buck\'s Row by Charles Cross, a carman, on his way to work at Pickfords in the City Road., and Robert Paul who joins him at his request. "Come and look over here, there\'s a woman." Cross calls to Paul. Cross believes she is dead. Her hands and face are cold but the arms above the elbow and legs are still warm. Paul believes he feels a faint heartbeat. "I think she\'s breathing," he says "but it is little if she is."',1);


INSERT INTO SCENE_WITNESS VALUES (1,1);
INSERT INTO SCENE_WITNESS VALUES (2,2);
INSERT INTO SCENE_WITNESS VALUES (2,3);
INSERT INTO SCENE_WITNESS VALUES (3,4);
INSERT INTO SCENE_WITNESS VALUES (3,5);
INSERT INTO SCENE_WITNESS VALUES (3,6);
INSERT INTO SCENE_WITNESS VALUES (3,7);
INSERT INTO SCENE_WITNESS VALUES (3,8);
INSERT INTO SCENE_WITNESS VALUES (3,18);
INSERT INTO SCENE_WITNESS VALUES (4,9);
INSERT INTO SCENE_WITNESS VALUES (4,10);
INSERT INTO SCENE_WITNESS VALUES (5,11);
INSERT INTO SCENE_WITNESS VALUES (5,12);
INSERT INTO SCENE_WITNESS VALUES (8,13);
INSERT INTO SCENE_WITNESS VALUES (8,14);
INSERT INTO SCENE_WITNESS VALUES (17,15);
INSERT INTO SCENE_WITNESS VALUES (17,16);
INSERT INTO SCENE_WITNESS VALUES (18,17);


INSERT INTO SCENE_DOCTOR VALUES (1,1);
INSERT INTO SCENE_DOCTOR VALUES (2,2);
INSERT INTO SCENE_DOCTOR VALUES (3,2);
INSERT INTO SCENE_DOCTOR VALUES (4,3);
INSERT INTO SCENE_DOCTOR VALUES (5,2);
INSERT INTO SCENE_DOCTOR VALUES (5,4);
INSERT INTO SCENE_DOCTOR VALUES (10,5);
INSERT INTO SCENE_DOCTOR VALUES (13,6);
INSERT INTO SCENE_DOCTOR VALUES (13,2);
INSERT INTO SCENE_DOCTOR VALUES (15,2);
INSERT INTO SCENE_DOCTOR VALUES (17,7);
INSERT INTO SCENE_DOCTOR VALUES (18,8);


INSERT INTO SCENE_INSPECTOR VALUES (1,26);
INSERT INTO SCENE_INSPECTOR VALUES (6,12);
INSERT INTO SCENE_INSPECTOR VALUES (10,12);
INSERT INTO SCENE_INSPECTOR VALUES (15,12);
INSERT INTO SCENE_INSPECTOR VALUES (15,24);
INSERT INTO SCENE_INSPECTOR VALUES (15,15);
INSERT INTO SCENE_INSPECTOR VALUES (16,15);
INSERT INTO SCENE_INSPECTOR VALUES (16,16);
INSERT INTO SCENE_INSPECTOR VALUES (16,7);
INSERT INTO SCENE_INSPECTOR VALUES (17,16);


COMMIT;


--Which of the victims survived?

select first_name, surname, date from victim where date LIKE '%survived%';

--How many witnesses did Elisabeth Stride have?
select count(*) from scene_witness where scene_id IN ( select scene_id from scene where victim_id IN ( select victim_id from victim where surname='Stride'));

--In how many cases did inspector Monro get involved?
select count(*) from scene_inspector where inspector_id IN (Select inspector_id from inspector where surname='Monro') Group by inspector_id;

--How many girls with blue eyes did he kill?
select count(*) from victim where eyes='blue';

--Which where the victims and where can we find their photographs
select first_name, surname, picture from victim order by surname;

--What places did Jack the Ripper murder his victims?
select surname, location from victim order by surname;

--Who is the doctor that involved in the murder scene of Elisabeth Stride?
select title, first_name, other_names, surname from doctor d, scene_doctor s where d.doctor_id = s.doctor_id and s.scene_id IN( SELECT scene_id FROM scene s, victim v where s.victim_id = v.victim_id and v.surname='Stride');

--How many victims are there in total?
SELECT count(*) FROM victim;

DROP TABLE SCENE_WITNESS;
DROP TABLE SCENE_DOCTOR;
DROP TABLE SCENE_INSPECTOR;
DROP TABLE INSPECTOR;
DROP TABLE SCENE;
DROP TABLE SUSPECT;
DROP TABLE TIMELINE;
DROP TABLE VICTIM;
DROP TABLE WITNESS;
DROP TABLE DOCTOR;
