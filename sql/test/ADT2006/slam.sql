CREATE TABLE Jtr_scene (
	scene_id int PRIMARY KEY NOT NULL, 
	victim varchar(50) NOT NULL
);

CREATE TABLE Jtr_event (
	event_id int PRIMARY KEY NOT NULL, 
	scene_id int NOT NULL, 
	time varchar(50) NOT NULL, 
	"text" text NOT NULL
);

CREATE TABLE Jtr_doctor (
	name varchar(50) PRIMARY KEY NOT NULL, 
	picture varchar(150) NOT NULL
);

CREATE TABLE Jtr_inspector (
	name varchar(50) PRIMARY KEY NOT NULL, 
	picture varchar(150) NOT NULL
);

CREATE TABLE Jtr_suspect (
	name varchar(50) PRIMARY KEY NOT NULL, 
	picture varchar(150) NOT NULL,
	notes text NOT NULL
);

CREATE TABLE Jtr_victim (
	name varchar(50) PRIMARY KEY NOT NULL, 
	picture varchar(150) NOT NULL,
	dob varchar(50) NOT NULL,
	length varchar(50) NOT NULL,
	eyes varchar(50) NOT NULL,
	hair varchar(50) NOT NULL,
	date varchar(50) NOT NULL,
	location varchar(150) NOT NULL,
	features text NOT NULL
);

CREATE TABLE Jtr_witness (
	name varchar(50) PRIMARY KEY NOT NULL, 
	time varchar(50) NOT NULL,
	appearence text NOT NULL,
	diction text NOT NULL
);

CREATE TABLE Jtr_scene_doctors (
	scene_id int NOT NULL, 
	doctor varchar(50) NOT NULL
);

CREATE TABLE Jtr_scene_inspectors (
	scene_id int NOT NULL, 
	inspector varchar(50) NOT NULL
);

CREATE TABLE Jtr_scene_suspects (
	scene_id int NOT NULL, 
	suspect varchar(50) NOT NULL
);

CREATE TABLE Jtr_scene_witnesses (
	scene_id int NOT NULL, 
	witness varchar(50) NOT NULL
);

INSERT INTO Jtr_scene (scene_id, victim) VALUES (0, 'Mary Ann Walker'); 
INSERT INTO Jtr_event (event_id, scene_id, time, "text") VALUES (0, 0, '11:00 PM', 'Polly is seen walking down Whitechapel Road, she is
					probably soliciting trade.'); 
INSERT INTO Jtr_event (event_id, scene_id, time, "text") VALUES (1, 0, '12:30 AM', 'She is seen leaving the Frying Pan Public House at
					the corner of Brick Lane and Thrawl Street. She
					returns to the lodging house at 18 Thrawl Street.'); 
INSERT INTO Jtr_event (event_id, scene_id, time, "text") VALUES (2, 0, '1:20 or 1:40 AM', 'She is told by the deputy to leave the kitchen of
					the lodging house because she could not produce her
					doss money. Polly, on leaving, asks him to save a
					bed for her. "Never Mind!" She says, "I''ll soon get
					my doss money. See what a jolly bonnet I''ve got
					now." She indicates a little black bonnet which no
					one had seen before.'); 
INSERT INTO Jtr_event (event_id, scene_id, time, "text") VALUES (3, 0, '2:30 AM', 'She meets Emily Holland, who was returning from
					watching the Shadwell Dry Dock fire, outside of a
					grocer''s shop on the corner of Whitechapel Road and
					Osborn Street. Polly had come down Osborn Street.
					Holland describes her as "very drunk and staggered
					against the wall." Holland calls attention to the
					church clock striking 2:30. Polly tells Emily that
					she had had her doss money three times that day and
					had drunk it away. She says she will return to
					Flower and Dean Street where she could share a bed
					with a man after one more attempt to find trade.
					"I''ve had my doss money three times today and spent
					it." She says, "It won''t be long before I''m back."
					The two women talk for seven or eight minutes. Polly
					leaves walking east down Whitechapel Road.
					At the time, the services of a destitute prostitute
					like Polly Nichols could be had for 2 or 3 pence or
					a stale loaf of bread. 3 pence was the going rate as
					that was the price of a large glass of gin.'); 
INSERT INTO Jtr_event (event_id, scene_id, time, "text") VALUES (4, 0, '3:15 AM', 'P.C. John Thain, 96J, passes down Buck''s Row on his
					beat. He sees nothing unusual. At approximately the
					same time Sgt. Kerby passes down Bucks Row and
					reports the same.'); 
INSERT INTO Jtr_event (event_id, scene_id, time, "text") VALUES (5, 0, '3:40 or 3:45 AM', 'Polly Nichols'' body is discovered in Buck''s Row by
					Charles Cross, a carman, on his way to work at
					Pickfords in the City Road., and Robert Paul who
					joins him at his request. "Come and look over here,
					there''s a woman." Cross calls to Paul. Cross
					believes she is dead. Her hands and face are cold
					but the arms above the elbow and legs are still
					warm. Paul believes he feels a faint heartbeat. "I
					think she''s breathing," he says "but it is little if
					she is."'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (0, 'Dr. Llewellyn'); 
INSERT INTO Jtr_scene_inspectors (scene_id, inspector) VALUES (0, 'P.C. Neil'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (0, 'Patrick Mulshaw'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (1, 'Annie Chapman'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (1, 'Dr. George Baxter Phillips'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (1, 'Emily Walter'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (1, 'Elizabeth Long'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (2, 'Elisabeth Stride'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (2, 'Dr. George Baxter Phillips'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (2, 'J. Best and John Gardner'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (2, 'William Marshall'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (2, 'Matthew Packer'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (2, 'P.C. William Smith'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (2, 'James Brown'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (2, 'Israel Schwartz'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (3, 'Catherine Eddowes'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (3, 'Dr. Frederick Gordon Brown'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (3, 'Joseph Lawende'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (3, 'James Blenkinsop'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (4, 'Mary Jane Kelly'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (4, 'Dr. George Baxter Phillips'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (4, 'Dr. Thomas Bond'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (4, 'Mary Ann Cox'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (4, 'George Hutchinson'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (5, '"Fairy Fay"'); 
INSERT INTO Jtr_scene_inspectors (scene_id, inspector) VALUES (5, 'Inspector Edmund Reid'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (6, 'Annie Millwood'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (6, 'no witnesses'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (7, 'Ada Wilson'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (7, 'Ada Wilson herself'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (7, 'Rose Bierman'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (8, 'Emma Smith'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (9, 'Martha Tabram'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (9, 'Dr. Timothy Robert Killeen'); 
INSERT INTO Jtr_scene_inspectors (scene_id, inspector) VALUES (9, 'Inspector Edmund Reid'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (10, 'Whitehall Mystery'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (11, 'Annie Farmer'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (12, 'Rose Mylett'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (12, 'Dr. Matthew Brownfield'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (12, 'Dr. George Baxter Phillips'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (13, 'Elisabeth Jackson'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (14, 'Alice MacKenzie'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (14, 'Dr. George Baxter Phillips'); 
INSERT INTO Jtr_scene_inspectors (scene_id, inspector) VALUES (14, 'Inspector Edmund Reid'); 
INSERT INTO Jtr_scene_inspectors (scene_id, inspector) VALUES (14, 'Sir Robert Anderson'); 
INSERT INTO Jtr_scene_inspectors (scene_id, inspector) VALUES (14, 'James Monro'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (15, 'Pinchin Street Murder, possibly Lydia Hart'); 
INSERT INTO Jtr_scene_inspectors (scene_id, inspector) VALUES (15, 'James Monro'); 
INSERT INTO Jtr_scene_inspectors (scene_id, inspector) VALUES (15, 'Sir Melville Macnaghten'); 
INSERT INTO Jtr_scene_inspectors (scene_id, inspector) VALUES (15, 'Chief Inspector Donald Swanson'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (16, 'Frances Coles'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (16, 'Cornoer Wynne E. Baxter'); 
INSERT INTO Jtr_scene_inspectors (scene_id, inspector) VALUES (16, 'Sir Melville Macnaghten'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (16, '"Jumbo" Friday'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (16, 'Duncan Campnell'); 
INSERT INTO Jtr_scene (scene_id, victim) VALUES (17, 'Carrie Brown'); 
INSERT INTO Jtr_scene_doctors (scene_id, doctor) VALUES (17, 'Dr. Jenkins'); 
INSERT INTO Jtr_scene_suspects (scene_id, suspect) VALUES (17, 'Severin Klosowski (George Chapman)'); 
INSERT INTO Jtr_scene_witnesses (scene_id, witness) VALUES (17, 'Mary Miniter'); 
INSERT INTO Jtr_doctor (name, picture) VALUES ('Dr. Llewellyn', ''); 
INSERT INTO Jtr_doctor (name, picture) VALUES ('Dr. George Baxter Phillips', ''); 
INSERT INTO Jtr_doctor (name, picture) VALUES ('Dr. Frederick Gordon Brown', ''); 
INSERT INTO Jtr_doctor (name, picture) VALUES ('Dr. Thomas Bond', ''); 
INSERT INTO Jtr_doctor (name, picture) VALUES ('Dr. Timothy Robert Killeen', ''); 
INSERT INTO Jtr_doctor (name, picture) VALUES ('Dr. Matthew Brownfield', ''); 
INSERT INTO Jtr_doctor (name, picture) VALUES ('Cornoer Wynne E. Baxter', ''); 
INSERT INTO Jtr_doctor (name, picture) VALUES ('Dr. Jenkins', ''); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Inspector Frederick Abberline', 'http://www.casebook.org/images/police_abb1.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Sir Robert Anderson', 'http://www.casebook.org/images/police_and.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Inspector Walter Andrews', 'http://www.casebook.org/images/police_walter_andrews.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Superintendent Thomas Arnold', 'http://www.casebook.org/images/police_thomas_arnold.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Detective Constable Walter Dew', 'http://www.casebook.org/images/police_dew.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Detective Sergeant George Godley', 'http://www.casebook.org/images/police_god.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('P.C. James Harvey', 'http://www.casebook.org/images/police_sag.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Inspector Joseph Henry Helson', 'http://www.casebook.org/images/police_helson.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Chief Inspector John George', 'http://www.casebook.org/images/police_lc.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Sir Melville Macnaghten', 'http://www.casebook.org/images/police_mac.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('James Monro', 'http://www.casebook.org/images/police_mon.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Chief Inspector Henry Moore', 'http://www.casebook.org/images/police_henry_moore.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('P.C. John Neil', 'http://www.casebook.org/images/police_john_neil.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Inspector Edmund Reid', 'http://www.casebook.org/images/police_edmund_reid.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Detective Constable Robert Sagar', 'http://www.casebook.org/images/police_sag.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Major Henry Smith', 'http://www.casebook.org/images/police_smi.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('P.C. William Smith', 'http://www.casebook.org/images/police_william_smith.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Inspector John Spratling', 'http://www.casebook.org/images/police_sag.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Chief Inspector Donald Swanson', 'http://www.casebook.org/images/police_sut.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Sergeant William Thick', 'http://www.casebook.org/images/police_thi.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('P.C. Ernest Thompson', 'http://www.casebook.org/images/police_ernest_thompson.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Sir Charles Warren', 'http://www.casebook.org/images/police_war.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('P.C. Edward Watkins', 'http://www.casebook.org/images/police_edward_watkins.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Sergeant Stephen White', 'http://www.casebook.org/images/police_steven_white.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('Chief Constable Adolphus Frederick Williamson', 'http://www.casebook.org/images/police_frederick_williamson.jpg'); 
INSERT INTO Jtr_inspector (name, picture) VALUES ('P.C. Neil', 'http://www.casebook.org/images/neil.jpg'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Prince Albert Victor', 'http://www.casebook.org/images/ed1890b.jpg', 'one of the most famous suspects'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Joseph Barnett', 'http://www.casebook.org/images/barnett2.jpg', 'was not described as a Ripper suspect until the 1970s'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Alfred Napier Blanchard', 'http://www.casebook.org/images/suspect_lodge.jpg', 'made a false confession'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('William Henry Bury', 'http://www.casebook.org/images/whbury.jpg', 'Police at the time investigated the matter but did not seem to consider Bury a viable suspect'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Lewis Carroll', 'http://www.casebook.org/images/suspect_carroll.jpg', 'a very unlikely suspect'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('David Cohen', 'http://www.casebook.org/images/suspect_lodge.jpg', 'a poor Polish Jew, living in Whitechapel and who had "homocidal tendensies and a great hatred of women", and was confined to a lunatic asylum at the right time for the murders to stop and died shortly afterwards'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Dr. Thomas Neill Cream', 'http://www.casebook.org/suspects/cream.html', 'did commit murders, but by poisoning'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Frederick Bailey Deeming', 'http://www.casebook.org/images/suspect_deem.jpg', 'The only two links he may have had with the Whitechapel murders were (1) his insanity and (2) his method of killing his family'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Montague John Druitt', 'http://www.casebook.org/images/suspect_druitt.jpg', 'He is considered by many to be the number one suspect in the case, yet there is little evidence'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Fogelma', 'http://www.casebook.org/images/suspect_lodge.jpg', 'no outside evidence to corroborate the story told by the Empire News'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('George Hutchinson (Br.)', 'http://www.casebook.org/images/suspect_lodge.jpg', 'Police at the time interviewed him but did not seem to consider him a suspect'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Mrs. Mary Pearcey', 'http://www.casebook.org/images/suspect_jill.jpg', 'Not very likely "Jill the Ripper"-theory, first published in 1939'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('James Kelly', 'http://www.casebook.org/images/suspect_jkell.jpg', 'there are some reasons in favour of and some against suspecting him'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Severin Klosowski (George Chapman)', 'http://www.casebook.org/images/suspect_klos.jpg', 'there are some reasons in favour of and some against suspecting him'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Aaron Kosminski', 'http://www.casebook.org/images/suspect_kosm.jpg', 'According to Anderson and Swanson, identified by a witness as the Ripper, but no charges were brought against him due to the witness''s reluctance to testify against "a fellow Jew." Known to have been insane.'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Jacob Levy', 'http://www.casebook.org/images/suspect_lodge.jpg', 'a butcher, and the cuts inflicted upon Catharine Eddowes were suggestive of a butcher'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('The Lodger (Frances Tumblety)', 'http://www.casebook.org/images/suspect_lodge.jpg', 'a very strong suspect'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('James Maybrick', 'http://www.casebook.org/images/suspect_may.jpg', 'The mysterious emergence of the so-called Maybrick journal in 1992 however, immediately thrust him to the forefront of credible Ripper suspects.'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Michael Ostrog', 'http://www.casebook.org/images/suspect_ost.jpg', 'Mentioned for the first time as a suspect in 1894,in 1994 a lot of information was published making him a prime suspect'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Dr. Alexander Pedachenko', 'http://www.casebook.org/images/suspect_lodge.jpg', 'may never have existed'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('The Royal Conspiracy', 'http://www.casebook.org/images/suspect_royal.jpg', 'a fascinating tapestry of conspiracy involving virtually every person who has ever been a Ripper suspect plus a few new ones'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Walter Sickert', 'http://www.casebook.org/images/suspect_sickert.jpg', 'a valid suspect since the 1990s'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('James Kenneth Stephen', 'http://www.casebook.org/images/suspect_jkstep.jpg', 'Known misogynist and lunatic but no connections with the East End'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('R. D´Onston Stephenson', 'http://www.casebook.org/images/suspect_dons.jpg', 'Known to have had an extraordinary interest in the murders. Wrote numerous articles and letters on the matter. Resided in the East End.'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Alois Szemeredy', 'http://www.casebook.org/images/suspect_szemeredy.jpg', 'from Buenos Aires, suspected of the Jack the Ripper- and other murders'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Francis Thompson', 'http://www.casebook.org/images/suspect_thompson.jpg', 'At 29, Thompson was the right age to fit the Ripper descriptions, and we know he had some medical training. He was also said to carry a dissecting scalpel around with him, which he claimed he used to shave'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Frances Tumblety', 'http://www.casebook.org/images/suspect_tumb.jpg', 'There is a strong case to be made that he was indeed the Batty Street Lodger'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Nikolay Vasiliev', 'http://www.casebook.org/images/suspect_lodge.jpg', 'an elusive legend, which probably had some basis in reality, but was mostly embellished by the journalists who wrote it up'); 
INSERT INTO Jtr_suspect (name, picture, notes) VALUES ('Dr. John Williams', 'http://www.casebook.org/images/dr-john-williams.jpg', 'there is very little to suggest that he was Jack the Ripper'); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Mary Ann Walker', 'http://www.casebook.org/images/victims_nichols.jpg', '1845-08-26', '5''12"', 'brown', 'brown hair turning grey', '1888-08-31', 'Buck''s Row by Charles Cross', 'five front teeth missing (Rumbelow); two bottom-one top
				front (Fido), her teeth are slightly discoloured. She is
				described as having small, delicate features with high
				cheekbones and grey eyes. She has a small scar on her
				forehead from a childhood injury.  She is described by
				Emily Holland as "a very clean woman who always seemed
				to keep to herself." The doctor at the post mortem
				remarked on the cleanliness of her thighs.  She is also
				an alcoholic.'); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Annie Chapman', 'http://www.casebook.org/images/victims_chapman.jpg', '1841-09', '5''', 'blue', 'dark brown, wavy', '1888-09-08', '29 Hanbury Street', 'Pallid complexion, excellent teeth (possibly two missing in lower jaw), strongly built (stout), thick nose'); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Elisabeth Stride', 'http://www.casebook.org/images/victims_stride.jpg', '1843-11-27', '5''5"', 'light gray', 'curly dark brown', '1888-09-30', 'Berner Street (Henriques Street today)', 'pale complexion, all the teeth in her lower left jaw were missing'); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Catherine Eddowes', 'http://www.casebook.org/images/eddowes1.jpg', '1842-04-14', '5''', 'hazel', 'dark auburn', '1888-09-30', 'Mitre Square', 'She has a tattoo in blue ink on her left forearm "TC."'); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Mary Jane Kelly', 'http://www.casebook.org/images/victims_kelly.jpg', 'around 1863', '5''7"', 'blue', 'blonde', '1888-11-09', '13 Miller''s Court', 'a fair complexion. "Said to have been possessed of considerable personal attractions." (McNaughten) She was last seen wearing a linsey frock and a red shawl pulled around her shoulders. She was bare headed. Detective Constable Walter Dew claimed to know Kelly well by sight and says that she was attractive and paraded around, usually in the company of two or three friends. He says she always wore a spotlessly clean white apron.
				Maria Harvey, a friend, says that she was "much superior to that of most persons in her position in life."'); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('"Fairy Fay"', 'http://www.casebook.org/images/victims_fairy.jpg', 'unknown', 'unknown', 'unknown', 'unknown', '1887-12-26', 'the alleys of Commercial Road', 'not recorded'); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Annie Millwood', 'http://www.casebook.org/images/victims_millwood.jpg', '1850', '', '', '', '1888-02-15', 'White''s Row, Spitalfields', ''); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Ada Wilson', 'http://www.casebook.org/images/victims_wilson.jpg', '', '', '', '', '(survived the attack on 1888-28-03)', '19 Maidman Street', ''); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Emma Smith', 'http://www.casebook.org/images/victims_smith.jpg', '1843', '', '', '', '1888-03-03', 'just outside Taylor Brothers Mustard and Cocoa Mill which was on the north-east corner of the Wentworth/Old Montague Street crossroads', ''); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Martha Tabram', 'http://www.casebook.org/images/victims_tabram.jpg', '1849-05-10', '', '', '', '1888-08-07', 'George Yard, a narrow north-south alley connecting Wentworth Street and Whitechapel High Street', ''); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Whitehall Mystery', 'http://www.casebook.org/images/victims_whitehall.jpg', '', '', '', '', '1888-10-03', 'a vault soon to become a section of the cellar of New Scotland Yard', 'the headless and limbless torso of a woman'); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Annie Farmer', 'http://www.casebook.org/images/victims_farmer.jpg', '1848', '', '', '', '(survived the attack on 1888-11-20)', '', ''); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Rose Mylett', 'http://www.casebook.org/images/victims_mylett.jpg', '1862', '', '', '', '1888-12-20', 'the yard between 184 and 186 Poplar High Street, in Clarke''s Yard', ''); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Elisabeth Jackson', 'http://www.casebook.org/images/victims_jackson.jpg', '', '', '', '', '1889-06', 'the Thames', ''); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Alice MacKenzie', 'http://www.casebook.org/images/victims_mckenzie.jpg', '1849', '', '', '', '1889-07-17', 'Castle Alley', 'as a freckle-faced woman with a penchant for both smoke and drink. Her left thumb was also injured in what was no doubt some sort of industrial accident.'); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Pinchin Street Murder, possibly Lydia Hart', 'http://www.casebook.org/images/victims_pinchin.jpg', '', '', '', '', '1889-09-10', 'under a railway arch in Pinchin Street', 'body, missing both head and legs'); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Frances Coles', 'http://www.casebook.org/images/victims_coles.jpg', '1865', '', '', '', '1891-02-13', 'Swallow Gardens', 'is often heralded as the prettiest of all the murder victims'); 
INSERT INTO Jtr_victim (name, picture, dob, length, eyes, hair, date, location, features) VALUES ('Carrie Brown', 'http://www.casebook.org/images/victims_brown.jpg', '', '5''8"', '', '', '1891-04-24', 'the room of the East River Hotel on the Manhattan waterfront of New York, U.S.A.', ''); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('Patrick Mulshaw', '4:00AM', 'suspicious', 'Watchman, old man, I believe somebody is murdered down the street.'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('Emily Walter', '2:00 A.M.', 'Foreigner aged 37, dark beard and moustache. Wearing short dark jacket, dark vest and trousers, black scarf and black felt hat.', 'Asked witness to enter the backyard of 29 Hanbury Street.'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('Elizabeth Long', '5:30 A.M.', 'Dark complexion, brown deerstalker hat, possibly a dark overcoat. Aged over 40, somewhat taller than Chapman. A foreigner of "shabby genteel."', '"Will you?"'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('J. Best and John Gardner', '11 P.M.', '5''5" tall, English, black moustache, sandy eyelashes, weak, wearing a morning suit and a billycock hat.', '(none)'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('William Marshall', '11:45 P.M.', 'Small, black coat, dark trousers, middle aged, round cap with a small sailor-like peak. 5''6", stout, appearance of a clerk. No moustache, no gloves, with a cutaway coat.', '"You would say anything but your prayers." Spoken mildly, with an English accent, and in an educated manner.'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('Matthew Packer', '12:00 - 12:30 P.M.', 'Aged 25-30, 5''7", long black coat buttoned up, soft felt hawker hat, broad shoulders. Maybe a young clerk, frock coat, no gloves.', 'Quiet in speaking, with a rough voice'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('P.C. William Smith', '12:30 A.M.', 'Aged 28, cleanshaven and respectable appearance, 5''7", hard dark felt deerstalker hat, dark clothes. Carrying a newspaper parcel 18 x 7 inches.', '(none)'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('James Brown', '12:45 A.M.', '5''7", stout, long black diagonal coat which reached almost to his heels.', '(none)'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('Israel Schwartz', '12:45 A.M.', 'First man: Aged 30, 5''5", brown haired, fair complexion, small brown moustache, full face, broad shoulders, dark jacket and trousers, black cap with peak. Second man: Aged 35, 5''11", fresh complexion, light brown hair, dark overcoat, old black hard felt hat with a wide brim, clay pipe.', '"Lipski!"'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('Joseph Lawende', '1:30 A.M', 'Aged 30, 5''7", fair complexion, brown moustache, salt-and-pepper coat, red neckerchief, grey peaked cloth cap. Sailor-like.', '(none)'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('James Blenkinsop', '1:30 A.M.', 'Well-dressed.', '"Have you seen a man and a woman go through here?"'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('Mary Ann Cox', '11:45 P.M.', 'Short, stout man, shabbily dressed. Billycock hat, blotchy face, carroty moustache, holding quart can of beer', '(none)'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('George Hutchinson', '2:00 A.M.', 'Aged 34-35, 5''6", pale complexion, dark hair, slight moustached curled at each end, long dark coat, collar cuffs of astrakhan, dark jacket underneath. Light waistcoat, thick gold chain with a red stone seal, dark trousers an'' button boots, gaiters, white buttons. White shirt, black tie fastened with a horseshoe pin. Dark hat, turned down in middle. Red kerchief. Jewish and respectable in appearance.', '(none)'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('no witnesses', '', '', '.'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('Ada Wilson herself', '', 'a man of about 30 years of age, 5ft 6ins in height, with a sunburnt face and a fair moustache. He was wearing a dark coat, light trousers and a wideawake hat.', '.'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('Rose Bierman', '', 'a young fair man with a light coat on', ''); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('"Jumbo" Friday', '', '', '.'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('Duncan Campnell', '', '', '.'); 
INSERT INTO Jtr_witness (name, time, appearence, diction) VALUES ('Mary Miniter', 'between 10:30 and 11:00', 'About 32 years of age. Five feet, eight inches tall. Slim build. Long, sharp nose. Heavy moustache of light color. Foreign in appearance, possibly German. Dark-brown cutaway coat. Black trousers. Old black derby hat with dented crown.', '.'); 

SELECT s.victim, v.location, e.time, e."text" FROM Jtr_scene s INNER JOIN Jtr_victim v ON s.victim = v.name LEFT OUTER JOIN Jtr_event e ON e.scene_id = s.scene_id ORDER BY s.victim, v.location, e.time, e."text";


SELECT v.name, sp.name, sp.picture FROM Jtr_scene sc INNER JOIN Jtr_victim v ON v.name = sc.victim LEFT OUTER JOIN (Jtr_scene_suspects map INNER JOIN Jtr_suspect sp ON map.suspect = sp.name )ON sc.scene_id = map.scene_id ORDER BY v.name, sp.name, sp.picture;


SELECT i.name, count(si.scene_id) AS times FROM Jtr_inspector i LEFT OUTER JOIN Jtr_scene_inspectors si ON i.name = si.inspector GROUP BY i.name ORDER BY i.name, times desc;


SELECT i.name AS name_inspector, d.name AS name_doctor FROM Jtr_scene s INNER JOIN Jtr_scene_inspectors si ON si.scene_id = s.scene_id  INNER JOIN Jtr_scene_doctors sd ON sd.scene_id = s.scene_id INNER JOIN Jtr_inspector i ON si.inspector = i.name INNER JOIN Jtr_doctor d ON sd.doctor = d.name ORDER BY name_inspector, name_doctor;


SELECT name, eyes, hair, features from Jtr_victim WHERE features LIKE '%missing%' ORDER BY name, eyes, hair, features;


SELECT eyes, count(name) AS times FROM Jtr_victim WHERE NOT eyes = '' GROUP BY eyes ORDER BY eyes, times desc;


SELECT s.victim, COUNT(w.witness) AS witnesses, COUNT(i.inspector) AS inspectors, COUNT(d.doctor) AS doctors, COUNT(sp.suspect) AS suspects FROM Jtr_scene s LEFT OUTER JOIN Jtr_scene_witnesses w ON w.scene_id = s.scene_id LEFT OUTER JOIN Jtr_scene_inspectors i ON i.scene_id = s.scene_id LEFT OUTER JOIN Jtr_scene_doctors d ON d.scene_id = s.scene_id LEFT OUTER JOIN Jtr_scene_suspects sp ON sp.scene_id = s.scene_id GROUP BY s.victim ORDER BY s.victim asc, witnesses, inspectors, doctors, suspects;

DROP TABLE Jtr_scene;
DROP TABLE Jtr_event;
DROP TABLE Jtr_doctor;
DROP TABLE Jtr_inspector;
DROP TABLE Jtr_suspect;
DROP TABLE Jtr_victim;
DROP TABLE Jtr_witness;
DROP TABLE Jtr_scene_doctors;
DROP TABLE Jtr_scene_inspectors;
DROP TABLE Jtr_scene_suspects;
DROP TABLE Jtr_scene_witnesses;
