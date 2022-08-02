CREATE TABLE "timeline"(
	"name"				varchar(40) PRIMARY KEY NOT NULL,
	"event"				varchar(2000),
	"time"				varchar(20)
);

INSERT INTO "timeline" VALUES('Mary Ann Walker', 'Polly is seen walking down Whitechapel Road, she is probably soliciting trade.', '11:00 PM'); 
INSERT INTO "timeline" VALUES('', 'She is seen leaving the Frying Pan Public House at the corner of Brick Lane and Thrawl Street. She returns to the lodging house at 18 Thrawl Street.', '12:30 AM'); 
--INSERT INTO "timeline" VALUES('Mary Ann Walker', "'She is told by the deputy to leave the kitchen of the lodging house because she could not produce her doss money. Polly, on leaving, asks him to save a bed for her. ""Never Mind!"" She says, ""I'll soon get my doss money. See what a jolly bonnet I've got now."" She indicates a little black bonnet which no one had seen before.'", '1:20 or 1:40 AM'); 
--INSERT INTO "timeline" VALUES('Mary Ann Walker', "She meets Emily Holland, who was returning from watching the Shadwell Dry Dock fire, outside of a grocer's shop on the corner of Whitechapel Road and Osborn Street. Polly had come down Osborn Street. Holland describes her as ""very drunk and staggered against the wall."" Holland calls attention to the church clock striking 2:30. Polly tells Emily that she had had her doss money three times that day and had drunk it away. She says she will return to Flower and Dean Street where she could share a bed with a man after one more attempt to find trade. ""I've had my doss money three times today and spentit."" She says, ""It won't be long before I'm back."" The two women talk for seven or eight minutes. Polly leaves walking east down Whitechapel Road. At the time, the services of a destitute prostitute like Polly Nichols could be had for 2 or 3 pence or a stale loaf of bread. 3 pence was the going rate as that was the price of a large glass of gin.", '2:30 AM'); 
--INSERT INTO "timeline" VALUES('Mary Ann Walker', "P.C. John Thain, 96J, passes down Buck's Row on his beat. He sees nothing unusual. At approximately the same time Sgt. Kerby passes down Bucks Row and reports the same.", '3:15 AM'); 
--INSERT INTO "timeline" VALUES('Mary Ann Walker', "Polly Nichols' body is discovered in Buck's Row by Charles Cross, a carman, on his way to work at Pickfords in the City Road., and Robert Paul who joins him at his request. ""Come and look over here, there's a woman." "Cross calls to Paul. Cross believes she is dead. Her hands and face are cold but the arms above the elbow and legs are still warm. Paul believes he feels a faint heartbeat. ""I think shes breathing, he says but it is little if she is.", '3:40 or 3:45 AM'); 

CREATE TABLE "victim"(
	"name"				varchar(40) PRIMARY KEY NOT NULL,
	"date_of_birth" 	varchar(12),
	"length"        	varchar(10),
	"eyes"          	varchar(15),
	"hair"          	varchar(40),
	"date"				varchar(48),
	"location"			varchar(280),
	"picture"			varchar(180),
	"features"			varchar(999),	
	"timeline"			varchar(40) REFERENCES "timeline"(name) 
);

INSERT INTO "victim" VALUES ('Mary Ann Walker', '1845-08-26', '(5''12")', 'brown', 'brown hair turning grey', '1888-08-31', '"Buck''s Row by Charles Cross"', 'http://www.casebook.org/images/victims_nichols.jpg', 'five front teeth missing (Rumbelow); two bottom-one top front (Fido), her teeth are slightly discoloured. She is described as having small, delicate features with high cheekbones and grey eyes. She has a small scar on her forehead from a childhood injury.  She is described by Emily Holland as "a very clean woman who always seemed to keep to herself." The doctor at the post mortem remarked on the cleanliness of her thighs.  She is also an alcoholic.', 'Mary Ann Walker');
INSERT INTO "victim" VALUES ('Annie Chapman', '1841-09', '(5'')', 'blue', 'dark brown, wavy', '1888-09-08', '"29 Hanbury Street"', 'http://www.casebook.org/images/victims_chapman.jpg', 'Pallid complexion, excellent teeth (possibly two missing in lower jaw), strongly built (stout), thick nose', '');
INSERT INTO "victim" VALUES ('Elisabeth Stride', '1843-11-27', '(5''5)', 'light gray', 'curly dark brown', '1888-09-30', 'Berner Street (Henriques Street today)', 'http://www.casebook.org/images/victims_stride.jpg', 'pale complexion, all the teeth in her lower left jaw were missing', '');
INSERT INTO "victim" VALUES ('Catherine Eddowes', '1842-04-14', '(5)', 'hazel', 'dark auburn', '1888-09-30', 'Mitre Square', 'http://www.casebook.org/images/eddowes1.jpg', 'She has a tattoo in blue ink on her left forearm "TC."', '');
INSERT INTO "victim" VALUES ('Mary Jane Kelly', 'around 1863', '(5''7")', 'blue', 'blonde', '1888-11-09', '13 Miller''s Court', 'http://www.casebook.org/images/victims_kelly.jpg', 'a fair complexion. "Said to have been possessed of considerable personal attractions." (McNaughten) She was last seen wearing a linsey frock and a red shawl pulled around her shoulders. She was bare headed. Detective Constable Walter Dew claimed to know Kelly well by sight and says that she was attractive and paraded around, usually in the company of two or three friends. He says she always wore a spotlessly clean white apron. Maria Harvey, a friend, says that she was "much superior to that of most persons in her position in life."', '');
INSERT INTO "victim" VALUES ('"Fairy Fay"', 'unknown', 'unknown', 'unknown', 'unknown', '1887-12-26', '"the alleys of Commercial Road"', 'http://www.casebook.org/images/victims_fairy.jpg', 'not recorded', '');
INSERT INTO "victim" VALUES ('Ada Wilson', '', '', '', '', '1888-28-03', '"19 Maidman Street"', 'http://www.casebook.org/images/victims_wilson.jpg', '', '');
INSERT INTO "victim" VALUES ('Emma Smith', '1843', '', '', '', '1888-03-03', '"just outside Taylor Brothers Mustard and Cocoa Mill which was on the north-east corner of the Wentworth/Old Montague Street crossroads"', 'http://www.casebook.org/images/victims_smith.jpg', '', '');
INSERT INTO "victim" VALUES ('Martha Tabram', '1849-05-10', '', '', '', '1888-08-07', '"George Yard, a narrow north-south alley connecting Wentworth Street and Whitechapel High Street"', 'http://www.casebook.org/images/victims_tabram.jpg', '', '');
INSERT INTO "victim" VALUES ('Whitehall Mystery', '', '', '', '', '1888-10-03', '"a vault soon to become a section of the cellar of New Scotland Yard"', 'http://www.casebook.org/images/victims_whitehall.jpg', 'the headless and limbless torso of a woman', '');
INSERT INTO "victim" VALUES ('Annie Farmer', '1848', '', '', '', '1888-11-20', '', 'http://www.casebook.org/images/victims_farmer.jpg', '', '');
INSERT INTO "victim" VALUES ('Rose Mylett', '1862', '', '', '', '1888-12-20', '"the yard between 184 and 186 Poplar High Street, in Clarke''s Yard"', 'http://www.casebook.org/images/victims_mylett.jpg', '', '');
INSERT INTO "victim" VALUES ('Elisabeth Jackson', '1862', '', '', '', '1889-06', '"the Thames"', 'http://www.casebook.org/images/victims_jackson.jpg', '', '');
INSERT INTO "victim" VALUES ('Alice MacKenzie', '1849', '', '', '', '1889-07-17', '"Castle Alley"', 'http://www.casebook.org/images/victims_mckenzie.jpg', 'as a freckle-faced woman with a penchant for both smoke and drink. Her left thumb was also injured in what was no doubt some sort of industrial accident.', '');
INSERT INTO "victim" VALUES ('Lydia Hart', '1849', '', '', '', '1889-09-10', '"under a railway arch in Pinchin Street"', 'http://www.casebook.org/images/victims_pinchin.jpg', 'body, missing both head and legs', '');
INSERT INTO "victim" VALUES ('Frances Coles', '1865', '', '', '', '1891-02-13', '"Swallow Gardens"', 'http://www.casebook.org/images/victims_coles.jpg', 'is often heralded as the prettiest of all the murder victims', '');
INSERT INTO "victim" VALUES ('Carrie Brown', '', '(5''8")', '', '', '1891-04-24', '"the room of the East River Hotel on the Manhattan waterfront of New York, U.S.A."', 'http://www.casebook.org/images/victims_brown.jpg', '', '');
INSERT INTO "victim" VALUES ('', '', '', '', '', '', '', '', '', '');



CREATE TABLE "witness"(
	"name"				varchar(40) PRIMARY KEY NOT NULL,
	"time"				varchar(100),
	"appearence"		varchar(900),
	"diction"			varchar(700)
);

INSERT INTO "witness" VALUES('Patrick Mulshaw', '4:00AM', 'suspicious', 'Watchman, old man, I believe somebody is murdered down the street.');
INSERT INTO "witness" VALUES('Emily Walter', '2:00 A.M.', 'Foreigner aged 37, dark beard and moustache. Wearing short dark jacket, dark vest and trousers, black scarf and black felt hat.', 'Asked witness to enter the backyard of 29 Hanbury Street.');
INSERT INTO "witness" VALUES('Elizabeth Long', '5:30 A.M.', 'Dark complexion, brown deerstalker hat, possibly a dark overcoat. Aged over 40, somewhat taller than Chapman. A foreigner of "shabby genteel."', '"Will you?"');
INSERT INTO "witness" VALUES('J. Best and John Gardner', '11 P.M.', '5''5" tall, English, black moustache, sandy eyelashes, weak, wearing a morning suit and a billycock hat."', '');
INSERT INTO "witness" VALUES('William Marshall', '11:45 P.M.', 'Small, black coat, dark trousers, middle aged, round cap with a small sailor-like peak. 5''6", stout, appearance of a clerk. No moustache, no gloves, with a cutaway coat.', '"You would say anything but your prayers." Spoken mildly, with an English accent, and in an educated manner.');
INSERT INTO "witness" VALUES('Matthew Packer', '12:00 - 12:30 P.M.', 'Aged 25-30, (5''7"), long black coat buttoned up, soft felt hawker hat, broad shoulders. Maybe a young clerk, frock coat, no gloves.', 'Quiet in speaking, with a rough voice');
INSERT INTO "witness" VALUES('P.C. William Smith', '12:30 A.M.', 'Aged 28, cleanshaven and respectable appearance, 5''7", hard dark felt deerstalker hat, dark clothes. Carrying a newspaper parcel 18 x 7 inches.', '');
INSERT INTO "witness" VALUES('James Brown', '12:45 A.M.', '5''7", stout, long black diagonal coat which reached almost to his heels.', '');
INSERT INTO "witness" VALUES('Israel Schwartz', '12:45 A.M.', 'First man: Aged 30, 5''5", brown haired, fair complexion, small brown moustache, full face, broad shoulders, dark jacket and trousers, black cap with peak. Second man: Aged 35, 5''11", fresh complexion, light brown hair, dark overcoat, old black hard felt hat with a wide brim, clay pipe.', '"Lipski!"');
INSERT INTO "witness" VALUES('Joseph Lawende', '1:30 A.M', 'Aged 30, 5''7", fair complexion, brown moustache, salt-and-pepper coat, red neckerchief, grey peaked cloth cap. Sailor-like.', '');
INSERT INTO "witness" VALUES('James Blenkinsop', '1:30 A.M.', 'Well-dressed.', '"Have you seen a man and a woman go through here?"');
INSERT INTO "witness" VALUES('Mary Ann Cox', '11:45 P.M.', 'Short, stout man, shabbily dressed. Billycock hat, blotchy face, carroty moustache, holding quart can of beer', '');
INSERT INTO "witness" VALUES('Mary Miniter', 'between 10:30 and 11:00', 'About 32 years of age. Five feet, eight inches tall. Slim build. Long, sharp nose. Heavy moustache of light color. Foreign in appearance, possibly German. Dark-brown cutaway coat. Black trousers. Old black derby hat with dented crown.', '');
INSERT INTO "witness" VALUES('George Hutchinson', '2:00 A.M.', 'Aged 34-35, 5''6", pale complexion, dark hair, slight moustached curled at each end, long dark coat, collar cuffs of astrakhan, dark jacket underneath. Light waistcoat, thick gold chain with a red stone seal, dark trousers and button boots, gaiters, white buttons. White shirt, black tie fastened with a horseshoe pin. Dark hat, turned down in middle. Red kerchief. Jewish and respectable in appearance.', '');
INSERT INTO "witness" VALUES('Ada Wilson', '', 'a man of about 30 years of age, 5ft 6ins in height, with a sunburnt face and a fair moustache. He was wearing a dark coat, light trousers and a wideawake hat.', '');
INSERT INTO "witness" VALUES('Rose Bierman', '', 'a young fair man with a light coat on', '');
INSERT INTO "witness" VALUES('"Jumbo" Friday', '', '', '');
INSERT INTO "witness" VALUES('Duncan Campnell', '', '', '');
INSERT INTO "witness" VALUES('George Chapman', '', '', '');
INSERT INTO "witness" VALUES('', '', '', '');



	
	

CREATE TABLE "suspect" (
	"name"				varchar(40) PRIMARY KEY NOT NULL,
	"picture"			varchar(80),
	"notes"				varchar(999),
	"victim"       		varchar(40)
);

INSERT INTO "suspect" VALUES('Prince Albert Victor', 'http://www.casebook.org/images/ed1890b.jpg', 'one of the most famous suspects', '');
INSERT INTO "suspect" VALUES('Joseph Barnett', 'http://www.casebook.org/images/barnett2.jpg', 'was not described as a Ripper suspect until the 1970s', 'Mary Kelly');
INSERT INTO "suspect" VALUES('Alfred Napier Blanchard', 'http://www.casebook.org/images/suspect_lodge.jpg', 'made a false confession', '');
INSERT INTO "suspect" VALUES('William Henry Bury', 'http://www.casebook.org/images/whbury.jpg', 'Police at the time investigated the matter but did not seem to consider Bury a viable suspect', 'Polly Nichols');
INSERT INTO "suspect" VALUES('Lewis Carroll', 'http://www.casebook.org/images/suspect_carroll.jpg', 'a very unlikely suspect', '');
INSERT INTO "suspect" VALUES('David Cohen', 'http://www.casebook.org/images/suspect_lodge.jpg', 'a poor Polish Jew, living in Whitechapel and who had "homocidal tendensies and a great hatred of women", and was confined to a lunatic asylum at the right time for the murders to stop and died shortly afterwards', 'Catherine Eddowes');
INSERT INTO "suspect" VALUES('Dr. Thomas Neill Cream', 'http://www.casebook.org/suspects/cream.html', 'did commit murders, but by poisoning', '');
INSERT INTO "suspect" VALUES('Frederick Bailey Deeming', 'http://www.casebook.org/images/suspect_deem.jpg', 'The only two links he may have had with the Whitechapel murders were (1) his insanity and (2) his method of killing his family', '');
INSERT INTO "suspect" VALUES('Montague John Druitt', 'http://www.casebook.org/images/suspect_druitt.jpg', 'He is considered by many to be the number one suspect in the case, yet there is little evidence', '');
INSERT INTO "suspect" VALUES('Fogelma', 'http://www.casebook.org/images/suspect_lodge.jpg', 'no outside evidence to corroborate the story told by the Empire News', '');
INSERT INTO "suspect" VALUES('George Hutchinson (Br.)', 'http://www.casebook.org/images/suspect_lodge.jpg', 'Police at the time interviewed him but did not seem to consider him a suspect', '');
INSERT INTO "suspect" VALUES('Mrs. Mary Pearcey', 'http://www.casebook.org/images/suspect_jill.jpg', 'Not very likely "Jill the Ripper"-theory, first published in 1939', 'Mary Kelly');
INSERT INTO "suspect" VALUES('James Kelly', 'http://www.casebook.org/images/suspect_jkell.jpg', 'there are some reasons in favour of and some against suspecting him', '');
INSERT INTO "suspect" VALUES('George Chapman', 'http://www.casebook.org/images/suspect_klos.jpg', 'there are some reasons in favour of and some against suspecting him', 'Carrie Brown');
INSERT INTO "suspect" VALUES('Aaron Kosminski', 'http://www.casebook.org/images/suspect_kosm.jpg', 'According to Anderson and Swanson, identified by a witness as the Ripper, but no charges were brought against him due to the witness''s reluctance to testify against "a fellow Jew." Known to have been insane.', '');
INSERT INTO "suspect" VALUES('Jacob Levy', 'http://www.casebook.org/images/suspect_lodge.jpg', 'a butcher, and the cuts inflicted upon Catharine Eddowes were suggestive of a butcher', 'Catharine Eddowes');
INSERT INTO "suspect" VALUES('The Lodger (Frances Tumblety)', 'http://www.casebook.org/images/suspect_lodge.jpg', 'a very strong suspect', '');
INSERT INTO "suspect" VALUES('James Maybrick', 'http://www.casebook.org/images/suspect_may.jpg', 'The mysterious emergence of the so-called Maybrick journal in 1992 however, immediately thrust him to the forefront of credible Ripper suspects.', '');
INSERT INTO "suspect" VALUES('Michael Ostrog', 'http://www.casebook.org/images/suspect_ost.jpg', 'Mentioned for the first time as a suspect in 1894,in 1994 a lot of information was published making him a prime suspect', '');
INSERT INTO "suspect" VALUES('Dr. Alexander Pedachenko', 'http://www.casebook.org/images/suspect_lodge.jpg', 'may never have existed', '');
INSERT INTO "suspect" VALUES('The Royal Conspiracy', 'http://www.casebook.org/images/suspect_royal.jpg', 'a fascinating tapestry of conspiracy involving virtually every person who has ever been a Ripper suspect plus a few new ones', '');
INSERT INTO "suspect" VALUES('Walter Sickert', 'http://www.casebook.org/images/suspect_sickert.jpg', 'a valid suspect since the 1990s', '');
INSERT INTO "suspect" VALUES('James Kenneth Stephen', 'http://www.casebook.org/images/suspect_jkstep.jpg', 'Known misogynist and lunatic but no connections with the East End', '');
INSERT INTO "suspect" VALUES ('R. D´Onston Stephenson', 'http://www.casebook.org/images/suspect_dons.jpg', 'Known to have had an extraordinary interest in the murders. Wrote numerous articles and letters on the matter. Resided in the East End.', ''); 
INSERT INTO "suspect" VALUES('Alois Szemeredy', 'http://www.casebook.org/images/suspect_szemeredy.jpg', 'from Buenos Aires, suspected of the Jack the Ripper- and other murders', '');
INSERT INTO "suspect" VALUES('Francis Thompson', 'http://www.casebook.org/images/suspect_thompson.jpg', 'At 29, Thompson was the right age to fit the Ripper descriptions, and we know he had some medical training. He was also said to carry a dissecting scalpel around with him, which he claimed he used to shave', '');
INSERT INTO "suspect" VALUES('Frances Tumblety', 'http://www.casebook.org/images/suspect_tumb.jpg', 'There is a strong case to be made that he was indeed the Batty Street Lodger', '');
INSERT INTO "suspect" VALUES('Nikolay Vasiliev', 'http://www.casebook.org/images/suspect_lodge.jpg', 'an elusive legend, which probably had some basis in reality, but was mostly embellished by the journalists who wrote it up', '');
INSERT INTO "suspect" VALUES('Dr. John Williams', 'http://www.casebook.org/images/dr-john-williams.jpg', 'there is very little to suggest that he was Jack the Ripper', '');
INSERT INTO "suspect" VALUES('', '', '', '');

CREATE TABLE "doctor"(
	"name"				varchar(40) PRIMARY KEY NOT NULL,
	"picture"			varchar(80)
);

INSERT INTO "doctor" VALUES('Dr. Llewellyn', '');
INSERT INTO "doctor" VALUES('Dr. George Baxter Phillips', '');
INSERT INTO "doctor" VALUES('Dr. Frederick Gordon Brown', '');
INSERT INTO "doctor" VALUES('Dr. Thomas Bond', '');
INSERT INTO "doctor" VALUES('Dr. Timothy Robert Killeen', '');
INSERT INTO "doctor" VALUES('Dr. Matthew Brownfield', '');
INSERT INTO "doctor" VALUES('Cornoer Wynne E. Baxter', '');
INSERT INTO "doctor" VALUES('Dr. Jenkins', '');
INSERT INTO "doctor" VALUES('', '');

CREATE TABLE "inspector" (
	"name"				varchar(140) PRIMARY KEY NOT NULL,
	"picture"       	varchar(80)
);

INSERT INTO "inspector" VALUES ('Inspector Frederick Abberline', 'http://www.casebook.org/images/police_abb1.jpg');
INSERT INTO "inspector" VALUES ('Sir Robert Anderson', 'http://www.casebook.org/images/police_and.jpg');
INSERT INTO "inspector" VALUES ('Inspector Walter Andrews', 'http://www.casebook.org/images/police_walter_andrews.jpg');
INSERT INTO "inspector" VALUES ('Superintendent Thomas Arnold', 'http://www.casebook.org/images/police_thomas_arnold.jpg');
INSERT INTO "inspector" VALUES ('Detective Constable Walter Dew', 'http://www.casebook.org/images/police_dew.jpg');
INSERT INTO "inspector" VALUES ('Detective Sergeant George Godley', 'http://www.casebook.org/images/police_god.jpg');
INSERT INTO "inspector" VALUES ('P.C. James Harvey', 'http://www.casebook.org/images/police_sag.jpg');
INSERT INTO "inspector" VALUES ('Inspector Joseph Henry Helson', 'http://www.casebook.org/images/police_helson.jpg');
INSERT INTO "inspector" VALUES ('Chief Inspector John George', 'http://www.casebook.org/images/police_lc.jpg');
INSERT INTO "inspector" VALUES ('Sir Melville Macnaghten', 'http://www.casebook.org/images/police_mac.jpg');
INSERT INTO "inspector" VALUES ('James Monro', 'http://www.casebook.org/images/police_mon.jpg');
INSERT INTO "inspector" VALUES ('Chief Inspector Henry Moore', 'http://www.casebook.org/images/police_henry_moore.jpg');
INSERT INTO "inspector" VALUES ('P.C. John Neil', 'http://www.casebook.org/images/police_john_neil.jpg');
INSERT INTO "inspector" VALUES ('Inspector Edmund Reid', 'http://www.casebook.org/images/police_edmund_reid.jpg');
INSERT INTO "inspector" VALUES ('Detective Constable Robert Sagar', 'http://www.casebook.org/images/police_sag.jpg');
INSERT INTO "inspector" VALUES ('Major Henry Smith', 'http://www.casebook.org/images/police_smi.jpg');
INSERT INTO "inspector" VALUES ('P.C. William Smith', 'http://www.casebook.org/images/police_william_smith.jpg');
INSERT INTO "inspector" VALUES ('Inspector John Spratling', 'http://www.casebook.org/images/police_sag.jpg');
INSERT INTO "inspector" VALUES ('Chief Inspector Donald Swanson', 'http://www.casebook.org/images/police_sut.jpg');
INSERT INTO "inspector" VALUES ('Sergeant William Thick', 'http://www.casebook.org/images/police_thi.jpg');
INSERT INTO "inspector" VALUES ('P.C. Ernest Thompson', 'http://www.casebook.org/images/police_ernest_thompson.jpg');
INSERT INTO "inspector" VALUES ('Sir Charles Warren', 'http://www.casebook.org/images/police_war.jpg');
INSERT INTO "inspector" VALUES ('P.C. Edward Watkins', 'http://www.casebook.org/images/police_edward_watkins.jpg');
INSERT INTO "inspector" VALUES ('Sergeant Stephen White', 'http://www.casebook.org/images/police_steven_white.jpg');
INSERT INTO "inspector" VALUES ('Chief Constable Adolphus Frederick Williamson', 'http://www.casebook.org/images/police_frederick_williamson.jpg');
INSERT INTO "inspector" VALUES ('','');
	
CREATE TABLE "scene"(
	"victim_name"		varchar(40) REFERENCES "victim"(name),
	"suspect_name"  	varchar(40) REFERENCES "suspect"(name),
	"witness_name"  	varchar(40) REFERENCES "witness"(name),
	"doctor_name"		varchar(40) REFERENCES "doctor"(name),
	"inspector_name"	varchar(40) REFERENCES "inspector"(name)
);
	
	
INSERT INTO "scene" VALUES ('Mary Ann Walker', '', 'Patrick Mulshaw', 'Dr. Llewellyn', 'P.C. John Neil');
INSERT INTO "scene" VALUES ('Annie Chapman', '', 'Emily Walter', 'Dr. George Baxter Phillips', '');
INSERT INTO "scene" VALUES ('Annie Chapman', '', 'Elizabeth Long', 'Dr. George Baxter Phillips', '');
INSERT INTO "scene" VALUES ('Elisabeth Stride', '', 'J. Best and John Gardner', '', '');
INSERT INTO "scene" VALUES ('Elisabeth Stride', '', 'William Marshall', 'Dr. George Baxter Phillips', '');
INSERT INTO "scene" VALUES ('Elisabeth Stride', '', 'Matthew Packer', 'Dr. George Baxter Phillips', '');
INSERT INTO "scene" VALUES ('Elisabeth Stride', '', 'P.C. William Smith', 'Dr. George Baxter Phillips', '');
INSERT INTO "scene" VALUES ('Elisabeth Stride', '', 'James Brown', 'Dr. George Baxter Phillips', '');
INSERT INTO "scene" VALUES ('Elisabeth Stride', '', 'Israel Schwartz', 'Dr. George Baxter Phillips', '');
INSERT INTO "scene" VALUES ('Catherine Eddowes', '', 'Joseph Lawende', '', '');
INSERT INTO "scene" VALUES ('Catherine Eddowes', '', 'James Blenkinsop', 'Dr. Frederick Gordon Brown', '');
INSERT INTO "scene" VALUES ('Mary Jane Kelly', '', 'Mary Ann Cox', 'Dr. George Baxter Phillips', '');
INSERT INTO "scene" VALUES ('Mary Jane Kelly', '', 'George Hutchinson', 'Dr. George Baxter Phillips', '');
INSERT INTO "scene" VALUES ('Mary Jane Kelly', '', 'Mary Ann Cox', 'Dr. Thomas Bond', '');
INSERT INTO "scene" VALUES ('Mary Jane Kelly', '', 'George Hutchinson', 'Dr. Thomas Bond', '');
INSERT INTO "scene" VALUES ('"Fairy Fay"', '', '', '', 'Inspector Edmund Reid');
INSERT INTO "scene" VALUES ('Ada Wilson', '', 'Ada Wilson', '', '');
INSERT INTO "scene" VALUES ('Ada Wilson', '', 'Rose Bierman', '', '');
INSERT INTO "scene" VALUES ('Emma Smith', '', '', '', '');
INSERT INTO "scene" VALUES ('Martha Tabram', '', '', 'Dr. Timothy Robert Killeen', 'Inspector Edmund Reid');
INSERT INTO "scene" VALUES ('Whitehall Mystery', '', '', '', '');
INSERT INTO "scene" VALUES ('Annie Farmer', '', '', '', '');
INSERT INTO "scene" VALUES ('Rose Mylett', '', '', 'Dr. Matthew Brownfield', '');
INSERT INTO "scene" VALUES ('Rose Mylett', '', '', 'Dr. George Baxter Phillips', '');
INSERT INTO "scene" VALUES ('Elisabeth Jackson', '', '', '', '');
INSERT INTO "scene" VALUES ('Alice MacKenzie', '', '', 'Dr. George Baxter Phillips', 'Inspector Edmund Reid');
INSERT INTO "scene" VALUES ('Alice MacKenzie', '', '', 'Dr. George Baxter Phillips', 'Sir Robert Anderson');
INSERT INTO "scene" VALUES ('Alice MacKenzie', '', '', 'Dr. George Baxter Phillips', 'James Monro');
INSERT INTO "scene" VALUES ('Lydia Hart', '', '', '', 'James Monro');
INSERT INTO "scene" VALUES ('Lydia Hart', '', '', '', 'Sir Melville Macnaghten');
INSERT INTO "scene" VALUES ('Lydia Hart', '', '', '', 'Chief Inspector Donald Swanson');
INSERT INTO "scene" VALUES ('Frances Coles', '', '"Jumbo" Friday', 'Cornoer Wynne E. Baxter', 'Sir Melville Macnaghten');
INSERT INTO "scene" VALUES ('Frances Coles', '', 'Duncan Campnell', 'Cornoer Wynne E. Baxter', 'Sir Melville Macnaghten');
INSERT INTO "scene" VALUES ('Carrie Brown', 'George Chapman', 'Mary Miniter', 'Dr. Jenkins', '');


SELECT name FROM victim WHERE eyes='blue';

SELECT name FROM victim WHERE features is NULL;

SELECT date_of_birth FROM victim WHERE length is not NULL;

SELECT location FROM victim WHERE name IN (SELECT name FROM inspector WHERE name='P.C. Neil ');


SELECT name FROM victim WHERE name IN (SELECT name FROM doctor WHERE name='Dr. George Baxter Phillips');


SELECT picture FROM suspect WHERE victim is NULL;


SELECT name FROM witness WHERE appearence in (SELECT "eyes" FROM victim WHERE "eyes" is NULL OR "eyes"='unknown');


SELECT name FROM victim WHERE name in (SELECT name FROM suspect where name is not NULL);


DROP TABLE "scene";
DROP TABLE "suspect";
DROP TABLE "inspector";
DROP TABLE "doctor";
DROP TABLE "witness";
DROP TABLE "victim";
DROP TABLE "timeline";
