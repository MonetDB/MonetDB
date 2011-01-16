-- The Jack The Ripper case is used in different shredding formats
-- as the basis for functional testing the SQL/XML features or MonetDB
-- Here we create some relational tables as a basis for XML output rendering

create table victim( 
	name 	string,
	dob 	string,
	length 	string,
	eyes	string,
	hair	string,
	crime	string,
	place	string,
	picture	string,
	features string);
insert into victim values(
	'Mary Ann Walker',
	'1845-08-26',
	'5\'12"',
	'brown',
	'brown hair turning grey',
	'1999-08-31',
	'Buck\'s Row by Charles Cross',
	'http://www.casebook.org/images/victims_nichols.jpg',
	'five front teeth missing (Rumbelow); two bottom-one top '
	'front (Fido), her teeth are slightly discoloured. She is '
	'described as having small, delicate features with high '
	'cheekbones and grey eyes. She has a small scar on her '
	'forehead from a childhood injury.  She is described by '
	'Emily Holland as "a very clean woman who always seemed '
	'to keep to herself." The doctor at the post mortem '
	'remarked on the cleanliness of her thighs.  She is also '
	'an alcoholic.');
insert into victim values(
	'Annie Chapman',
	'1841-09',
	'5\'',
	'blue',
	'dark brown, wavy',
	'1888-09-08',
	'29 Hanbury Street',
	'http://www.casebook.org/images/victims_chapman.jpg',
	'Pallid complexion, excellent teeth (possibly two '
	'missing in lower jaw), strongly built (stout), thick nose');
insert into victim values(
	'Elisabeth Stride',
	'1843-11-27',
	'5\'5"',
	'light gray',
	'curly dark brown',
	'1888-09-30',
	'Berner Street (Henriques Street today)',
	'http://www.casebook.org/images/victims_stride.jpg',
	'pale complexion, all the teeth in her lower left '
	'jaw were missing');
            
insert into victim values(
	'Catherine Eddowes',
	'1842-04-14',
	'5\'',
	'hazel',
	'dark auburn',
	'1888-09-30',
	'Mitre Square',
	'http://www.casebook.org/images/eddowes1.jpg',
	'She has a tattoo in blue ink on her left forearm "TC."');

insert into victim values(
	'Mary Jane Kelly',
	'around 1863',
	'5\'7"',
	'blue',
	'blonde',
	'1888-11-09',
	'13 Miller\'s Court',
	'http://www.casebook.org/images/victims_kelly.jpg',
	'a fair complexion. "Said to have been possessed of considerable '
	'personal attractions." (McNaughten) She was last seen wearing a '
	'linsey frock and a red shawl pulled around her shoulders. She was '
	'bare headed. Detective Constable Walter Dew claimed to know Kelly '
	'well by sight and says that she was attractive and paraded around, '
	'usually in the company of two or three friends. He says she always '
	'wore a spotlessly clean white apron. '
	'Maria Harvey, a friend, says that she was "much superior to that '
	'of most persons in her position in life."');

insert into victim values(
	'Fairy Fay',
	null,
	null,
	null,
	null,
	'1887-12-26',
	'the alleys of Commercial Road',
	'http://www.casebook.org/images/victims_fairy.jpg',
	null);

insert into victim values(
	'Annie Millwood',
	'1850',
	null,
	null,
	null,
	'1888-02-15',
	'White\'s Row, Spitalfields',
	'http://www.casebook.org/images/victims_millwood.jpg',
	null);


insert into victim values(
	'Ada Wilson',
	null,
	null,
	null,
	null,
	'(survived the attack on 1888-28-03)',
	'19 Maidman Street',
	'http://www.casebook.org/images/victims_wilson.jpg',
	null);


insert into victim values(
	'Emma Smith',
	'1843',
	null,
	null,
	null,
	'1888-03-03',
	'just outside Taylor Brothers Mustard and Cocoa Mill '
	'which was on the north-east corner of the Wentworth/Old '
	'Montague Street crossroads',
	'http://www.casebook.org/images/victims_smith.jpg',
	null);

insert into victim values(
	'Martha Tabram',
	'1849-05-10',
	null,
	null,
	null,
	'1888-08-07',
	'George Yard, a narrow north-south alley connecting Wentworth '
	'Street and Whitechapel High Street',
	'http://www.casebook.org/images/victims_tabram.jpg',
	null);

insert into victim values(
	'Whitehall Mystery',
	null,
	null,
	null,
	null,
	'1888-10-03',
	'a vault soon to become a section of the cellar of New Scotland Yard',
	'http://www.casebook.org/images/victims_whitehall.jpg',
	'the headless and limbless torso of a woman');

insert into victim values(
	'Annie Farmer',
	'1848',
	null,
	null,
	null,
	'(survived the attack on 1888-11-20)',
	null,
	'http://www.casebook.org/images/victims_farmer.jpg',
	null);

insert into victim values(
	'Rose Mylett',
	'1862',
	null,
	null,
	null,
	'1888-12-20',
	'the yard between 184 and 186 Poplar High Street, in Clarke\'s Yard',
	'http://www.casebook.org/images/victims_mylett.jpg',
	null);

insert into victim values(
	'Elisabeth Jackson',
	null,
	null,
	null,
	null,
	'1889-06',
	'the Thames ',
	'http://www.casebook.org/images/victims_jackson.jpg',
	null);

insert into victim values(
	'Alice MacKenzie',
	'1849',
	null,
	null,
	null,
	'1889-07-17',
	'Castle Alley',
	'http://www.casebook.org/images/victims_mckenzie.jpg',
	'as a freckle-faced woman with a penchant for both smoke '
	'and drink. Her left thumb was also injured in what was '
	'no doubt some sort of industrial accident.');

insert into victim values(
	'Pinchin Street Murder, possibly Lydia Hart',
	null,
	null,
	null,
	null,
	'1889-09-10',
	'under a railway arch in Pinchin Street',
	'http://www.casebook.org/images/victims_pinchin.jpg',
	'body, missing both head and legs');

insert into victim values(
	'Frances Coles',
	'1865',
	null,
	null,
	null,
	'1891-02-13',
	'Swallow Gardens',
	'http://www.casebook.org/images/victims_coles.jpg',
	'is often heralded as the prettiest of all the murder victims');

insert into victim values(
	'Carrie Brown',
	null,
	'5\'8"',
	null,
	null,
	'1891-04-24',
	'the room of the East River Hotel on the Manhattan waterfront '
	'of New York, U.S.A.',
	'http://www.casebook.org/images/victims_brown.jpg',
	null);
