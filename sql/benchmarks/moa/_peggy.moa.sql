 --show tables like '_t%';
 SELECT * FROM MOA_DD WHERE name='Video' AND type='match';

 SELECT * FROM MOA_DD WHERE name='MediaLocator' AND type='match';

 SELECT * FROM MOA_DD WHERE name='MediaInformation' AND type='match';

 SELECT * FROM MOA_DD WHERE name='MediaProfile' AND type='match';

 SELECT * FROM MOA_DD WHERE name='MediaFormat' AND type='match';

 SELECT * FROM MOA_DD WHERE name='Creation' AND type='match';

 SELECT * FROM MOA_DD WHERE name='CreationCreator' AND type='match';
 SELECT * FROM MOA_DD WHERE name='CreationCreator' AND type='primary';


 SELECT * FROM MOA_DD WHERE name='Creator' AND type='match';

 SELECT * FROM MOA_DD WHERE name='Individual' AND type='match';

 SELECT * FROM MOA_DD WHERE name='CreationMetaInformation' AND type='match';

 SELECT * FROM MOA_DD WHERE name='Classification' AND type='match';

 SELECT * FROM MOA_DD WHERE name='ClassificationGenre' AND type='match';

 SELECT * FROM MOA_DD WHERE name='Genre' AND type='match';

 create table _t28 (CreatorPK integer, Role varchar(255), IndividualFK integer);
 SELECT * FROM MOA_DD WHERE name='ParentalGuidance' AND type='match';

 SELECT * FROM MOA_DD WHERE name='MediaReview' AND type='match';

 SELECT * FROM MOA_DD WHERE name='RatingCriterion' AND type='match';

 SELECT * from Video;
 SELECT * from MediaLocator;
 SELECT * from MediaInformation;
 SELECT * from MediaProfile;
 SELECT * from MediaFormat;
 SELECT * from Creation;
 SELECT * from CreationCreator;
 SELECT * from Creator;
 SELECT * from Individual;
 SELECT * from CreationMetaInformation;
 SELECT * from Classification;
 SELECT * from ClassificationGenre;
 SELECT * from Genre;
 SELECT * from ParentalGuidance;
 SELECT * from MediaReview;
 SELECT * from RatingCriterion;
 create table _t1 (CreationPK integer, Title varchar(255), CreationDate varchar(255));
 INSERT INTO _t1 SELECT * from Creation WHERE (Creation.Title = 'Se7en');
 create table _t2 (ID integer, IMDB varchar(255), Image varchar(255), MediaInformationFK integer);
 INSERT INTO _t2 SELECT Video.ID, Video.IMDB, Video.Image, Video.MediaInformationFK FROM Video,_t1 WHERE (Video.ID = _t1.CreationPK);
 create table _t3 (MediaLocatorPK integer, MediaURL varchar(255), MediaTime varchar(255));
 INSERT INTO _t3 SELECT MediaLocator.MediaLocatorPK, MediaLocator.MediaURL, MediaLocator.MediaTime FROM MediaLocator,_t1 WHERE (MediaLocator.MediaLocatorPK = _t1.CreationPK);
 create table _t4 (MediaInformationPK integer, MediaProfileFK integer);
 INSERT INTO _t4 SELECT MediaInformation.MediaInformationPK, MediaInformation.MediaProfileFK FROM MediaInformation,_t2 WHERE (MediaInformation.MediaInformationPK = _t2.MediaInformationFK);
 create table _t5 (MediaProfilePK integer, master varchar(255), MediaFormatFK integer);
 INSERT INTO _t5 SELECT MediaProfile.MediaProfilePK, MediaProfile.master, MediaProfile.MediaFormatFK FROM MediaProfile,_t4 WHERE (MediaProfile.MediaProfilePK = _t4.MediaProfileFK);
 create table _t6 (MediaFormatPK integer, FileFormat varchar(255), Color varchar(255), Sound varchar(255));
 INSERT INTO _t6 SELECT MediaFormat.MediaFormatPK, MediaFormat.FileFormat, MediaFormat.Color, MediaFormat.Sound FROM MediaFormat,_t5 WHERE (MediaFormat.MediaFormatPK = _t5.MediaFormatFK);
 create table _t7 (CreationFK integer, CreatorFK integer);
 INSERT INTO _t7 SELECT CreationCreator.CreationFK, CreationCreator.CreatorFK FROM CreationCreator,_t1 WHERE (CreationCreator.CreationFK = _t1.CreationPK);
 create table _t8 (CreatorPK integer, Role varchar(255), IndividualFK integer);
 INSERT INTO _t8 SELECT Creator.CreatorPK, Creator.Role, Creator.IndividualFK FROM Creator,_t7 WHERE (Creator.CreatorPK = _t7.CreatorFK);
 create table _t9 (IndividualPK integer, PersonName varchar(255));
 INSERT INTO _t9 SELECT Individual.IndividualPK, Individual.PersonName FROM Individual,_t8 WHERE (Individual.IndividualPK = _t8.IndividualFK);
 create table _t10 (CreationMetaInformationPK integer, ClassificationFK integer);
 INSERT INTO _t10 SELECT CreationMetaInformation.CreationMetaInformationPK, CreationMetaInformation.ClassificationFK FROM CreationMetaInformation,_t1 WHERE (CreationMetaInformation.CreationMetaInformationPK = _t1.CreationPK);
 create table _t11 (ClassificationPK integer, Language varchar(255), CountryCode varchar(255), ParentalGuidanceFK integer);
 INSERT INTO _t11 SELECT Classification.ClassificationPK, Classification.Language, Classification.CountryCode, Classification.ParentalGuidanceFK FROM Classification,_t10 WHERE (Classification.ClassificationPK = _t10.ClassificationFK);
 create table _t12 (ClassificationFK integer, GenreFK integer);
 INSERT INTO _t12 SELECT ClassificationGenre.ClassificationFK, ClassificationGenre.GenreFK FROM ClassificationGenre,_t10 WHERE (ClassificationGenre.ClassificationFK = _t10.ClassificationFK);
 create table _t13 (GenrePK integer, Genre varchar(255));
 INSERT INTO _t13 SELECT Genre.GenrePK, Genre.Genre FROM Genre,_t12 WHERE (Genre.GenrePK = _t12.GenreFK);
 create table _t14 (ParentalGuidancePK integer, ParentalRatingValue varchar(255), Country varchar(255));
 INSERT INTO _t14 SELECT ParentalGuidance.ParentalGuidancePK, ParentalGuidance.ParentalRatingValue, ParentalGuidance.Country FROM ParentalGuidance,_t11 WHERE (ParentalGuidance.ParentalGuidancePK = _t11.ParentalGuidanceFK);
 create table _t15 (MediaReviewPK integer, RatingValue varchar(255), FreeTextReview string, RatingCriterionFK integer);
 INSERT INTO _t15 SELECT MediaReview.MediaReviewPK, MediaReview.RatingValue, MediaReview.FreeTextReview, MediaReview.RatingCriterionFK FROM MediaReview,_t10 WHERE (MediaReview.MediaReviewPK = _t10.ClassificationFK);
 create table _t16 (RatingCriterionPK integer, WorstRating varchar(255), BestRating varchar(255));
 INSERT INTO _t16 SELECT RatingCriterion.RatingCriterionPK, RatingCriterion.WorstRating, RatingCriterion.BestRating FROM RatingCriterion,_t15 WHERE (RatingCriterion.RatingCriterionPK = _t15.RatingCriterionFK);
 SELECT * from _t;
 SELECT * from _t;
 SELECT * from _t;
 SELECT * from _t;
 SELECT * from _t;
 SELECT * from _t;
 SELECT * from _t;
 SELECT * from _t;
 SELECT * from _t;
 SELECT * from _t1;
 SELECT * from _t1;
 SELECT * from _t1;
 SELECT * from _t1;
 SELECT * from _t1;
 SELECT * from _t1;
 SELECT * from _t1;
 create table _t17 (CreatorPK integer, Role varchar(255), IndividualFK integer);
 INSERT INTO _t17 SELECT * from Creator WHERE (Creator.Role = 'Director');
 create table _t18 (CreationFK integer, CreatorFK integer);
 INSERT INTO _t18 SELECT CreationCreator.CreationFK, CreationCreator.CreatorFK FROM CreationCreator,_t17 WHERE (CreationCreator.CreatorFK = _t17.CreatorPK);
 create table _t19 (IndividualPK integer, PersonName varchar(255));
 INSERT INTO _t19 SELECT Individual.IndividualPK, Individual.PersonName FROM Individual,_t17 WHERE (Individual.IndividualPK = _t17.IndividualFK);
 SELECT * from _t1;
 SELECT * from _t1;
 SELECT * from _t1;
 create table _t20 (CreationPK integer, Title varchar(255), CreationDate varchar(255));
 INSERT INTO _t20 SELECT * from Creation WHERE (Creation.CreationDate = 1996);
 create table _t21 (ID integer, IMDB varchar(255), Image varchar(255), MediaInformationFK integer);
 INSERT INTO _t21 SELECT Video.ID, Video.IMDB, Video.Image, Video.MediaInformationFK FROM Video,_t20 WHERE (Video.ID = _t20.CreationPK);
 create table _t22 (CreatorPK integer, Role varchar(255), IndividualFK integer);
 INSERT INTO _t22 SELECT * from Creator WHERE (Creator.Role = 'Writer');
 create table _t23 (CreationFK integer, CreatorFK integer);
 INSERT INTO _t23 SELECT CreationCreator.CreationFK, CreationCreator.CreatorFK FROM CreationCreator,_t22 WHERE (CreationCreator.CreatorFK = _t22.CreatorPK);
 create table _t24 (CreationFK integer, CreatorFK integer);
 INSERT INTO _t24 SELECT _t23.CreationFK, _t23.CreatorFK FROM _t23,_t20 WHERE (_t23.CreationFK = _t20.CreationPK);
 create table _t25 (CreatorPK integer, Role varchar(255), IndividualFK integer);
 INSERT INTO _t25 SELECT _t22.CreatorPK, _t22.Role, _t22.IndividualFK FROM _t22,_t24 WHERE (_t22.CreatorPK = _t24.CreatorFK);
 create table _t26 (IndividualPK integer, PersonName varchar(255));
 INSERT INTO _t26 SELECT Individual.IndividualPK, Individual.PersonName FROM Individual,_t22 WHERE (Individual.IndividualPK = _t22.IndividualFK);
 create table _t27 (IndividualPK integer, PersonName varchar(255));
 INSERT INTO _t27 SELECT _t26.IndividualPK, _t26.PersonName FROM _t26,_t25 WHERE (_t26.IndividualPK = _t25.IndividualFK);
 SELECT * from _t2;
 SELECT * from _t2;
 SELECT * from _t2;
 SELECT * from _t2;
 SELECT * from _t2;
 create table _t28 (CreatorPK integer, Role varchar(255), IndividualFK integer);
 INSERT INTO _t28 SELECT * from Creator WHERE (Creator.Role = 'Director');
 create table _t29 (CreationFK integer, CreatorFK integer);
 INSERT INTO _t29 SELECT CreationCreator.CreationFK, CreationCreator.CreatorFK FROM CreationCreator,_t28 WHERE (CreationCreator.CreatorFK = _t28.CreatorPK);
 create table _t30 (CreationPK integer, Title varchar(255), CreationDate varchar(255));
 INSERT INTO _t30 SELECT Creation.CreationPK, Creation.Title, Creation.CreationDate FROM Creation,_t29 WHERE (Creation.CreationPK = _t29.CreationFK);
 SELECT * from _t2;
 SELECT * from _t2;
 SELECT * from _t3;
