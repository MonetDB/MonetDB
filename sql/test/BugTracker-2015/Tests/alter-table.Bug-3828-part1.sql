CREATE TABLE hmda_lar_14 (AsOfYear DOUBLE PRECISION, RespondentID VARCHAR(255), AgencyCode VARCHAR(255), LoanType DOUBLE PRECISION, PropertyType VARCHAR(255), LoanPurpose DOUBLE PRECISION, Occupancy DOUBLE PRECISION, LoanAmount DOUBLE PRECISION, Preapproval VARCHAR(255), ActionType DOUBLE PRECISION, MSA_MD VARCHAR(255), StateCode VARCHAR(255), CountyCode VARCHAR(255), CensusTractNumber VARCHAR(255), ApplicantEthnicity VARCHAR(255), CoApplicantEthnicity VARCHAR(255), ApplicantRace1 VARCHAR(255), ApplicantRace2 VARCHAR(255), ApplicantRace3 VARCHAR(255), ApplicantRace4 VARCHAR(255), ApplicantRace5 VARCHAR(255), CoApplicantRace1 VARCHAR(255), CoApplicantRace2 VARCHAR(255), CoApplicantRace3 VARCHAR(255), CoApplicantRace4 VARCHAR(255), CoApplicantRace5 VARCHAR(255), ApplicantSex DOUBLE PRECISION, CoApplicantSex DOUBLE PRECISION, ApplicantIncome VARCHAR(255), PurchaserType VARCHAR(255), DenialReason1 VARCHAR(255), DenialReason2 VARCHAR(255), DenialReason3 VARCHAR(255), RateSpread VARCHAR(255), HOEPAStatus VARCHAR(255), LienStatus VARCHAR(255), EditStatus VARCHAR(255), SequenceNumber VARCHAR(255), Population VARCHAR(255), MinorityPopulationPct VARCHAR(255), HUDMedianFamilyIncome VARCHAR(255), TracttoMSA_MDIncomePct VARCHAR(255), NumberofOwnerOccupiedUnits VARCHAR(255), Numberof1to4Familyunits VARCHAR(255), ApplicationDateIndicator DOUBLE PRECISION);

ALTER TABLE hmda_lar_14 ADD COLUMN temp_double DOUBLE;
ALTER TABLE hmda_lar_14 DROP COLUMN tracttomsa_mdincomepct;
ALTER TABLE hmda_lar_14 ADD COLUMN tracttomsa_mdincomepct DOUBLE;
ALTER TABLE hmda_lar_14 DROP COLUMN temp_double;
ALTER TABLE hmda_lar_14 ADD COLUMN temp_double DOUBLE;
ALTER TABLE hmda_lar_14 DROP COLUMN numberofowneroccupiedunits;
ALTER TABLE hmda_lar_14 ADD COLUMN numberofowneroccupiedunits DOUBLE;
ALTER TABLE hmda_lar_14 DROP COLUMN temp_double;
ALTER TABLE hmda_lar_14 ADD COLUMN temp_double DOUBLE;
ALTER TABLE hmda_lar_14 DROP COLUMN numberof1to4familyunits;
ALTER TABLE hmda_lar_14 ADD COLUMN numberof1to4familyunits DOUBLE;
ALTER TABLE hmda_lar_14 DROP COLUMN temp_double;

CREATE TABLE hmda_ins_14 (
	activityyear             DOUBLE,
	respondentid             VARCHAR(255),
	agencycode               VARCHAR(255),
	fedtaxid                 VARCHAR(255),
	respondentname_ts        VARCHAR(255),
	respondentmailingaddress VARCHAR(255),
	respondentcity_ts        VARCHAR(255),
	respondentstate_ts       VARCHAR(255),
	respondentzipcode        VARCHAR(255),
	parentname_ts            VARCHAR(255),
	parentaddress            VARCHAR(255),
	parentcity_ts            VARCHAR(255),
	parentstate_ts           VARCHAR(255),
	parentzipcode            VARCHAR(255),
	respondentname_panel     VARCHAR(255),
	respondentcity_panel     VARCHAR(255),
	respondentstate_panel    VARCHAR(255),
	assets_panel             DOUBLE,
	otherlendercode_panel    DOUBLE,
	regioncode_panel         DOUBLE,
	larcount                 DOUBLE,
	validityerror            VARCHAR(255)
);

CREATE TABLE hmda_14 AS SELECT a.* , b.activityyear, b.fedtaxid, b.respondentname_ts, b.respondentmailingaddress, b.respondentcity_ts, b.respondentstate_ts, b.respondentzipcode, b.parentname_ts, b.parentaddress, b.parentcity_ts, b.parentstate_ts, b.parentzipcode, b.respondentname_panel, b.respondentcity_panel, b.respondentstate_panel, b.assets_panel, b.otherlendercode_panel, b.regioncode_panel, b.larcount, b.validityerror FROM hmda_lar_14 AS a INNER JOIN hmda_ins_14 AS b ON a.respondentid = b.respondentid AND a.agencycode = b.agencycode WITH DATA;

select actiontype, propertytype, loanpurpose, count(*) as num_records from hmda_14 group by actiontype, propertytype, loanpurpose;

select tables.name, columns.name, location from tables inner join columns on tables.id=columns.table_id left join storage on tables.name=storage.table and columns.name=storage.column where location is null and tables.name like 'hmda%';

