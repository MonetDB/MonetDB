	-- Taken from http://dev.lsstcorp.org/trac/browser/DMS/cat/trunk/sql/lsstSchema4mysqlDC3b.sql
	-- Modified to be acceptable to MonetDB
	-- Author: M.L. Kersten
	-- Date : Nov 2, 2010

	-- LSST Database Schema
	-- $Author$
	-- $Revision$
	-- $Date$
	--
	-- See <http://lsstdev.ncsa.uiuc.edu/trac/wiki/Copyrights>
	-- for copyright information.
	
	
	ALTER TABLE prv_cnf_PolicyKey DROP CONSTRAINT FK_prv_cnf_PolicyKey_prv_PolicyKey
	        FOREIGN KEY (policyKeyId) REFERENCES prv_PolicyKey (policyKeyId);
	
	ALTER TABLE prv_cnf_SoftwarePackage DROP CONSTRAINT FK_prv_cnf_SoftwarePackage_prv_SoftwarePackage
	        FOREIGN KEY (packageId) REFERENCES prv_SoftwarePackage (packageId);
	
	ALTER TABLE prv_PolicyKey DROP CONSTRAINT FK_prv_PolicyKey_prv_PolicyFile
	        FOREIGN KEY (policyFileId) REFERENCES prv_PolicyFile (policyFileId);
	
	ALTER TABLE _mops_EonQueue DROP CONSTRAINT FK__mopsEonQueue_MovingObject
	        FOREIGN KEY (movingObjectId,movingObjectVersion) REFERENCES MovingObject (movingObjectId,movingObjectVersion);
	
	ALTER TABLE _mops_MoidQueue DROP CONSTRAINT FK__mops_MoidQueue_MovingObject
	        FOREIGN KEY (movingObjectId,movingObjectVersion) REFERENCES MovingObject (movingObjectId,movingObjectVersion);
	
	ALTER TABLE mops_SSM DROP CONSTRAINT FK_mopsSSM_mopsSSMDesc
	        FOREIGN KEY (ssmDescId) REFERENCES mops_SSMDesc (ssmDescId);
	
	ALTER TABLE mops_Tracklet DROP CONSTRAINT FK_mopsTracklets_mopsSSM
	        FOREIGN KEY (ssmId) REFERENCES mops_SSM (ssmId);
	
	ALTER TABLE Source_pt2 DROP CONSTRAINT FK_Source_Object
	        FOREIGN KEY (objectId) REFERENCES Object (objectId);

	
	DROP TABLE AAA_Version_3_1_88 (version CHAR);
	DROP TABLE prv_Activity;
	DROP TABLE prv_cnf_PolicyKey;
	DROP TABLE prv_cnf_SoftwarePackage;
	DROP TABLE prv_Filter;
	DROP TABLE prv_PolicyFile;
	DROP TABLE prv_PolicyKey;
	DROP TABLE prv_Run;
	DROP TABLE prv_SoftwarePackage;
	DROP TABLE _MovingObjectToType;
	DROP TABLE _ObjectToType;
	DROP TABLE _qservChunkMap;
	DROP TABLE _qservObjectIdMap;
	DROP TABLE _qservSubChunkMap;
	DROP TABLE _tmpl_Id;
	DROP TABLE _tmpl_IdPair;
	DROP TABLE _tmpl_MatchPair;
	DROP TABLE Ccd_Detector;
	DROP TABLE Durations;
	DROP TABLE Filter;
	DROP TABLE Logs;
	DROP TABLE ObjectType;
	DROP TABLE sdqa_ImageStatus;
	DROP TABLE sdqa_Metric;
	DROP TABLE sdqa_Rating_ForScienceAmpExposure;
	DROP TABLE sdqa_Rating_ForScienceCcdExposure;
	DROP TABLE sdqa_Threshold;
	DROP TABLE _mops_Config;
	DROP TABLE _mops_EonQueue;
	DROP TABLE _mops_MoidQueue;
	DROP TABLE _tmpl_mops_Ephemeris;
	DROP TABLE _tmpl_mops_Prediction;
	DROP TABLE mops_Event;
	DROP TABLE mops_Event_OrbitDerivation;
	DROP TABLE mops_Event_OrbitIdentification;
	DROP TABLE mops_Event_TrackletAttribution;
	DROP TABLE mops_Event_TrackletPrecovery;
	DROP TABLE mops_MovingObjectToTracklet;
	DROP TABLE mops_SSM;
	DROP TABLE mops_SSMDesc;
	DROP TABLE mops_Tracklet;
	DROP TABLE mops_TrackletToDiaSource;
	DROP TABLE mops_TrackToTracklet;
	DROP TABLE _Raw_Ccd_ExposureToVisit;
	DROP TABLE FpaMetadata;
	DROP TABLE RaftMetadata;
	DROP TABLE Raw_Amp_Exposure;
	DROP TABLE Raw_Amp_Exposure_Metadata;
	DROP TABLE Raw_Ccd_Exposure;
	DROP TABLE Raw_Ccd_Exposure_Metadata;
	DROP TABLE Science_Amp_Exposure;
	DROP TABLE Science_Amp_Exposure_Metadata;
	DROP TABLE Science_Ccd_Exposure;
	DROP TABLE Science_Ccd_Exposure_Metadata;
	DROP TABLE Visit;
	DROP TABLE CalibSource;
	DROP TABLE DiaSource;
	DROP TABLE ForcedSource;
	DROP TABLE MovingObject;
	DROP TABLE Object;
	DROP TABLE ObjectExtras;
	DROP TABLE Source_pt1;
	DROP TABLE Source_pt2;
