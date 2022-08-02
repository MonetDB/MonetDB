	-- Taken from http://dev.lsstcorp.org/trac/browser/DMS/cat/trunk/sql/lsstSchema4mysqlDC3b.sql
	-- Modified to be acceptable to MonetDB
	-- Author: M.L. Kersten
	-- Date : Nov 2, 2010

	-- Changes to the original
	-- The INDEX directives are dropped
	-- The KEY directive is turned into a UNIQUE property of the column
	-- MySQL FOREIGN_KEY_CHECKS flag setting dropped
	-- TYPE=myisam setting dropped
	-- UNIQUE index turned into column constraint
	-- MEDIUMINT turned into INT
	-- TEXT turned into STRING
	-- DATETIME turned into TIMESTAMP
	--  dropped initialization of DATETIME,  taiObs TIMESTAMP NOT NULL, -- DEFAULT 0,
	-- name clashes: offset ->offs, second ->scnd, start ->startduration
	-- FLOAT(0) -> FLOAT
	-- incomplete foreign reference: ALTER TABLE _mops_MoidQueue ADD CONSTRAINT FK__mops_MoidQueue_MovingObject
	--      FOREIGN KEY (movingObjectId,movingObjectVersion) REFERENCES MovingObject (movingObjectId,movingObjectVersion);
	-- ALTER TABLE _mops_EonQueue ADD CONSTRAINT FK__mopsEonQueue_MovingObject
	--      FOREIGN KEY (movingObjectId,movingObjectVersion) REFERENCES MovingObject (movingObjectId,movingObjectVersion);
	-- added missing column to satisfy FK: CREATE TABLE _mops_EonQueue movingObjectVersion BIGINT NOT NULL,

	-- LSST Database Schema
	-- $Author$
	-- $Revision$
	-- $Date$
	--
	-- See <http://lsstdev.ncsa.uiuc.edu/trac/wiki/Copyrights>
	-- for copyright information.
	
	
	CREATE TABLE AAA_Version_3_1_88 (version CHAR);
	
	--SET FOREIGN_KEY_CHECKS=0;
	
	
	CREATE TABLE prv_Activity
	(
	        activityId INTEGER NOT NULL,
	        offs INTEGER NOT NULL,  
	        name VARCHAR(64) NOT NULL,
	        type VARCHAR(64) NOT NULL,
	        platform VARCHAR(64) NOT NULL,
	        PRIMARY KEY (activityId, offs)
	) ;
	
	
	CREATE TABLE prv_cnf_PolicyKey
	(
	        policyKeyId INTEGER NOT NULL,
	        value STRING NULL, -- TEXT NULL,
	        validityBegin TIMESTAMP NULL,
	        validityEnd TIMESTAMP NULL,
	        PRIMARY KEY (policyKeyId)
	) ;
	
	
	CREATE TABLE prv_cnf_SoftwarePackage
	(
	        packageId INTEGER NOT NULL,
	        version VARCHAR(255) NOT NULL,
	        directory VARCHAR(255) NOT NULL,
	        validityBegin TIMESTAMP NULL,
	        validityEnd TIMESTAMP NULL,
	        PRIMARY KEY (packageId)
	) ;
	
	
	CREATE TABLE prv_Filter
	(
	        filterId TINYINT NOT NULL,
	        focalPlaneId TINYINT NOT NULL,
	        name VARCHAR(80) UNIQUE NOT NULL,
	        url VARCHAR(255) NULL,
	        clam FLOAT NOT NULL,
	        bw FLOAT NOT NULL,
	        PRIMARY KEY (filterId)
	        --UNIQUE name(name),
	        --INDEX focalPlaneId (focalPlaneId ASC)
	) ; --TYPE=MyISAM;
	
	
	CREATE TABLE prv_PolicyFile
	(
	        policyFileId INTEGER NOT NULL,
	        pathName VARCHAR(255) NOT NULL,
	        hashValue CHAR(32) NOT NULL,
	        modifiedDate BIGINT NOT NULL,
	        PRIMARY KEY (policyFileId)
	) ;
	
	
	CREATE TABLE prv_PolicyKey
	(
	        policyKeyId INTEGER NOT NULL,
	        policyFileId INTEGER UNIQUE NOT NULL,
	        keyName VARCHAR(255) NOT NULL,
	        keyType VARCHAR(16) NOT NULL,
	        PRIMARY KEY (policyKeyId)
	        -- KEY (policyFileId)
	) ;
	
	
	CREATE TABLE prv_Run
	(
	        offs INTEGER NOT NULL AUTO_INCREMENT, --MEDIUMINT NOT NULL AUTO_INCREMENT,
	        runId VARCHAR(255) UNIQUE NOT NULL,
	        PRIMARY KEY (offs)
	        --UNIQUE UQ_prv_Run_runId(runId)
	) ;
	
	
	CREATE TABLE prv_SoftwarePackage
	(
	        packageId INTEGER NOT NULL,
	        packageName VARCHAR(64) NOT NULL,
	        PRIMARY KEY (packageId)
	) ;
	
	
	CREATE TABLE _MovingObjectToType
	(
	        movingObjectId BIGINT UNIQUE NOT NULL,
	        typeId SMALLINT UNIQUE NOT NULL,
	        probability TINYINT NULL DEFAULT 100
	        -- KEY (typeId),
	        -- KEY (movingObjectId)
	) ;
	
	
	CREATE TABLE _ObjectToType
	(
	        objectId BIGINT UNIQUE NOT NULL,
	        typeId SMALLINT UNIQUE NOT NULL,
	        probability TINYINT NULL DEFAULT 100
	        --KEY (typeId),
	        --KEY (objectId)
	) ;
	
	
	CREATE TABLE _qservChunkMap
	(
	        raMin DOUBLE NOT NULL,
	        raMax DOUBLE NOT NULL,
	        declMin DOUBLE NOT NULL,
	        declMax DOUBLE NOT NULL,
	        chunkId INTEGER NOT NULL,
	        objCount INTEGER NOT NULL
	) ;
	
	
	CREATE TABLE _qservObjectIdMap
	(
	        objectId BIGINT NOT NULL,
	        chunkId INTEGER NOT NULL,
	        subChunkId INTEGER NOT NULL
	) ;
	
	
	CREATE TABLE _qservSubChunkMap
	(
	        raMin DOUBLE NOT NULL,
	        raMax DOUBLE NOT NULL,
	        declMin DOUBLE NOT NULL,
	        declMax DOUBLE NOT NULL,
	        chunkId INTEGER NOT NULL,
	        subChunkId INTEGER NOT NULL,
	        objCount INTEGER NOT NULL
	) ;
	
	
	CREATE TABLE _tmpl_Id
	(
	        id BIGINT NOT NULL
	) ;
	
	
	CREATE TABLE _tmpl_IdPair
	(
	        first BIGINT NOT NULL,
	        scnd BIGINT NOT NULL
	) ;
	
	
	CREATE TABLE _tmpl_MatchPair
	(
	        first BIGINT NOT NULL,
	        scnd BIGINT NOT NULL,
	        distance DOUBLE NOT NULL
	) ;
	
	
	CREATE TABLE Ccd_Detector
	(
	        ccdDetectorId INTEGER NOT NULL DEFAULT 1,
	        biasSec VARCHAR(20) NOT NULL DEFAULT '[0:0,0:0]',
	        trimSec VARCHAR(20) NOT NULL DEFAULT '[0:0,0:0]',
	        gain FLOAT NULL,
	        rdNoise FLOAT NULL,
	        saturate FLOAT NULL,
	        PRIMARY KEY (ccdDetectorId)
	) ;
	
	
	CREATE TABLE Durations
	(
	        id INTEGER NOT NULL AUTO_INCREMENT,
	        RUNID VARCHAR(80) NULL,
	        name VARCHAR(80) NULL,
	        SLICEID INTEGER NULL DEFAULT -1,
	        duration BIGINT NULL,
	        HOSTID VARCHAR(80) NULL,
	        LOOPNUM INTEGER NULL DEFAULT -1,
	        STAGEID INTEGER NULL DEFAULT -1,
	        PIPELINE VARCHAR(80) NULL,
	        COMMENT VARCHAR(255) NULL,
	        startduration VARCHAR(80) NULL,
	        userduration BIGINT NULL,
	        systemduration BIGINT NULL,
	        PRIMARY KEY (id)
	        --INDEX dur_runid (RUNID ASC),
	        --INDEX idx_durations_pipeline (PIPELINE ASC),
	        --INDEX idx_durations_name (name ASC)
	) ;
	
	
	CREATE TABLE Filter
	(
	        filterId TINYINT NOT NULL,
	        filterName CHAR(255) NOT NULL,
	        photClam FLOAT NOT NULL,
	        photBW FLOAT NOT NULL,
	        PRIMARY KEY (filterId)
	) ;
	
	
	CREATE TABLE Logs
	(
	        id INTEGER NOT NULL AUTO_INCREMENT,
	        HOSTID VARCHAR(80) NULL,
	        RUNID VARCHAR(80) NULL,
	        LOG VARCHAR(80) NULL,
	        workerid VARCHAR(80) NULL,
	        SLICEID INTEGER NULL,
	        STAGEID INTEGER NULL,
	        LOOPNUM INTEGER NULL,
	        STATUS VARCHAR(80) NULL,
	        LEVEL INTEGER NULL DEFAULT 9999,
	        DATE VARCHAR(30) NULL,
	        DATETIME BIGINT NULL,
	        node INTEGER NULL,
	        custom VARCHAR(4096) NULL,
	        timereceived TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	        visitid INTEGER NULL,
	        COMMENT TEXT NULL,
	        PIPELINE VARCHAR(80) NULL,
	        TYPE VARCHAR(5) NULL,
	        EVENTTIME BIGINT NULL,
	        PUBTIME BIGINT NULL,
	        usertime BIGINT NULL,
	        systemtime BIGINT NULL,
	        PRIMARY KEY (id)
	        --INDEX a (RUNID ASC)
	) ;
	
	
	CREATE TABLE ObjectType
	(
	        typeId SMALLINT NOT NULL,
	        description VARCHAR(255) NULL,
	        PRIMARY KEY (typeId)
	) ;
	
	
	CREATE TABLE sdqa_ImageStatus
	(
	        sdqa_imageStatusId SMALLINT NOT NULL AUTO_INCREMENT,
	        statusName VARCHAR(30) NOT NULL,
	        definition VARCHAR(255) NOT NULL,
	        PRIMARY KEY (sdqa_imageStatusId)
	) ;
	
	
	CREATE TABLE sdqa_Metric
	(
	        sdqa_metricId SMALLINT NOT NULL AUTO_INCREMENT,
	        metricName VARCHAR(30) UNIQUE UNIQUE NOT NULL,
	        physicalUnits VARCHAR(30) NOT NULL,
	        dataType CHAR(1) NOT NULL,
	        definition VARCHAR(255) NOT NULL,
	        PRIMARY KEY (sdqa_metricId)
	        --UNIQUE UQ_sdqaMetric_metricName(metricName)
	) ;
	
	
	CREATE TABLE sdqa_Rating_ForScienceAmpExposure
	(
	        sdqa_ratingId BIGINT NOT NULL AUTO_INCREMENT,
	        sdqa_metricId SMALLINT UNIQUE NOT NULL,
	        sdqa_thresholdId SMALLINT UNIQUE NOT NULL,
	        ampExposureId BIGINT UNIQUE NOT NULL,
	        metricValue DOUBLE NOT NULL,
	        metricSigma DOUBLE NOT NULL,
	        PRIMARY KEY (sdqa_ratingId)
	        --?? UNIQUE UQ_sdqaRating_ForScienceAmpExposure_metricId_ampExposureId(sdqa_metricId, ampExposureId),
	        --KEY (sdqa_metricId),
	        --KEY (sdqa_thresholdId),
	        --KEY (ampExposureId)
	) ;
	
	
	CREATE TABLE sdqa_Rating_ForScienceCcdExposure
	(
	        sdqa_ratingId BIGINT NOT NULL AUTO_INCREMENT,
	        sdqa_metricId SMALLINT UNIQUE NOT NULL,
	        sdqa_thresholdId SMALLINT UNIQUE NOT NULL,
	        ccdExposureId BIGINT UNIQUE NOT NULL,
	        metricValue DOUBLE NOT NULL,
	        metricSigma DOUBLE NOT NULL,
	        PRIMARY KEY (sdqa_ratingId)
	        --?? UNIQUE UQ_sdqa_Rating_ForScienceCCDExposure_metricId_ccdExposureId(sdqa_metricId, ccdExposureId),
	        --KEY (sdqa_metricId),
	        --KEY (sdqa_thresholdId),
	        --KEY (ccdExposureId)
	) ;
	
	
	CREATE TABLE sdqa_Threshold
	(
	        sdqa_thresholdId SMALLINT NOT NULL AUTO_INCREMENT,
	        sdqa_metricId SMALLINT UNIQUE NOT NULL,
	        upperThreshold DOUBLE NULL,
	        lowerThreshold DOUBLE NULL,
	        createdDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	        PRIMARY KEY (sdqa_thresholdId)
	        --?? UNIQUE UQ_sdqa_Threshold_sdqa_metricId(sdqa_metricId),
	        --KEY (sdqa_metricId)
	) ;
	
	
	CREATE TABLE _mops_Config
	(
	        configId BIGINT NOT NULL AUTO_INCREMENT,
	        configText TEXT NULL,
	        PRIMARY KEY (configId)
	) ;
	
	
	CREATE TABLE _mops_EonQueue
	(
	        movingObjectId BIGINT NOT NULL,
	        movingObjectVersion BIGINT NOT NULL,
	        eventId BIGINT NOT NULL,
	        insertTime TIMESTAMP NOT NULL,
	        status CHAR(1) NULL DEFAULT 'I',
	        PRIMARY KEY (movingObjectId,movingObjectVersion)
	        --INDEX idx__mopsEonQueue_eventId (eventId ASC)
	) ;
	
	
	CREATE TABLE _mops_MoidQueue
	(
	        movingObjectId BIGINT UNIQUE NOT NULL,
	        movingObjectVersion INT NOT NULL,
	        eventId BIGINT NOT NULL,
	        insertTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	        PRIMARY KEY (movingObjectId, movingObjectVersion)
	        --KEY (movingObjectId),
	        --INDEX idx_mopsMoidQueue_eventId (eventId ASC)
	) ;
	
	
	CREATE TABLE _tmpl_mops_Ephemeris
	(
	        movingObjectId BIGINT NOT NULL,
	        movingObjectVersion INTEGER NOT NULL,
	        ra DOUBLE NOT NULL,
	        decl DOUBLE NOT NULL,
	        mjd DOUBLE NOT NULL,
	        smia DOUBLE NULL,
	        smaa DOUBLE NULL,
	        pa DOUBLE NULL,
	        mag DOUBLE NULL
	        --INDEX idx_mopsEphemeris_movingObjectId (movingObjectId ASC)
	) ; --TYPE=MyISAM;
	
	
	CREATE TABLE _tmpl_mops_Prediction
	(
	        movingObjectId BIGINT NOT NULL,
	        movingObjectVersion INTEGER NOT NULL,
	        ra DOUBLE NOT NULL,
	        decl DOUBLE NOT NULL,
	        mjd DOUBLE NOT NULL,
	        smia DOUBLE NOT NULL,
	        smaa DOUBLE NOT NULL,
	        pa DOUBLE NOT NULL,
	        mag DOUBLE NOT NULL,
	        magErr FLOAT NOT NULL
	) ; --TYPE=MyISAM;
	
	
	CREATE TABLE mops_Event
	(
	        eventId BIGINT NOT NULL AUTO_INCREMENT,
	        procHistoryId INT NOT NULL,
	        eventType CHAR(1) NOT NULL,
	        eventTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	        movingObjectId BIGINT UNIQUE NULL,
	        movingObjectVersion INT NULL,
	        orbitCode CHAR(1) NULL,
	        d3 FLOAT NULL,
	        d4 FLOAT NULL,
	        ccdExposureId BIGINT NULL,
	        classification CHAR(1) NULL,
	        ssmId BIGINT NULL,
	        PRIMARY KEY (eventId)
	        -- KEY (movingObjectId)
	        --INDEX idx_mopsEvent_ccdExposureId (ccdExposureId ASC),
	        --INDEX idx_mopsEvent_movingObjectId (movingObjectId ASC, movingObjectVersion ASC),
	        --INDEX idx_mopsEvent_procHistoryId (procHistoryId ASC),
	        --INDEX idx_mopsEvent_ssmId (ssmId ASC)
	) ;
	
	
	CREATE TABLE mops_Event_OrbitDerivation
	(
	        eventId BIGINT UNIQUE NOT NULL,
	        trackletId BIGINT NOT NULL,
	        PRIMARY KEY (eventId, trackletId)
	        --INDEX idx_mopsEventDerivation_trackletId (trackletId ASC),
	        --KEY (eventId)
	) ;
	
	
	CREATE TABLE mops_Event_OrbitIdentification
	(
	        eventId BIGINT NOT NULL,
	        childObjectId BIGINT NOT NULL,
	        PRIMARY KEY (eventId)
	        --INDEX idx_mopsEventOrbitIdentification2MovingObject_childObjectId (childObjectId ASC)
	) ;
	
	
	CREATE TABLE mops_Event_TrackletAttribution
	(
	        eventId BIGINT NOT NULL,
	        trackletId BIGINT NOT NULL,
	        ephemerisDistance FLOAT NOT NULL,
	        ephemerisUncertainty FLOAT NULL,
	        PRIMARY KEY (eventId)
	        --INDEX idx_mopsEventTrackletAttribution_trackletId (trackletId ASC)
	) ;
	
	
	CREATE TABLE mops_Event_TrackletPrecovery
	(
	        eventId BIGINT NOT NULL,
	        trackletId BIGINT NOT NULL,
	        ephemerisDistance FLOAT NOT NULL,
	        ephemerisUncertainty FLOAT NULL,
	        PRIMARY KEY (eventId)
	        --INDEX idx_mopsEventTrackletPrecovery_trackletId (trackletId ASC)
	) ;
	
	
	CREATE TABLE mops_Event_TrackletRemoval
	(
	        eventId BIGINT NOT NULL,
	        trackletId BIGINT NOT NULL,
	        PRIMARY KEY (eventId)
	        --INDEX idx_mopsEventTrackletRemoval_trackletId (trackletId ASC)
	) ;
	
	
	CREATE TABLE mops_MovingObjectToTracklet
	(
	        movingObjectId BIGINT NOT NULL,
	        trackletId BIGINT NOT NULL
	        --INDEX idx_mopsMovingObjectToTracklets_movingObjectId (movingObjectId ASC),
	        --INDEX idx_mopsMovingObjectToTracklets_trackletId (trackletId ASC)
	) ;
	
	
	CREATE TABLE mops_SSM
	(
	        ssmId BIGINT NOT NULL AUTO_INCREMENT,
	        ssmDescId SMALLINT NULL,
	        q DOUBLE NOT NULL,
	        e DOUBLE NOT NULL,
	        i DOUBLE NOT NULL,
	        node DOUBLE NOT NULL,
	        argPeri DOUBLE NOT NULL,
	        timePeri DOUBLE NOT NULL,
	        epoch DOUBLE NOT NULL,
	        h_v DOUBLE NOT NULL,
	        h_ss DOUBLE NULL,
	        g DOUBLE NULL,
	        albedo DOUBLE NULL,
	        ssmObjectName VARCHAR(32) NOT NULL,
	        PRIMARY KEY (ssmId)
	        --??UNIQUE UQ_mopsSSM_ssmObjectName(ssmObjectName),
	        --INDEX idx_mopsSSM_ssmDescId (ssmDescId ASC),
	        --INDEX idx_mopsSSM_epoch (epoch ASC)
	) ;
	
	
	CREATE TABLE mops_SSMDesc
	(
	        ssmDescId SMALLINT NOT NULL AUTO_INCREMENT,
	        prefix CHAR(4) NULL,
	        description VARCHAR(100) NULL,
	        PRIMARY KEY (ssmDescId)
	) ;
	
	
	CREATE TABLE mops_Tracklet
	(
	        trackletId BIGINT NOT NULL AUTO_INCREMENT,
	        ccdExposureId BIGINT NOT NULL,
	        procHistoryId INT NOT NULL,
	        ssmId BIGINT NULL,
	        velRa DOUBLE NULL,
	        velRaErr DOUBLE NULL,
	        velDecl DOUBLE NULL,
	        velDeclErr DOUBLE NULL,
	        velTot DOUBLE NULL,
	        accRa DOUBLE NULL,
	        accRaErr DOUBLE NULL,
	        accDecl DOUBLE NULL,
	        accDeclErr DOUBLE NULL,
	        extEpoch DOUBLE NULL,
	        extRa DOUBLE NULL,
	        extRaErr DOUBLE NULL,
	        extDecl DOUBLE NULL,
	        extDeclErr DOUBLE NULL,
	        extMag DOUBLE NULL,
	        extMagErr DOUBLE NULL,
	        probability DOUBLE NULL,
	        status CHAR(1) NULL,
	        classification CHAR(1) NULL,
	        PRIMARY KEY (trackletId)
	        --INDEX idx_mopsTracklets_ccdExposureId (ccdExposureId ASC),
	        --INDEX idx_mopsTracklets_ssmId (ssmId ASC),
	        --INDEX idx_mopsTracklets_classification (classification ASC),
	        --INDEX idx_mopsTracklets_extEpoch (extEpoch ASC)
	) ;
	
	
	CREATE TABLE mops_TrackletToDiaSource
	(
	        trackletId BIGINT UNIQUE NOT NULL,
	        diaSourceId BIGINT NOT NULL,
	        PRIMARY KEY (trackletId, diaSourceId)
	        --INDEX idx_mopsTrackletsToDIASource_diaSourceId (diaSourceId ASC),
	        --KEY (trackletId)
	) ;
	
	
	CREATE TABLE mops_TrackToTracklet
	(
	        trackId BIGINT NOT NULL,
	        trackletId BIGINT NOT NULL,
	        PRIMARY KEY (trackId, trackletId)
	        --INDEX IDX_mopsTrackToTracklet_trackletId (trackletId ASC)
	) ;
	
	
	CREATE TABLE _Raw_Ccd_ExposureToVisit
	(
	        visitId INTEGER UNIQUE NOT NULL,
	        ccdExposureId BIGINT UNIQUE NOT NULL
	        --KEY (ccdExposureId),
	        --KEY (visitId)
	) ;
	
	
	CREATE TABLE FpaMetadata
	(
	        ccdExposureId BIGINT NOT NULL,
	        exposureType TINYINT NOT NULL,
	        metadataKey VARCHAR(255) NOT NULL,
	        metadataValue VARCHAR(255) NULL,
	        PRIMARY KEY (ccdExposureId)
	) ;
	
	
	CREATE TABLE RaftMetadata
	(
	        raftId BIGINT NOT NULL,
	        metadataKey VARCHAR(255) NOT NULL,
	        metadataValue VARCHAR(255) NULL,
	        PRIMARY KEY (raftId)
	) ;
	
	
	CREATE TABLE Raw_Amp_Exposure
	(
	        rawAmpExposureId BIGINT NOT NULL,
	        rawCcdExposureId BIGINT UNIQUE NOT NULL,
	        ampId INTEGER NOT NULL,
	        PRIMARY KEY (rawAmpExposureId)
	        -- KEY (rawCcdExposureId)
	) ;
	
	
	CREATE TABLE Raw_Amp_Exposure_Metadata
	(
	        rawAmpExposureId BIGINT NOT NULL,
	        exposureType TINYINT NOT NULL,
	        metadataKey VARCHAR(255) NOT NULL,
	        metadataValue VARCHAR(255) NULL,
	        PRIMARY KEY (rawAmpExposureId)
	) ;
	
	
	CREATE TABLE Raw_Ccd_Exposure
	(
	        rawCcdExposureId BIGINT NOT NULL,
	        ra DOUBLE NOT NULL,
	        decl DOUBLE NOT NULL,
	        filterId INTEGER NOT NULL,
	        equinox FLOAT NOT NULL,
	        radecSys VARCHAR(20) NULL,
	        dateObs TIMESTAMP NOT NULL, -- DEFAULT 0,
	        url VARCHAR(255) NOT NULL,
	        ctype1 VARCHAR(20) NOT NULL,
	        ctype2 VARCHAR(20) NOT NULL,
	        mjdObs DOUBLE NULL,
	        airmass FLOAT NULL,
	        crpix1 FLOAT NOT NULL,
	        crpix2 FLOAT NOT NULL,
	        crval1 DOUBLE NOT NULL,
	        crval2 DOUBLE NOT NULL,
	        cd11 DOUBLE NOT NULL,
	        cd21 DOUBLE NOT NULL,
	        darkTime FLOAT NULL,
	        cd12 DOUBLE NOT NULL,
	        zd FLOAT NULL,
	        cd22 DOUBLE NOT NULL,
	        taiObs TIMESTAMP NOT NULL, -- DEFAULT 0,
	        expTime FLOAT NOT NULL,
	        PRIMARY KEY (rawCcdExposureId)
	) ;
	
	
	CREATE TABLE Raw_Ccd_Exposure_Metadata
	(
	        rawCcdExposureId BIGINT NOT NULL,
	        exposureType TINYINT NOT NULL,
	        metadataKey VARCHAR(255) NOT NULL,
	        metadataValue VARCHAR(255) NULL,
	        PRIMARY KEY (rawCcdExposureId)
	) ;
	
	
	CREATE TABLE Science_Amp_Exposure
	(
	        scienceAmpExposureId BIGINT NOT NULL,
	        scienceCcdExposureId BIGINT UNIQUE NOT NULL,
	        rawAmpExposureId BIGINT UNIQUE NULL,
	        ampId INTEGER NULL,
	        PRIMARY KEY (scienceAmpExposureId)
	        --KEY (scienceCcdExposureId),
	        --KEY (rawAmpExposureId)
	) ;
	
	
	CREATE TABLE Science_Amp_Exposure_Metadata
	(
	        scienceAmpExposureId BIGINT NOT NULL,
	        exposureType TINYINT NOT NULL,
	        metadataKey VARCHAR(255) NOT NULL,
	        metadataValue VARCHAR(255) NULL,
	        PRIMARY KEY (scienceAmpExposureId)
	) ;
	
	
	CREATE TABLE Science_Ccd_Exposure
	(
	        scienceCcdExposureId BIGINT NOT NULL,
	        rawCcdExposureId BIGINT UNIQUE NULL,
	        snapId TINYINT NOT NULL,
	        filterId INTEGER NULL,
	        equinox FLOAT NULL,
	        url VARCHAR(255) NULL,
	        ctype1 VARCHAR(20) NULL,
	        ctype2 VARCHAR(20) NULL,
	        crpix1 FLOAT NULL,
	        crpix2 FLOAT NULL,
	        crval1 DOUBLE NULL,
	        crval2 DOUBLE NULL,
	        cd1_1 DOUBLE NULL,
	        cd2_1 DOUBLE NULL,
	        cd1_2 DOUBLE NULL,
	        cd2_2 DOUBLE NULL,
	        taiMjd DOUBLE NULL,
	        ccdSize VARCHAR(50) NULL,
	        dateObs TIMESTAMP NULL, -- DEFAULT 0,
	        expTime FLOAT NULL,
	        photoFlam FLOAT NULL,
	        photoZP FLOAT NULL,
	        nCombine INTEGER NULL DEFAULT 1,
	        binX INTEGER NULL,
	        binY INTEGER NULL,
	        readNoise DOUBLE NULL,
	        saturationLimit BIGINT NULL,
	        dataSection VARCHAR(24) NULL,
	        gain DOUBLE NULL,
	        PRIMARY KEY (scienceCcdExposureId)
	        --KEY (rawCcdExposureId)
	) ;
	
	
	CREATE TABLE Science_Ccd_Exposure_Metadata
	(
	        scienceCcdExposureId BIGINT NOT NULL,
	        exposureType TINYINT NOT NULL,
	        metadataKey VARCHAR(255) NOT NULL,
	        metadataValue VARCHAR(255) NULL,
	        PRIMARY KEY (scienceCcdExposureId)
	) ;
	
	
	CREATE TABLE Visit
	(
	        visitId INTEGER NOT NULL
	) ;
	
	
	CREATE TABLE CalibSource
	(
	        calibSourceId BIGINT NOT NULL,
	        ccdExposureId BIGINT UNIQUE NULL,
	        filterId TINYINT UNIQUE NULL,
	        astroRefCatId BIGINT NULL,
	        photoRefCatId BIGINT NULL,
	        ra DOUBLE NOT NULL,
	        raSigma FLOAT NOT NULL,
	        decl DOUBLE NOT NULL,
	        declSigma FLOAT NOT NULL,
	        xAstrom DOUBLE NOT NULL,
	        xAstromSigma FLOAT UNIQUE NOT NULL,
	        yAstrom DOUBLE NOT NULL,
	        yAstromSigma FLOAT NOT NULL,
	        xyAstromCov FLOAT NOT NULL,
	        psfFlux DOUBLE NOT NULL,
	        psfFluxSigma FLOAT NOT NULL,
	        apFlux DOUBLE NOT NULL,
	        apFluxSigma FLOAT NULL,
	        momentIxx FLOAT NULL,
	        momentIxxSigma FLOAT NULL,
	        momentIyy FLOAT NULL,
	        momentIyySigma FLOAT NULL,
	        momentIxy FLOAT NULL,
	        momentIxySigma FLOAT NULL,
	        flag BIGINT NULL,
	        _chunkId INTEGER NULL,
	        _subChunkId INTEGER NULL,
	        PRIMARY KEY (calibSourceId)
	        --KEY (ccdExposureId),
	        --KEY (filterId),
	        --KEY (xAstromSigma)
	) ; --TYPE=MyISAM;
	
	
	CREATE TABLE DiaSource
	(
	        diaSourceId BIGINT NOT NULL,
	        ccdExposureId BIGINT UNIQUE NULL,
	        filterId TINYINT UNIQUE NOT NULL,
	        objectId BIGINT UNIQUE NULL,
	        movingObjectId BIGINT UNIQUE NULL,
	        ra DOUBLE NOT NULL,
	        raSigma FLOAT NOT NULL,
	        decl DOUBLE NOT NULL,
	        declSigma FLOAT NOT NULL,
	        xAstrom FLOAT NOT NULL,
	        xAstromSigma FLOAT NOT NULL,
	        yAstrom FLOAT NOT NULL,
	        yAstromSigma FLOAT NOT NULL,
	        xyAstromCov FLOAT NOT NULL,
	        xOther FLOAT NOT NULL,
	        xOtherSigma FLOAT NOT NULL,
	        yOther FLOAT NOT NULL,
	        yOtherSigma FLOAT NOT NULL,
	        xyOtherCov FLOAT NOT NULL,
	        astromRefrRa FLOAT NULL,
	        astromRefrRaSigma FLOAT NULL,
	        astromRefrDecl FLOAT NULL,
	        astromRefrDeclSigma FLOAT NULL,
	        sky FLOAT NOT NULL,
	        skySigma FLOAT NOT NULL,
	        psfLnL FLOAT NULL,
	        lnL_SG FLOAT NULL,
	        flux_PS FLOAT NOT NULL,
	        flux_PS_Sigma FLOAT NOT NULL,
	        flux_SG FLOAT NOT NULL,
	        flux_SG_Sigma FLOAT NOT NULL,
	        flux_CSG FLOAT NOT NULL,
	        flux_CSG_Sigma FLOAT NOT NULL,
	        extendedness FLOAT NULL,
	        galExtinction FLOAT NULL,
	        apCorrection FLOAT NOT NULL,
	        grayExtinction FLOAT NOT NULL,
	        nonGrayExtinction FLOAT NOT NULL,
	        midPoint FLOAT NOT NULL,
	        momentIx FLOAT NULL,
	        momentIxSigma FLOAT NULL,
	        momentIy FLOAT NULL,
	        momentIySigma FLOAT NULL,
	        momentIxx FLOAT NULL,
	        momentIxxSigma FLOAT NULL,
	        momentIyy FLOAT NULL,
	        momentIyySigma FLOAT NULL,
	        momentIxy FLOAT NULL,
	        momentIxySigma FLOAT NULL,
	        flags BIGINT NOT NULL,
	        _chunkId INTEGER NULL,
	        _subChunkId INTEGER NULL,
	        PRIMARY KEY (diaSourceId)
	        --KEY (ccdExposureId),
	        --KEY (filterId),
	        --KEY (movingObjectId),
	        --KEY (objectId)
	) ; --TYPE=MyISAM;
	
	
	CREATE TABLE ForcedSource
	(
	        objectId BIGINT NOT NULL,
	        ccdExposureId BIGINT NOT NULL,
	        sky FLOAT NOT NULL,
	        skySigma FLOAT NOT NULL,
	        flux_PS FLOAT NULL,
	        flux_PS_Sigma FLOAT NULL,
	        flux_SG FLOAT NULL,
	        flux_SG_Sigma FLOAT NULL,
	        flux_CSG FLOAT NULL,
	        flux_CSG_Sigma FLOAT NULL,
	        psfLnL FLOAT NULL,
	        modelLSLnL FLOAT NULL,
	        modelSGLnL FLOAT NULL,
	        flags BIGINT NOT NULL,
	        _chunkId INTEGER NULL,
	        _subChunkId INTEGER NULL,
	        PRIMARY KEY (objectId, ccdExposureId)
	) ;
	
	
	CREATE TABLE MovingObject
	(
	        movingObjectId BIGINT NOT NULL,
	        movingObjectVersion INT NOT NULL DEFAULT '1',
	        procHistoryId INTEGER UNIQUE NOT NULL,
	        taxonomicTypeId SMALLINT NULL,
	        ssmObjectName VARCHAR(32) NULL,
	        q DOUBLE NOT NULL,
	        e DOUBLE NOT NULL,
	        i DOUBLE NOT NULL,
	        node DOUBLE NOT NULL,
	        meanAnom DOUBLE NOT NULL,
	        argPeri DOUBLE NOT NULL,
	        distPeri DOUBLE NOT NULL,
	        timePeri DOUBLE NOT NULL,
	        epoch DOUBLE NOT NULL,
	        h_v DOUBLE NOT NULL,
	        g DOUBLE NULL DEFAULT 0.15,
	        rotationPeriod DOUBLE NULL,
	        rotationEpoch DOUBLE NULL,
	        albedo DOUBLE NULL,
	        poleLat DOUBLE NULL,
	        poleLon DOUBLE NULL,
	        d3 DOUBLE NULL,
	        d4 DOUBLE NULL,
	        orbFitResidual DOUBLE NOT NULL,
	        orbFitChi2 DOUBLE NULL,
	        classification CHAR(1) NULL,
	        ssmId BIGINT NULL,
	        mopsStatus CHAR(1) NULL,
	        stablePass CHAR(1) NULL,
	        timeCreated TIMESTAMP NULL,
	        uMag DOUBLE NULL,
	        uMagErr FLOAT NULL,
	        uAmplitude FLOAT NULL,
	        uPeriod FLOAT NULL,
	        gMag DOUBLE NULL,
	        gMagErr FLOAT NULL,
	        gAmplitude FLOAT NULL,
	        gPeriod FLOAT NULL,
	        rMag DOUBLE NULL,
	        rMagErr FLOAT NULL,
	        rAmplitude FLOAT NULL,
	        rPeriod FLOAT NULL,
	        iMag DOUBLE NULL,
	        iMagErr FLOAT NULL,
	        iAmplitude FLOAT NULL,
	        iPeriod FLOAT NULL,
	        zMag DOUBLE NULL,
	        zMagErr FLOAT NULL,
	        zAmplitude FLOAT NULL,
	        zPeriod FLOAT NULL,
	        yMag DOUBLE NULL,
	        yMagErr FLOAT NULL,
	        yAmplitude FLOAT NULL,
	        yPeriod FLOAT NULL,
	        flag INTEGER NULL,
	        src01 DOUBLE NULL,
	        src02 DOUBLE NULL,
	        src03 DOUBLE NULL,
	        src04 DOUBLE NULL,
	        src05 DOUBLE NULL,
	        src06 DOUBLE NULL,
	        src07 DOUBLE NULL,
	        src08 DOUBLE NULL,
	        src09 DOUBLE NULL,
	        src10 DOUBLE NULL,
	        src11 DOUBLE NULL,
	        src12 DOUBLE NULL,
	        src13 DOUBLE NULL,
	        src14 DOUBLE NULL,
	        src15 DOUBLE NULL,
	        src16 DOUBLE NULL,
	        src17 DOUBLE NULL,
	        src18 DOUBLE NULL,
	        src19 DOUBLE NULL,
	        src20 DOUBLE NULL,
	        src21 DOUBLE NULL,
	        convCode VARCHAR(8) NULL,
	        o_minus_c DOUBLE NULL,
	        moid1 DOUBLE NULL,
	        moidLong1 DOUBLE NULL,
	        moid2 DOUBLE NULL,
	        moidLong2 DOUBLE NULL,
	        arcLengthDays DOUBLE NULL,
	        PRIMARY KEY (movingObjectId, movingObjectVersion)
	        --KEY (procHistoryId),
	        --INDEX idx_MovingObject_taxonomicTypeId (taxonomicTypeId ASC),
	        --INDEX idx_MovingObject_ssmId (ssmId ASC),
	        --INDEX idx_MovingObject_ssmObjectName (ssmObjectName ASC),
	        --INDEX idx_MovingObject_status (mopsStatus ASC)
	) ;
	
	
	CREATE TABLE Object
	(
	        objectId BIGINT NOT NULL,
	        iauId CHAR(34) NULL,
	        ra_PS DOUBLE NOT NULL,
	        ra_PS_Sigma FLOAT NOT NULL,
	        decl_PS DOUBLE NOT NULL,
	        decl_PS_Sigma FLOAT NOT NULL,
	        radecl_PS_Cov FLOAT NULL,
	        ra_SG DOUBLE NULL,
	        ra_SG_Sigma FLOAT NULL,
	        decl_SG DOUBLE NULL,
	        decl_SG_Sigma FLOAT NULL,
	        radecl_SG_Cov FLOAT NULL,
	        raRange FLOAT NULL,
	        declRange FLOAT NULL,
	        muRa_PS DOUBLE NULL,
	        muRa_PS_Sigma FLOAT NULL,
	        muDecl_PS DOUBLE NULL,
	        muDecl_PS_Sigma FLOAT NULL,
	        muRaDecl_PS_Cov FLOAT NULL,
	        parallax_PS DOUBLE NULL,
	        parallax_PS_Sigma FLOAT NULL,
	        canonicalFilterId TINYINT NULL,
	        extendedness FLOAT NULL,
	        varProb FLOAT NULL,
	        earliestObsTime DOUBLE NULL,
	        latestObsTime DOUBLE NULL,
	        flags INTEGER NULL,
	        uNumObs INTEGER NULL,
	        uExtendedness FLOAT NULL,
	        uVarProb FLOAT NULL,
	        uRaOffset_PS FLOAT NULL,
	        uRaOffset_PS_Sigma FLOAT NULL,
	        uDeclOffset_PS FLOAT NULL,
	        uDeclOffset_PS_Sigma FLOAT NULL,
	        uRaDeclOffset_PS_Cov FLOAT NULL,
	        uRaOffset_SG FLOAT NULL,
	        uRaOffset_SG_Sigma FLOAT NULL,
	        uDeclOffset_SG FLOAT NULL,
	        uDeclOffset_SG_Sigma FLOAT NULL,
	        uRaDeclOffset_SG_Cov FLOAT NULL,
	        uLnL_PS FLOAT NULL,
	        uLnL_SG FLOAT NULL,
	        uFlux_PS FLOAT NULL,
	        uFlux_PS_Sigma FLOAT NULL,
	        uFlux_SG FLOAT NULL,
	        uFlux_SG_Sigma FLOAT NULL,
	        uFlux_CSG FLOAT NULL,
	        uFlux_CSG_Sigma FLOAT NULL,
	        uTimescale FLOAT NULL,
	        uEarliestObsTime DOUBLE NULL,
	        uLatestObsTime DOUBLE NULL,
	        uSersicN_SG FLOAT NULL,
	        uSersicN_SG_Sigma FLOAT NULL,
	        uE1_SG FLOAT NULL,
	        uE1_SG_Sigma FLOAT NULL,
	        uE2_SG FLOAT NULL,
	        uE2_SG_Sigma FLOAT NULL,
	        uRadius_SG FLOAT NULL,
	        uRadius_SG_Sigma FLOAT NULL,
	        uFlags INTEGER NULL,
	        gNumObs INTEGER NULL,
	        gExtendedness FLOAT NULL,
	        gVarProb FLOAT NULL,
	        gRaOffset_PS FLOAT NULL,
	        gRaOffset_PS_Sigma FLOAT NULL,
	        gDeclOffset_PS FLOAT NULL,
	        gDeclOffset_PS_Sigma FLOAT NULL,
	        gRaDeclOffset_PS_Cov FLOAT NULL,
	        gRaOffset_SG FLOAT NULL,
	        gRaOffset_SG_Sigma FLOAT NULL,
	        gDeclOffset_SG FLOAT NULL,
	        gDeclOffset_SG_Sigma FLOAT NULL,
	        gRaDeclOffset_SG_Cov FLOAT NULL,
	        gLnL_PS FLOAT NULL,
	        gLnL_SG FLOAT NULL,
	        gFlux_PS FLOAT NULL,
	        gFlux_PS_Sigma FLOAT NULL,
	        gFlux_SG FLOAT NULL,
	        gFlux_SG_Sigma FLOAT NULL,
	        gFlux_CSG FLOAT NULL,
	        gFlux_CSG_Sigma FLOAT NULL,
	        gTimescale FLOAT NULL,
	        gEarliestObsTime DOUBLE NULL,
	        gLatestObsTime DOUBLE NULL,
	        gSersicN_SG FLOAT NULL,
	        gSersicN_SG_Sigma FLOAT NULL,
	        gE1_SG FLOAT NULL,
	        gE1_SG_Sigma FLOAT NULL,
	        gE2_SG FLOAT NULL,
	        gE2_SG_Sigma FLOAT NULL,
	        gRadius_SG FLOAT NULL,
	        gRadius_SG_Sigma FLOAT NULL,
	        gFlags INTEGER NULL,
	        rNumObs INTEGER NULL,
	        rExtendedness FLOAT NULL,
	        rVarProb FLOAT NULL,
	        rRaOffset_PS FLOAT NULL,
	        rRaOffset_PS_Sigma FLOAT NULL,
	        rDeclOffset_PS FLOAT NULL,
	        rDeclOffset_PS_Sigma FLOAT NULL,
	        rRaDeclOffset_PS_Cov FLOAT NULL,
	        rRaOffset_SG FLOAT NULL,
	        rRaOffset_SG_Sigma FLOAT NULL,
	        rDeclOffset_SG FLOAT NULL,
	        rDeclOffset_SG_Sigma FLOAT NULL,
	        rRaDeclOffset_SG_Cov FLOAT NULL,
	        rLnL_PS FLOAT NULL,
	        rLnL_SG FLOAT NULL,
	        rFlux_PS FLOAT NULL,
	        rFlux_PS_Sigma FLOAT NULL,
	        rFlux_SG FLOAT NULL,
	        rFlux_SG_Sigma FLOAT NULL,
	        rFlux_CSG FLOAT NULL,
	        rFlux_CSG_Sigma FLOAT NULL,
	        rTimescale FLOAT NULL,
	        rEarliestObsTime DOUBLE NULL,
	        rLatestObsTime DOUBLE NULL,
	        rSersicN_SG FLOAT NULL,
	        rSersicN_SG_Sigma FLOAT NULL,
	        rE1_SG FLOAT NULL,
	        rE1_SG_Sigma FLOAT NULL,
	        rE2_SG FLOAT NULL,
	        rE2_SG_Sigma FLOAT NULL,
	        rRadius_SG FLOAT NULL,
	        rRadius_SG_Sigma FLOAT NULL,
	        rFlags INTEGER NULL,
	        iNumObs INTEGER NULL,
	        iExtendedness FLOAT NULL,
	        iVarProb FLOAT NULL,
	        iRaOffset_PS FLOAT NULL,
	        iRaOffset_PS_Sigma FLOAT NULL,
	        iDeclOffset_PS FLOAT NULL,
	        iDeclOffset_PS_Sigma FLOAT NULL,
	        iRaDeclOffset_PS_Cov FLOAT NULL,
	        iRaOffset_SG FLOAT NULL,
	        iRaOffset_SG_Sigma FLOAT NULL,
	        iDeclOffset_SG FLOAT NULL,
	        iDeclOffset_SG_Sigma FLOAT NULL,
	        iRaDeclOffset_SG_Cov FLOAT NULL,
	        iLnL_PS FLOAT NULL,
	        iLnL_SG FLOAT NULL,
	        iFlux_PS FLOAT NULL,
	        iFlux_PS_Sigma FLOAT NULL,
	        iFlux_SG FLOAT NULL,
	        iFlux_SG_Sigma FLOAT NULL,
	        iFlux_CSG FLOAT NULL,
	        iFlux_CSG_Sigma FLOAT NULL,
	        iTimescale FLOAT NULL,
	        iEarliestObsTime DOUBLE NULL,
	        iLatestObsTime DOUBLE NULL,
	        iSersicN_SG FLOAT NULL,
	        iSersicN_SG_Sigma FLOAT NULL,
	        iE1_SG FLOAT NULL,
	        iE1_SG_Sigma FLOAT NULL,
	        iE2_SG FLOAT NULL,
	        iE2_SG_Sigma FLOAT NULL,
	        iRadius_SG FLOAT NULL,
	        iRadius_SG_Sigma FLOAT NULL,
	        iFlags INTEGER NULL,
	        zNumObs INTEGER NULL,
	        zExtendedness FLOAT NULL,
	        zVarProb FLOAT NULL,
	        zRaOffset_PS FLOAT NULL,
	        zRaOffset_PS_Sigma FLOAT NULL,
	        zDeclOffset_PS FLOAT NULL,
	        zDeclOffset_PS_Sigma FLOAT NULL,
	        zRaDeclOffset_PS_Cov FLOAT NULL,
	        zRaOffset_SG FLOAT NULL,
	        zRaOffset_SG_Sigma FLOAT NULL,
	        zDeclOffset_SG FLOAT NULL,
	        zDeclOffset_SG_Sigma FLOAT NULL,
	        zRaDeclOffset_SG_Cov FLOAT NULL,
	        zLnL_PS FLOAT NULL,
	        zLnL_SG FLOAT NULL,
	        zFlux_PS FLOAT NULL,
	        zFlux_PS_Sigma FLOAT NULL,
	        zFlux_SG FLOAT NULL,
	        zFlux_SG_Sigma FLOAT NULL,
	        zFlux_CSG FLOAT NULL,
	        zFlux_CSG_Sigma FLOAT NULL,
	        zTimescale FLOAT NULL,
	        zEarliestObsTime DOUBLE NULL,
	        zLatestObsTime DOUBLE NULL,
	        zSersicN_SG FLOAT NULL,
	        zSersicN_SG_Sigma FLOAT NULL,
	        zE1_SG FLOAT NULL,
	        zE1_SG_Sigma FLOAT NULL,
	        zE2_SG FLOAT NULL,
	        zE2_SG_Sigma FLOAT NULL,
	        zRadius_SG FLOAT NULL,
	        zRadius_SG_Sigma FLOAT NULL,
	        zFlags INTEGER NULL,
	        yNumObs INTEGER NULL,
	        yExtendedness FLOAT NULL,
	        yVarProb FLOAT NULL,
	        yRaOffset_PS FLOAT NULL,
	        yRaOffset_PS_Sigma FLOAT NULL,
	        yDeclOffset_PS FLOAT NULL,
	        yDeclOffset_PS_Sigma FLOAT NULL,
	        yRaDeclOffset_PS_Cov FLOAT NULL,
	        yRaOffset_SG FLOAT NULL,
	        yRaOffset_SG_Sigma FLOAT NULL,
	        yDeclOffset_SG FLOAT NULL,
	        yDeclOffset_SG_Sigma FLOAT NULL,
	        yRaDeclOffset_SG_Cov FLOAT NULL,
	        yLnL_PS FLOAT NULL,
	        yLnL_SG FLOAT NULL,
	        yFlux_PS FLOAT NULL,
	        yFlux_PS_Sigma FLOAT NULL,
	        yFlux_SG FLOAT NULL,
	        yFlux_SG_Sigma FLOAT NULL,
	        yFlux_CSG FLOAT NULL,
	        yFlux_CSG_Sigma FLOAT NULL,
	        yTimescale FLOAT NULL,
	        yEarliestObsTime DOUBLE NULL,
	        yLatestObsTime DOUBLE NULL,
	        ySersicN_SG FLOAT NULL,
	        ySersicN_SG_Sigma FLOAT NULL,
	        yE1_SG FLOAT NULL,
	        yE1_SG_Sigma FLOAT NULL,
	        yE2_SG FLOAT NULL,
	        yE2_SG_Sigma FLOAT NULL,
	        yRadius_SG FLOAT NULL,
	        yRadius_SG_Sigma FLOAT NULL,
	        yFlags INTEGER NULL,
	        _chunkId INTEGER NULL,
	        _subChunkId INTEGER NULL,
	        PRIMARY KEY (objectId)
	) ; --TYPE=MyISAM;
	
	
	CREATE TABLE ObjectExtras
	(
	        objectId BIGINT NOT NULL,
	        uFlux_ra_PS_Cov FLOAT NULL,
	        uFlux_decl_PS_Cov FLOAT NULL,
	        uRa_decl_PS_Cov FLOAT NULL,
	        uFlux_ra_SG_Cov FLOAT NULL,
	        uFlux_decl_SG_Cov FLOAT NULL,
	        uFlux_SersicN_SG_Cov FLOAT NULL,
	        uFlux_e1_SG_Cov FLOAT NULL,
	        uFlux_e2_SG_Cov FLOAT NULL,
	        uFlux_radius_SG_Cov FLOAT NULL,
	        uRa_decl_SG_Cov FLOAT NULL,
	        uRa_SersicN_SG_Cov FLOAT NULL,
	        uRa_e1_SG_Cov FLOAT NULL,
	        uRa_e2_SG_Cov FLOAT NULL,
	        uRa_radius_SG_Cov FLOAT NULL,
	        uDecl_SersicN_SG_Cov FLOAT NULL,
	        uDecl_e1_SG_Cov FLOAT NULL,
	        uDecl_e2_SG_Cov FLOAT NULL,
	        uDecl_radius_SG_Cov FLOAT NULL,
	        uSersicN_e1_SG_Cov FLOAT NULL,
	        uSersicN_e2_SG_Cov FLOAT NULL,
	        uSersicN_radius_SG_Cov FLOAT NULL,
	        uE1_e2_SG_Cov FLOAT NULL,
	        uE1_radius_SG_Cov FLOAT NULL,
	        uE2_radius_SG_Cov FLOAT NULL,
	        gFlux_ra_PS_Cov FLOAT NULL,
	        gFlux_decl_PS_Cov FLOAT NULL,
	        gRa_decl_PS_Cov FLOAT NULL,
	        gFlux_ra_SG_Cov FLOAT NULL,
	        gFlux_decl_SG_Cov FLOAT NULL,
	        gFlux_SersicN_SG_Cov FLOAT NULL,
	        gFlux_e1_SG_Cov FLOAT NULL,
	        gFlux_e2_SG_Cov FLOAT NULL,
	        gFlux_radius_SG_Cov FLOAT NULL,
	        gRa_decl_SG_Cov FLOAT NULL,
	        gRa_SersicN_SG_Cov FLOAT NULL,
	        gRa_e1_SG_Cov FLOAT NULL,
	        gRa_e2_SG_Cov FLOAT NULL,
	        gRa_radius_SG_Cov FLOAT NULL,
	        gDecl_SersicN_SG_Cov FLOAT NULL,
	        gDecl_e1_SG_Cov FLOAT NULL,
	        gDecl_e2_SG_Cov FLOAT NULL,
	        gDecl_radius_SG_Cov FLOAT NULL,
	        gSersicN_e1_SG_Cov FLOAT NULL,
	        gSersicN_e2_SG_Cov FLOAT NULL,
	        gSersicN_radius_SG_Cov FLOAT NULL,
	        gE1_e2_SG_Cov FLOAT NULL,
	        gE1_radius_SG_Cov FLOAT NULL,
	        gE2_radius_SG_Cov FLOAT NULL,
	        rFlux_ra_PS_Cov FLOAT NULL,
	        rFlux_decl_PS_Cov FLOAT NULL,
	        rRa_decl_PS_Cov FLOAT NULL,
	        rFlux_ra_SG_Cov FLOAT NULL,
	        rFlux_decl_SG_Cov FLOAT NULL,
	        rFlux_SersicN_SG_Cov FLOAT NULL,
	        rFlux_e1_SG_Cov FLOAT NULL,
	        rFlux_e2_SG_Cov FLOAT NULL,
	        rFlux_radius_SG_Cov FLOAT NULL,
	        rRa_decl_SG_Cov FLOAT NULL,
	        rRa_SersicN_SG_Cov FLOAT NULL,
	        rRa_e1_SG_Cov FLOAT NULL,
	        rRa_e2_SG_Cov FLOAT NULL,
	        rRa_radius_SG_Cov FLOAT NULL,
	        rDecl_SersicN_SG_Cov FLOAT NULL,
	        rDecl_e1_SG_Cov FLOAT NULL,
	        rDecl_e2_SG_Cov FLOAT NULL,
	        rDecl_radius_SG_Cov FLOAT NULL,
	        rSersicN_e1_SG_Cov FLOAT NULL,
	        rSersicN_e2_SG_Cov FLOAT NULL,
	        rSersicN_radius_SG_Cov FLOAT NULL,
	        rE1_e2_SG_Cov FLOAT NULL,
	        rE1_radius_SG_Cov FLOAT NULL,
	        rE2_radius_SG_Cov FLOAT NULL,
	        iFlux_ra_PS_Cov FLOAT NULL,
	        iFlux_decl_PS_Cov FLOAT NULL,
	        iRa_decl_PS_Cov FLOAT NULL,
	        iFlux_ra_SG_Cov FLOAT NULL,
	        iFlux_decl_SG_Cov FLOAT NULL,
	        iFlux_SersicN_SG_Cov FLOAT NULL,
	        iFlux_e1_SG_Cov FLOAT NULL,
	        iFlux_e2_SG_Cov FLOAT NULL,
	        iFlux_radius_SG_Cov FLOAT NULL,
	        iRa_decl_SG_Cov FLOAT NULL,
	        iRa_SersicN_SG_Cov FLOAT NULL,
	        iRa_e1_SG_Cov FLOAT NULL,
	        iRa_e2_SG_Cov FLOAT NULL,
	        iRa_radius_SG_Cov FLOAT NULL,
	        iDecl_SersicN_SG_Cov FLOAT NULL,
	        iDecl_e1_SG_Cov FLOAT NULL,
	        iDecl_e2_SG_Cov FLOAT NULL,
	        iDecl_radius_SG_Cov FLOAT NULL,
	        iSersicN_e1_SG_Cov FLOAT NULL,
	        iSersicN_e2_SG_Cov FLOAT NULL,
	        iSersicN_radius_SG_Cov FLOAT NULL,
	        iE1_e2_SG_Cov FLOAT NULL,
	        iE1_radius_SG_Cov FLOAT NULL,
	        iE2_radius_SG_Cov FLOAT NULL,
	        zFlux_ra_PS_Cov FLOAT NULL,
	        zFlux_decl_PS_Cov FLOAT NULL,
	        zRa_decl_PS_Cov FLOAT NULL,
	        zFlux_ra_SG_Cov FLOAT NULL,
	        zFlux_decl_SG_Cov FLOAT NULL,
	        zFlux_SersicN_SG_Cov FLOAT NULL,
	        zFlux_e1_SG_Cov FLOAT NULL,
	        zFlux_e2_SG_Cov FLOAT NULL,
	        zFlux_radius_SG_Cov FLOAT NULL,
	        zRa_decl_SG_Cov FLOAT NULL,
	        zRa_SersicN_SG_Cov FLOAT NULL,
	        zRa_e1_SG_Cov FLOAT NULL,
	        zRa_e2_SG_Cov FLOAT NULL,
	        zRa_radius_SG_Cov FLOAT NULL,
	        zDecl_SersicN_SG_Cov FLOAT NULL,
	        zDecl_e1_SG_Cov FLOAT NULL,
	        zDecl_e2_SG_Cov FLOAT NULL,
	        zDecl_radius_SG_Cov FLOAT NULL,
	        zSersicN_e1_SG_Cov FLOAT NULL,
	        zSersicN_e2_SG_Cov FLOAT NULL,
	        zSersicN_radius_SG_Cov FLOAT NULL,
	        zE1_e2_SG_Cov FLOAT NULL,
	        zE1_radius_SG_Cov FLOAT NULL,
	        zE2_radius_SG_Cov FLOAT NULL,
	        yFlux_ra_PS_Cov FLOAT NULL,
	        yFlux_decl_PS_Cov FLOAT NULL,
	        yRa_decl_PS_Cov FLOAT NULL,
	        yFlux_ra_SG_Cov FLOAT NULL,
	        yFlux_decl_SG_Cov FLOAT NULL,
	        yFlux_SersicN_SG_Cov FLOAT NULL,
	        yFlux_e1_SG_Cov FLOAT NULL,
	        yFlux_e2_SG_Cov FLOAT NULL,
	        yFlux_radius_SG_Cov FLOAT NULL,
	        yRa_decl_SG_Cov FLOAT NULL,
	        yRa_SersicN_SG_Cov FLOAT NULL,
	        yRa_e1_SG_Cov FLOAT NULL,
	        yRa_e2_SG_Cov FLOAT NULL,
	        yRa_radius_SG_Cov FLOAT NULL,
	        yDecl_SersicN_SG_Cov FLOAT NULL,
	        yDecl_e1_SG_Cov FLOAT NULL,
	        yDecl_e2_SG_Cov FLOAT NULL,
	        yDecl_radius_SG_Cov FLOAT NULL,
	        ySersicN_e1_SG_Cov FLOAT NULL,
	        ySersicN_e2_SG_Cov FLOAT NULL,
	        ySersicN_radius_SG_Cov FLOAT NULL,
	        yE1_e2_SG_Cov FLOAT NULL,
	        yE1_radius_SG_Cov FLOAT NULL,
	        yE2_radius_SG_Cov FLOAT NULL,
	        _chunkId INTEGER NULL,
	        _subChunkId INTEGER NULL,
	        PRIMARY KEY (objectId)
	) ;
	
	
	CREATE TABLE Source_pt1
	(
	        sourceId BIGINT NOT NULL,
	        ampExposureId BIGINT NULL,
	        filterId TINYINT NOT NULL,
	        objectId BIGINT NULL,
	        movingObjectId BIGINT NULL,
	        procHistoryId INTEGER NOT NULL,
	        ra DOUBLE NOT NULL,
	        raErrForDetection FLOAT NULL,
	        raErrForWcs FLOAT NOT NULL,
	        decl DOUBLE NOT NULL,
	        declErrForDetection FLOAT NULL,
	        declErrForWcs FLOAT NOT NULL,
	        xFlux DOUBLE NULL,
	        xFluxErr FLOAT NULL,
	        yFlux DOUBLE NULL,
	        yFluxErr FLOAT NULL,
	        raFlux DOUBLE NULL,
	        raFluxErr FLOAT NULL,
	        declFlux DOUBLE NULL,
	        declFluxErr FLOAT NULL,
	        xPeak DOUBLE NULL,
	        yPeak DOUBLE NULL,
	        raPeak DOUBLE NULL,
	        declPeak DOUBLE NULL,
	        xAstrom DOUBLE NULL,
	        xAstromErr FLOAT NULL,
	        yAstrom DOUBLE NULL,
	        yAstromErr FLOAT NULL,
	        raAstrom DOUBLE NULL,
	        raAstromErr FLOAT NULL,
	        declAstrom DOUBLE NULL,
	        declAstromErr FLOAT NULL,
	        raObject DOUBLE NULL,
	        declObject DOUBLE NULL,
	        taiMidPoint DOUBLE NOT NULL,
	        taiRange FLOAT NULL,
	        psfFlux DOUBLE NOT NULL,
	        psfFluxErr FLOAT NOT NULL,
	        apFlux DOUBLE NOT NULL,
	        apFluxErr FLOAT NOT NULL,
	        modelFlux DOUBLE NOT NULL,
	        modelFluxErr FLOAT NOT NULL,
	        petroFlux DOUBLE NULL,
	        petroFluxErr FLOAT NULL,
	        instFlux DOUBLE NOT NULL,
	        instFluxErr FLOAT NOT NULL,
	        nonGrayCorrFlux DOUBLE NULL,
	        nonGrayCorrFluxErr FLOAT NULL,
	        atmCorrFlux DOUBLE NULL,
	        atmCorrFluxErr FLOAT NULL,
	        apDia FLOAT NULL,
	        Ixx FLOAT NULL,
	        IxxErr FLOAT NULL,
	        Iyy FLOAT NULL,
	        IyyErr FLOAT NULL,
	        Ixy FLOAT NULL,
	        IxyErr FLOAT NULL,
	        snr FLOAT NOT NULL,
	        chi2 FLOAT NOT NULL,
	        sky FLOAT NULL,
	        skyErr FLOAT NULL,
	        extendedness FLOAT NULL,
	        flux_PS FLOAT NULL,
	        flux_PS_Sigma FLOAT NULL,
	        flux_SG FLOAT NULL,
	        flux_SG_Sigma FLOAT NULL,
	        sersicN_SG FLOAT NULL,
	        sersicN_SG_Sigma FLOAT NULL,
	        e1_SG FLOAT NULL,
	        e1_SG_Sigma FLOAT NULL,
	        e2_SG FLOAT NULL,
	        e2_SG_Sigma FLOAT NULL,
	        radius_SG FLOAT NULL,
	        radius_SG_Sigma FLOAT NULL,
	        flux_flux_SG_Cov FLOAT NULL,
	        flux_e1_SG_Cov FLOAT NULL,
	        flux_e2_SG_Cov FLOAT NULL,
	        flux_radius_SG_Cov FLOAT NULL,
	        flux_sersicN_SG_Cov FLOAT NULL,
	        e1_e1_SG_Cov FLOAT NULL,
	        e1_e2_SG_Cov FLOAT NULL,
	        e1_radius_SG_Cov FLOAT NULL,
	        e1_sersicN_SG_Cov FLOAT NULL,
	        e2_e2_SG_Cov FLOAT NULL,
	        e2_radius_SG_Cov FLOAT NULL,
	        e2_sersicN_SG_Cov FLOAT NULL,
	        radius_radius_SG_Cov FLOAT NULL,
	        radius_sersicN_SG_Cov FLOAT NULL,
	        sersicN_sersicN_SG_Cov FLOAT NULL,
	        flagForAssociation SMALLINT NULL,
	        flagForDetection INTEGER NULL,
	        flagForWcs SMALLINT NULL,
	        PRIMARY KEY (sourceId)
	        --INDEX ampExposureId (ampExposureId ASC),
	        --INDEX filterId (filterId ASC),
	        --INDEX movingObjectId (movingObjectId ASC),
	        --INDEX objectId (objectId ASC),
	        --INDEX procHistoryId (procHistoryId ASC)
	) ; --TYPE=MyISAM;
	
	
	CREATE TABLE Source_pt2
	(
	        sourceId BIGINT NOT NULL,
	        ccdExposureId BIGINT UNIQUE NULL,
	        filterId TINYINT UNIQUE NOT NULL,
	        objectId BIGINT UNIQUE NULL,
	        movingObjectId BIGINT UNIQUE NULL,
	        ra DOUBLE NOT NULL,
	        raSigma FLOAT NOT NULL,
	        decl DOUBLE NOT NULL,
	        declSigma FLOAT NOT NULL,
	        xAstrom FLOAT NOT NULL,
	        xAstromSigma FLOAT NOT NULL,
	        yAstrom FLOAT NOT NULL,
	        yAstromSigma FLOAT NOT NULL,
	        xyAstromCov FLOAT NOT NULL,
	        xOther FLOAT NOT NULL,
	        xOtherSigma FLOAT NOT NULL,
	        yOther FLOAT NOT NULL,
	        yOtherSigma FLOAT NOT NULL,
	        xyOtherCov FLOAT NOT NULL,
	        astromRefrRa FLOAT NULL,
	        astromRefrRaSigma FLOAT NULL,
	        astromRefrDecl FLOAT NULL,
	        astromRefrDeclSigma FLOAT NULL,
	        sky FLOAT NOT NULL,
	        skySigma FLOAT NOT NULL,
	        psfLnL FLOAT NULL,
	        lnL_SG FLOAT NULL,
	        flux_PS FLOAT NULL,
	        flux_PS_Sigma FLOAT NULL,
	        flux_SG FLOAT NULL,
	        flux_SG_Sigma FLOAT NULL,
	        flux_CSG FLOAT NULL,
	        flux_CSG_Sigma FLOAT NULL,
	        extendedness FLOAT NULL,
	        galExtinction FLOAT NULL,
	        sersicN_SG FLOAT NULL,
	        sersicN_SG_Sigma FLOAT NULL,
	        e1_SG FLOAT NULL,
	        e1_SG_Sigma FLOAT NULL,
	        e2_SG FLOAT NULL,
	        e2_SG_Sigma FLOAT NULL,
	        radius_SG FLOAT NULL,
	        radius_SG_Sigma FLOAT NULL,
	        midPoint FLOAT NOT NULL,
	        apCorrection FLOAT NOT NULL,
	        grayExtinction FLOAT NOT NULL,
	        nonGrayExtinction FLOAT NOT NULL,
	        momentIx FLOAT NULL,
	        momentIxSigma FLOAT NULL,
	        momentIy FLOAT NULL,
	        momentIySigma FLOAT NULL,
	        momentIxx FLOAT NULL,
	        momentIxxSigma FLOAT NULL,
	        momentIyy FLOAT NULL,
	        momentIyySigma FLOAT NULL,
	        momentIxy FLOAT NULL,
	        momentIxySigma FLOAT NULL,
	        flags BIGINT NOT NULL,
	        _chunkId INTEGER NULL,
	        _subChunkId INTEGER NULL,
	        PRIMARY KEY (sourceId)
	        --KEY (objectId),
	        --KEY (ccdExposureId),
	        --KEY (filterId),
	        --KEY (movingObjectId),
	        --KEY (objectId)
	) ; --TYPE=MyISAM;
	
	
	
	--SET FOREIGN_KEY_CHECKS=1;
	
	
	ALTER TABLE prv_cnf_PolicyKey ADD CONSTRAINT FK_prv_cnf_PolicyKey_prv_PolicyKey
	        FOREIGN KEY (policyKeyId) REFERENCES prv_PolicyKey (policyKeyId);
	
	ALTER TABLE prv_cnf_SoftwarePackage ADD CONSTRAINT FK_prv_cnf_SoftwarePackage_prv_SoftwarePackage
	        FOREIGN KEY (packageId) REFERENCES prv_SoftwarePackage (packageId);
	
	ALTER TABLE prv_PolicyKey ADD CONSTRAINT FK_prv_PolicyKey_prv_PolicyFile
	        FOREIGN KEY (policyFileId) REFERENCES prv_PolicyFile (policyFileId);
	
	ALTER TABLE _mops_EonQueue ADD CONSTRAINT FK__mopsEonQueue_MovingObject
	        FOREIGN KEY (movingObjectId,movingObjectVersion) REFERENCES MovingObject (movingObjectId,movingObjectVersion);
	
	ALTER TABLE _mops_MoidQueue ADD CONSTRAINT FK__mops_MoidQueue_MovingObject
	        FOREIGN KEY (movingObjectId,movingObjectVersion) REFERENCES MovingObject (movingObjectId,movingObjectVersion);
	
	ALTER TABLE mops_SSM ADD CONSTRAINT FK_mopsSSM_mopsSSMDesc
	        FOREIGN KEY (ssmDescId) REFERENCES mops_SSMDesc (ssmDescId);
	
	ALTER TABLE mops_Tracklet ADD CONSTRAINT FK_mopsTracklets_mopsSSM
	        FOREIGN KEY (ssmId) REFERENCES mops_SSM (ssmId);
	
	ALTER TABLE Source_pt2 ADD CONSTRAINT FK_Source_Object
	        FOREIGN KEY (objectId) REFERENCES Object (objectId);
