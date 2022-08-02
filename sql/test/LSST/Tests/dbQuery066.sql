-- http://dev.lsstcorp.org/trac/wiki/dbQuery066
/* This is the query that I use to return info on Extremely Red
Objects.  I get the filenames and extnums to make postage stamps. */
/* The nested subquery is broken down at the bottom. */

SELECT ls.sourceid,ls.framesetid,T5.minSep,ls.ra,ls.dec,
  rtrim(substring
   (mfy.filename,charindex("w2",mfy.filename,1),32))
   AS yfilename,
  rtrim(substring
   (mfj.filename,charindex("w2",mfj.filename,-1),32))
   AS j_1filename,
  rtrim(substring
   (mfh.filename,charindex("w2",mfh.filename,-1),32))
   AS hfilename,
  rtrim(substring
   (mfk.filename,charindex("w2",mfk.filename,-1),32))
   AS kfilename,
  lml.yenum AS yextnum,
  lml.j_1enum AS j_1extnum,
  lml.henum AS hextnum,
  lml.kenum AS kextnum,
  ls.yapermag3,
  ls.j_1apermag3,
  ls.hapermag3,
  ls.kapermag3,
  ls.j_1mhPnt + ls.hmkPnt as j_1mkPnt,
  /* compute the 5sigma depth as in dye 2006 */
  mfdy.PhotZPCat-2.5*LOG10(5.0*mfdy.skyNoise*SQRT(1.2*3.141593)/(cay.xPixSize*mfy.expTime))-mfdy.AperCor3 AS y_depth_dye2006,
  mfdj.PhotZPCat-2.5*LOG10(5.0*mfdj.skyNoise*SQRT(1.2*3.141593)/(caj.xPixSize*mfj.expTime))-mfdj.AperCor3 AS j_depth_dye2006,
  mfdh.PhotZPCat-2.5*LOG10(5.0*mfdh.skyNoise*SQRT(1.2*3.141593)/(cah.xPixSize*mfh.expTime))-mfdh.AperCor3 AS h_depth_dye2006,
  mfdk.PhotZPCat-2.5*LOG10(5.0*mfdk.skyNoise*SQRT(1.2*3.141593)/(cak.xPixSize*mfk.expTime))-mfdk.AperCor3 AS k_depth_dye2006

FROM

/* get the neighbour count and nearest neigbbour using subquery */
/* I want to exclude objects which have a close neighbour */
/* I have detailed how this is constructed at the bottom */

(SELECT sourceID,T4.masterobjID,T4.numNeighbs,T4.minSep,T4.slaveObjID,
        T4.master_framesetid,T4.master_priorsec,
        T4.slave_framesetid,T4.slave_priorsec
 FROM lasYJHKsource LEFT JOIN
      (SELECT T3.masterobjID,T3.numNeighbs,T3.minSep,T3.slaveObjID,
              T3.master_framesetid,T3.master_priorsec,
              ls1.framesetid AS slave_framesetid,
              ls1.priorsec AS slave_priorsec
       FROM lasSource AS ls1 INNER JOIN
            (SELECT sourceID,T1.masterObjID,T1.numNeighbs,T1.minSep,slaveObjID,
                    T1.master_framesetid,T1.master_priorsec 
             FROM (SELECT sourceID,T2.*,framesetid AS master_framesetid,
                          priorsec AS master_priorsec
                   FROM lasYJHKSource INNER JOIN 
                        (SELECT masterObjID,count(*) AS numNeighbs, 
                                MIN(distanceMins) AS minSep
                         FROM lasSourceNeighbours
                         GROUP BY masterObjID
                        ) AS T2 ON sourceID=T2.masterObjID
                  ) AS T1 INNER JOIN lasSourceNeighbours AS X
                    ON T1.sourceID=X.masterObjID AND T1.minSep=X.distanceMins
            ) AS T3 ON T3.slaveObjID=ls1.sourceID
      ) AS T4 ON sourceID=T4.masterobjID
) AS T5,

  LasYJHKSource AS ls,
  Lasdetection AS ldy,
  Lasdetection AS ldj,
  Lasdetection AS ldh,
  Lasdetection AS ldk,
  Lasmergelog AS lml,
  Multiframe AS mfy,
  Multiframe AS mfj,
  Multiframe AS mfh,
  Multiframe AS mfk,
  MultiframeDetector AS mfdy,
  MultiframeDetector AS mfdj,
  MultiframeDetector AS mfdh,
  MultiframeDetector AS mfdk,
  CurrentAstrometry AS cay,
  CurrentAstrometry AS caj,
  CurrentAstrometry AS cah,
  CurrentAstrometry AS cak

WHERE

  /* Exclude duplicates*/
  (ls.priorsec = 0 OR ls.priorsec = ls.framesetid) AND

  /* 12.5<K<17 and Kerr<0.1*/
  ls.kapermag3 BETWEEN 12.5 AND 17.0 AND
  ls.kapermag3err <= 0.1 AND

  /*Stellar in K-band*/
  ls.kclass = -1 AND

  /* J-K>2.5 including J-band dropouts*/
  ((ls.j_1apermag3-ls.kapermag3) >= 2.5 OR ls.j_1objid < 0) AND 

  /*Exclude edges. Define in arcseconds as EDR microstepped differently*/
  /*to later DRs. Can use pperrbits from DR2*/
  cak.xPIxSize*ldk.x > 18 AND cak.xPIxSize*ldk.x < 817 AND
  cak.yPIxSize*ldk.y > 18 AND cak.yPIxSize*ldk.y < 817 AND

  /*H-band detection required to exclude moving objects*/
  ls.hobjid>0 AND

  /* Objects must not be blue in Y-J*/
  (ls.yobjid < 0 OR (ls.yobjid > 0 AND ls.j_1objid > 0 AND ls.yapermag3 > ls.j_1apermag3)) AND

  /* Objects must not be blue in H-K*/
  ls.hapermag3 > ls.kapermag3 AND

  /* I get a cleaner sample if I use H-K>0.75 instead of the preceding two criteria*/

  /* Use pperrbits from DR2 onwards*/
  ls.yppErrBits < 65536 AND
  ls.j_1ppErrBits < 65536 AND
  ls.hppErrBits < 65536 AND
  ls.kppErrBits < 65536 AND

  /* Selected objects must have either no neighbours or none within 4" or */
  /* a nearest neighbour that is a duplicate of it */
  /* Where the nearest neighbour is a duplicate there could be other */
  /* neighbours within 4 arcseconds and these will be retained. These are */
  /* easier and quicker to exclude by eye than coding up */

  (T5.numneighbs IS NULL OR 
   T5.minSep >= 4.0/60.0 OR
   T5.slave_priorsec=T5.master_framesetid) AND

  T5.sourceid = ls.sourceid AND
  ls.framesetid = lml.framesetid AND

  /* For EDR to DR3 */
  (ldy.multiframeID=lml.ymfID OR ldy.multiframeID=-99999999) AND
  (ldy.extNum=lml.yenum OR ldy.extNum=0) AND
  ldy.seqNum=ls.ySeqNum AND
  (ldj.multiframeID=lml.j_1mfid OR ldj.multiframeID=-99999999) AND
  (ldj.extNum=lml.j_1enum OR ldj.extNum=0) AND
  ldj.seqNum=ls.j_1SeqNum AND
  ldh.multiframeID=lml.hmfid AND
  ldh.extNum=lml.henum AND
  ldh.seqNum=ls.hSeqNum AND
  ldk.multiframeID=lml.kmfid AND
  ldk.extNum=lml.kenum AND
  ldk.seqNum=ls.kSeqNum AND

  /* for DR4 only this becomes*/
  /*ldy.multiframeID=lml.ymfID AND
  ldy.extNum=lml.yenum AND
  ldy.seqNum=ls.ySeqNum AND
  ldj.multiframeID=lml.j_1mfid AND
  ldj.extNum=lml.j_1enum AND
  ldj.seqNum=ls.j_1SeqNum AND
  ldh.multiframeID=lml.hmfid AND
  ldh.extNum=lml.henum AND
  ldh.seqNum=ls.hSeqNum AND
  ldk.multiframeID=lml.kmfid AND
  ldk.extNum=lml.kenum AND
  ldk.seqNum=ls.kSeqNum AND*/

  lml.ymfID=mfdy.multiframeID AND
  lml.yeNum=mfdy.extNum AND
  lml.j_1mfID=mfdj.multiframeID AND
  lml.j_1eNum=mfdj.extNum AND
  lml.hmfID=mfdh.multiframeID AND
  lml.heNum=mfdh.extNum AND
  lml.kmfID=mfdk.multiframeID AND
  lml.keNum=mfdk.extNum AND
  mfy.MultiFrameID = mfdy.MultiFrameID AND
  mfj.MultiFrameID = mfdj.MultiFrameID AND
  mfh.MultiFrameID = mfdh.MultiFrameID AND
  mfk.MultiFrameID = mfdk.MultiFrameID AND
  cay.MultiFrameID = mfdy.MultiFrameID AND
  caj.MultiFrameID = mfdj.MultiFrameID AND
  cah.MultiFrameID = mfdh.MultiFrameID AND
  cak.MultiFrameID = mfdk.MultiFrameID AND
  cay.extNum = mfdy.extNum AND
  caj.extNum = mfdj.extNum AND
  cah.extNum = mfdh.extNum AND
  cak.extNum = mfdk.extNum
ORDER BY ls.ra;

/* For each object in lasSource with at least one neighbour (i.e. in */
/* lasSourceNeighbours), get the number of neighbours for each and */
/* distance to the nearest neighbour */

(SELECT masterObjID,count(*) AS numNeighbs, 
        MIN(distanceMins) AS minSep
 FROM lasSourceNeighbours
 GROUP BY masterObjID
) AS T2 ;

/* Do an inner join with lasYJHKsource to get the framesetid and */
/* priorsec for each masterobj with YJHK observations */

(SELECT sourceID,T2.*,framesetid AS master_framesetid,
        priorsec AS master_priorsec
 FROM lasYJHKSource INNER JOIN 
      (SELECT masterObjID,count(*) AS numNeighbs, 
              MIN(distanceMins) AS minSep
       FROM lasSourceNeighbours
       GROUP BY masterObjID
      ) AS T2 ON sourceID=T2.masterObjID
) AS T1;


/* Do another inner join with lasSourceNeighbours to get the */
/* slaveObjID of the nearest neighbour for each masterobj */

(SELECT sourceID,T1.masterObjID,T1.numNeighbs,T1.minSep,slaveObjID,
        T1.master_framesetid,T1.master_priorsec 
 FROM (SELECT sourceID,T2.*,framesetid AS master_framesetid,
              priorsec AS master_priorsec
       FROM lasYJHKSource INNER JOIN 
            (SELECT masterObjID,count(*) AS numNeighbs, 
                    MIN(distanceMins) AS minSep
             FROM lasSourceNeighbours
             GROUP BY masterObjID
            ) AS T2 ON sourceID=T2.masterObjID
      ) AS T1 INNER JOIN lasSourceNeighbours AS X
        ON T1.sourceID=X.masterObjID AND T1.minSep=X.distanceMins
) AS T3;

/* Do another inner join with lasSource to get the framesetid and */
/* priorsec for the nearest neighbour (slaveobj) */

(SELECT T3.masterobjID,T3.numNeighbs,T3.minSep,T3.slaveObjID,
        T3.master_framesetid,T3.master_priorsec,
        ls1.framesetid AS slave_framesetid,
        ls1.priorsec AS slave_priorsec
 FROM lasSource AS ls1 INNER JOIN
      (SELECT sourceID,T1.masterObjID,T1.numNeighbs,T1.minSep,slaveObjID,
              T1.master_framesetid,T1.master_priorsec 
       FROM (SELECT sourceID,T2.*,framesetid AS master_framesetid,
                    priorsec AS master_priorsec
             FROM lasYJHKSource INNER JOIN 
                  (SELECT masterObjID,count(*) AS numNeighbs, 
                          MIN(distanceMins) AS minSep
                   FROM lasSourceNeighbours
                   GROUP BY masterObjID
                  ) AS T2 ON sourceID=T2.masterObjID
            ) AS T1 INNER JOIN lasSourceNeighbours AS X
              ON T1.sourceID=X.masterObjID AND T1.minSep=X.distanceMins
      ) AS T3 ON T3.slaveObjID=ls1.sourceID
) AS T4;

/* Do lasYJHKsource LEFT JOIN with T4 to include info for YJHK objects */
/* with no neighbour within 10 arcsecs */

SELECT sourceID,T4.masterobjID,T4.numNeighbs,T4.minSep,T4.slaveObjID,
       T4.master_framesetid,T4.master_priorsec,
       T4.slave_framesetid,T4.slave_priorsec
FROM lasYJHKsource LEFT JOIN
     (SELECT T3.masterobjID,T3.numNeighbs,T3.minSep,T3.slaveObjID,
             T3.master_framesetid,T3.master_priorsec,
             ls1.framesetid AS slave_framesetid,
             ls1.priorsec AS slave_priorsec
      FROM lasSource AS ls1 INNER JOIN
           (SELECT sourceID,T1.masterObjID,T1.numNeighbs,T1.minSep,slaveObjID,
                   T1.master_framesetid,T1.master_priorsec 
            FROM (SELECT sourceID,T2.*,framesetid AS master_framesetid,
                         priorsec AS master_priorsec
                  FROM lasYJHKSource INNER JOIN 
                       (SELECT masterObjID,count(*) AS numNeighbs, 
                               MIN(distanceMins) AS minSep
                        FROM lasSourceNeighbours
                        GROUP BY masterObjID
                       ) AS T2 ON sourceID=T2.masterObjID
                 ) AS T1 INNER JOIN lasSourceNeighbours AS X
                   ON T1.sourceID=X.masterObjID AND T1.minSep=X.distanceMins
           ) AS T3 ON T3.slaveObjID=ls1.sourceID
     ) AS T4 ON sourceID=T4.masterobjID
) AS T5;






