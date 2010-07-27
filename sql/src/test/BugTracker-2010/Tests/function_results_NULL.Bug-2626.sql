CREATE SEQUENCE "seq_frequencybands" AS INTEGER;

CREATE TABLE frequencybands (
  freqbandid INT NOT NULL DEFAULT NEXT VALUE FOR "seq_frequencybands",
  freq_low DOUBLE DEFAULT NULL,
  freq_high DOUBLE DEFAULT NULL,
  PRIMARY KEY (freqbandid)
);

INSERT INTO frequencybands
    (freq_low
    ,freq_high
    ) 
VALUES 
    (30000000 - 900000 / 2
    ,30000000 + 900000 / 2
    )
    ,
    (34000000 - 960000 / 2
    ,34000000 + 960000 / 2
    )
    ,
    (38000000 - 1040000 / 2
    ,38000000 + 1040000 / 2
    )
    ,
    (42000000 - 1100000 / 2
    ,42000000 + 1100000 / 2
    )
    ,
    (120000000 - 350000 / 2
    ,120000000 + 350000 / 2
    )
    ,
    (130000000 - 450000 / 2
    ,130000000 + 450000 / 2
    )
    ,
    (140000000 - 550000 / 2
    ,140000000 + 550000 / 2
    )
    ,
    (150000000 - 700000 / 2
    ,150000000 + 700000 / 2
    )
    ,
    (160000000 - 850000 / 2
    ,160000000 + 850000 / 2
    )
    ,
    (170000000 - 1100000 / 2
    ,170000000 + 1100000 / 2
    )
    ,
    (350000000 - 100000000 / 2
    ,350000000 + 100000000 / 2
    )
    ,
    (640000000 - 100000000 / 2
    ,640000000 + 100000000 / 2
    )
    ,
    (850000000 - 100000000 / 2
    ,850000000 + 100000000 / 2
    )
    ,
    (1400000000 - 260000000 / 2
    ,1400000000 + 260000000 / 2
    )
    ,
    (2300000000 - 250000000 / 2
    ,2300000000 + 250000000 / 2
    )
    ,
    (4800000000 - 250000000 / 2
    ,4800000000 + 250000000 / 2
    )
    ,
    (8500000000 - 250000000 / 2
    ,8500000000 + 250000000 / 2
    )
  ;

CREATE FUNCTION getBand(ifreq_eff DOUBLE) RETURNS INT
BEGIN
  
  DECLARE nfreqbandid, ifreqbandid, ofreqbandid INT;
  DECLARE ibandwidth DOUBLE;

  /* For now, we default the bandwidth of a new band to 10MHz */
  SET ibandwidth = 10000000;

  set nfreqbandid = (
    SELECT COUNT(*)
      FROM frequencybands
     WHERE freq_low <= ifreq_eff
       AND freq_high >= ifreq_eff 
  );
  
  IF nfreqbandid = 1 THEN
  set  ifreqbandid = ( SELECT freqbandid
      FROM frequencybands
     WHERE freq_low <= ifreq_eff
       AND freq_high >= ifreq_eff
   ) ;
  ELSE
    set ifreqbandid = (SELECT NEXT VALUE FOR seq_frequencybands);
    INSERT INTO frequencybands
      (freqbandid
      ,freq_low
      ,freq_high
      ) VALUES
      (ifreqbandid
      ,ifreq_eff - (ibandwidth / 2)
      ,ifreq_eff + (ibandwidth / 2)
      )
    ;
  END IF;

  SET ofreqbandid = ifreqbandid;
  RETURN ofreqbandid;

END;

select getBand(300000000.0);

drop function getBand;
drop table frequencybands;
-- drop sequence "seq_frequencybands";
