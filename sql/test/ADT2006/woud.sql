START TRANSACTION;

-- Victim / scene
CREATE TABLE victim (
    name varchar(100),
    murderdate varchar(100),
    dateofbirth varchar(100),
    eyes varchar(100),
    features varchar(1000),
    hair varchar(100),
    length varchar(100),
    location varchar(100),
    picture varchar(100)
);

CREATE TABLE victimtimeline (
    victim varchar(100),
    time varchar(100),
    event varchar(1000)
);

CREATE TABLE scenedoctors (
    victim varchar(100),
    doctor varchar(100)
);

CREATE TABLE sceneinspectors (
    victim varchar(100),
    inspector varchar(100)
);

CREATE TABLE scenesuspects (
    victim varchar(100),
    suspect varchar(100)
);

CREATE TABLE scenewitnesses (
    victim varchar(100),
    witness varchar(100)
);

-- Suspect table
CREATE TABLE suspect (
    name varchar(100),
    picture varchar(100)
);

CREATE TABLE suspectnotes (
    suspect varchar(100),
    notes varchar(1000)
);

CREATE TABLE suspectvictims (
    suspect varchar(100),
    victims varchar(100)
);

-- Inspector: case has only one victim, so include it here
CREATE TABLE inspector (
    name varchar(100),
    picture varchar(100),
    casevictim varchar(100)
);

CREATE TABLE inspectorsuspects (
    inspector varchar(100),
    suspect varchar(100)
);

-- Doctor
CREATE TABLE doctor (
    name varchar(100),
    picture varchar(100)
);

-- Witnesses
CREATE TABLE witness (
    name varchar(100),
    time varchar(100),
    appearance varchar(1000),
    diction varchar(100)
);

COMMIT;


-- We already have these in one table, handy!
SELECT * FROM sceneinspectors;

-- Now we need to join
SELECT inspector.name, victim.name 
    FROM inspector, victim, sceneinspectors 
    WHERE inspector.name = sceneinspectors.inspector 
        AND sceneinspectors.victim = victim.name;

-- We can't be as specific here, as LIKE is not as powerful as a regexp
SELECT CAST(substring(murderdate,1,4) AS integer) - CAST(substring(dateofbirth,1,4) AS integer) 
    FROM victim 
    WHERE murderdate LIKE '1___%' 
        AND dateofbirth LIKE '1___%';

-- Problem: GROUP BY removes 0-rows for victim/inspector join, so we union
SELECT AVG(num) 
    FROM (SELECT COUNT(victim) AS num 
            FROM sceneinspectors 
            GROUP BY victim 
          UNION ALL
          SELECT 0 AS num 
            FROM victim 
            WHERE name NOT IN 
                (SELECT victim FROM sceneinspectors)
         ) AS numbers;

SELECT name,features 
    FROM victim 
    WHERE features LIKE '%teeth%' 
        AND features LIKE '%missing%';

-- Date continues to be a problem as it is not in one format (and therefore not castable to 'date' type)
SELECT MIN(murderdate),MAX(murderdate) 
    FROM victim 
    WHERE murderdate LIKE '1%';

-- The names of the doctors
SELECT DISTINCT(doctor) 
    FROM scenedoctors;

-- Names of suspects, victims, inspectors and doctors
SELECT DISTINCT(name) FROM (
    SELECT name FROM suspect 
    UNION 
    SELECT name FROM victim 
    UNION 
    SELECT name FROM inspector 
    UNION 
    SELECT doctor AS name FROM scenedoctors
) AS allnames 
ORDER BY name;

DROP TABLE victim;
DROP TABLE victimtimeline;
DROP TABLE scenedoctors;
DROP TABLE sceneinspectors;
DROP TABLE scenesuspects;
DROP TABLE scenewitnesses;
DROP TABLE suspect;
DROP TABLE suspectnotes;
DROP TABLE suspectvictims;
DROP TABLE inspector;
DROP TABLE inspectorsuspects;
DROP TABLE doctor;
DROP TABLE witness;
