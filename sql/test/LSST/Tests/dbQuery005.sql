-- http://dev.lsstcorp.org/trac/wiki/dbQuery005 
SELECT objectId
FROM   Alert
JOIN   _Alert2Type USING (alertId)
JOIN   AlertType USING (alertTypeId)
WHERE  alertTypeDescr = 'newTransients'
  AND  Alert.timeGenerated BETWEEN 1 AND 2;
