-- http://dev.lsstcorp.org/trac/wiki/dbQuery042
SET @binned =   dbo.fPhotoFlags('BINNED1') +
                dbo.fPhotoFlags('BINNED2') +
                dbo.fPhotoFlags('BINNED4') 
SET @deblendedChild =   dbo.fPhotoFlags('BLENDED')   +
                        dbo.fPhotoFlags('NODEBLEND') +
                        dbo.fPhotoFlags('CHILD')
SET @blended = dbo.fPhotoFlags('BLENDED')
DECLARE @noPetro BIGINT
SET @noPetro = dbo.fPhotoFlags('NOPETRO')
DECLARE @tooLarge BIGINT
SET @tooLarge = dbo.fPhotoFlags('TOO_LARGE')
SET @saturated = dbo.fPhotoFlags('SATURATED')
SELECT run,
       camCol,
       rerun,
       field,
       objectId, 
       ra, 
       decl
FROM   Galaxy
WHERE  (flags &  @binned )> 0  
  AND (flags &  @deblendedChild ) !=  @blended
  AND (  (( flags & @noPetro = 0) 
  AND petroRad_i > 15)
        OR ((flags & @noPetro > 0) 
  AND petroRad_i > 7.5)
        OR ((flags & @tooLarge > 0) 
  AND petroRad_i > 2.5)
        OR --note, Gray changed this and to an or, becuase it did not make sense as an and.
                ((flags & @saturated  = 0 )
  AND petroRad_i > 17.5)
        ) ;

