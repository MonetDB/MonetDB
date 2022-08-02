-- http://dev.lsstcorp.org/trac/wiki/dbQuery041
-- Michael Strauss <strauss@astro.princeton.edu>
-- For all galaxies with r_Petro < 18, not saturated, not bright, not
-- edge, give me those whose centers are appreciably bluer than their
-- outer parts. That is, define the center color as: u_psf - g_psf And
-- define the outer color as: u_model - g_model Give me all objects
-- which have (u_model - g_model) - (u_psf - g_psf) < -0.4

DECLARE @flags  BIGINT;
SET @flags =    dbo.fPhotoFlags('SATURATED') +
                dbo.fPhotoFlags('BRIGHT')    +
                dbo.fPhotoFlags('EDGE') 
SELECT colc_u, colc_g,  objID       --or whatever you want from each object
FROM  Galaxy
WHERE (Flags &  @flags )= 0  
and petroRad_r < 18;

