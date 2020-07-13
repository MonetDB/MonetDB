plan
UPDATE fluxz
   SET (filter
       ,f_datapoints
       ,avg_flux
       ,avg_fluxsq
       ,avg_w
       ,avg_wflux
       ,avg_wfluxsq
       ,avg_dec_zone_deg
       )
       =
       (SELECT filter
              ,f_datapoints
              ,avg_flux
              ,avg_fluxsq
              ,avg_w
              ,avg_wflux
              ,avg_wfluxsq
              ,avg_dec_zone_deg
          FROM cm_flux 
         WHERE cm_flux.runcat = fluxz.runcat 
           AND cm_flux.active = TRUE 
           AND cm_flux.filter = 'g' 
           AND cm_flux.filter = fluxz.filter
       )
 WHERE EXISTS (SELECT runcat
                 FROM cm_flux
                WHERE cm_flux.runcat = fluxz.runcat 
                  AND cm_flux.active = TRUE 
                  AND cm_flux.filter = 'g' 
                  AND cm_flux.filter = fluxz.filter
              )
;
