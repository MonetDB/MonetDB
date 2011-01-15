ALTER TABLE Ap7Mag  DROP  CONSTRAINT fk_Ap7Mag_objID_PhotoObjAll_objI 
;
ALTER TABLE BestTarget2Sector  DROP  CONSTRAINT fk_BestTarget2Sector_objID_Photo 
;
ALTER TABLE BestTarget2Sector  DROP  CONSTRAINT fk_BestTarget2Sector_regionID_Se 
;
ALTER TABLE Chunk  DROP  CONSTRAINT fk_Chunk_stripe_StripeDefs_strip 
;
ALTER TABLE DBColumns  DROP  CONSTRAINT fk_DBColumns_tablename_DBObjects 
;
ALTER TABLE DBViewCols  DROP  CONSTRAINT fk_DBViewCols_viewname_DBObjects 
;
ALTER TABLE DR3QuasarCatalog  DROP  CONSTRAINT fk_DR3QuasarCatalog_specObjID_Sp 
;
ALTER TABLE DR5QuasarCatalog  DROP  CONSTRAINT fk_DR5QuasarCatalog_specObjID_Sp 
;
ALTER TABLE ELRedShift  DROP  CONSTRAINT fk_ELRedshift_specObjID_SpecObjA 
;
ALTER TABLE Field  DROP  CONSTRAINT fk_Field_segmentID_Segment_segme 
;
ALTER TABLE FieldProfile  DROP  CONSTRAINT fk_FieldProfile_fieldID_Field_fi 
;
ALTER TABLE FileGroupMap  DROP  CONSTRAINT fk_FileGroupMap_tableFileGroup_P 
;
ALTER TABLE First  DROP  CONSTRAINT fk_First_objID_PhotoObjAll_objID 
;
ALTER TABLE Frame  DROP  CONSTRAINT fk_Frame_fieldID_Field_fieldID 
;
ALTER TABLE HalfSpace  DROP  CONSTRAINT fk_HalfSpace_regionID_Region_reg 
;
ALTER TABLE HoleObj  DROP  CONSTRAINT fk_HoleObj_plateID_PlateX_plateI 
;
ALTER TABLE IndexMap  DROP  CONSTRAINT fk_IndexMap_tableName_DBObjects_ 
;
ALTER TABLE Inventory  DROP  CONSTRAINT fk_Inventory_name_DBObjects_name 
;
ALTER TABLE MaskedObject  DROP  CONSTRAINT fk_MaskedObject_maskID_Mask_mask 
;
ALTER TABLE MaskedObject  DROP  CONSTRAINT fk_MaskedObject_objID_PhotoObjAl 
;
ALTER TABLE Match  DROP  CONSTRAINT fk_Match_matchHead_MatchHead_obj 
;
ALTER TABLE Match  DROP  CONSTRAINT fk_Match_objID1_PhotoObjAll_objI 
;
ALTER TABLE MatchHead  DROP  CONSTRAINT fk_MatchHead_objID_PhotoObjAll_o 
;
ALTER TABLE Neighbors  DROP  CONSTRAINT fk_Neighbors_objID_PhotoObjAll_o 
;
ALTER TABLE ObjMask  DROP  CONSTRAINT fk_ObjMask_objID_PhotoObjAll_obj 
;
ALTER TABLE PhotoObjAll  DROP  CONSTRAINT fk_PhotoObjAll_fieldID_Field_fie 
;
ALTER TABLE PhotoProfile  DROP  CONSTRAINT fk_PhotoProfile_objID_PhotoObjAl 
;
ALTER TABLE Photoz  DROP  CONSTRAINT fk_Photoz_objID_PhotoObjAll_objI 
;
ALTER TABLE Photoz2  DROP  CONSTRAINT fk_Photoz2_objID_PhotoObjAll_obj 
;
ALTER TABLE PlateX  DROP  CONSTRAINT fk_PlateX_tile_TileAll_tile 
;
ALTER TABLE QsoBest  DROP  CONSTRAINT fk_QsoBest_headObjID_QsoBunch_he 
;
ALTER TABLE QsoCatalogAll  DROP  CONSTRAINT fk_QsoCatalogAll_bestObjID_QsoBe 
;
ALTER TABLE QsoCatalogAll  DROP  CONSTRAINT fk_QsoCatalogAll_headObjID_QsoBu 
;
ALTER TABLE QsoCatalogAll  DROP  CONSTRAINT fk_QsoCatalogAll_specObjID_QsoSp 
;
ALTER TABLE QsoCatalogAll  DROP  CONSTRAINT fk_QsoCatalogAll_targetObjID_Qso 
;
ALTER TABLE QsoConcordanceAll  DROP  CONSTRAINT fk_QsoConcordanceAll_bestObjID_Q 
;
ALTER TABLE QsoConcordanceAll  DROP  CONSTRAINT fk_QsoConcordanceAll_headObjID_Q 
;
ALTER TABLE QsoConcordanceAll  DROP  CONSTRAINT fk_QsoConcordanceAll_specObjID_Q 
;
ALTER TABLE QsoConcordanceAll  DROP  CONSTRAINT fk_QsoConcordanceAll_targetObjID 
;
ALTER TABLE QsoSpec  DROP  CONSTRAINT fk_QsoSpec_headObjID_QsoBunch_he 
;
ALTER TABLE QsoTarget  DROP  CONSTRAINT fk_QsoTarget_headObjID_QsoBunch_ 
;
ALTER TABLE Region2Box  DROP  CONSTRAINT fk_Region2Box_boxID_Region_regio 
;
ALTER TABLE RegionArcs  DROP  CONSTRAINT fk_RegionArcs_regionID_Region_re 
;
ALTER TABLE RegionPatch  DROP  CONSTRAINT fk_RegionPatch_regionID_Region_r 
;
ALTER TABLE Rosat  DROP  CONSTRAINT fk_Rosat_objID_PhotoObjAll_objID 
;
ALTER TABLE RunQA  DROP  CONSTRAINT fk_RunQA_fieldID_Field_fieldID 
;
ALTER TABLE Sector  DROP  CONSTRAINT fk_Sector_regionID_Region_region 
;
ALTER TABLE Sector2Tile  DROP  CONSTRAINT fk_Sector2Tile_regionID_Region_r 
;
ALTER TABLE Sector2Tile  DROP  CONSTRAINT fk_Sector2Tile_tile_TileAll_tile 
;
ALTER TABLE Segment  DROP  CONSTRAINT fk_Segment_chunkId_Chunk_chunkId 
;
ALTER TABLE Segment  DROP  CONSTRAINT fk_Segment_stripe_StripeDefs_str 
;
ALTER TABLE SpecLineAll  DROP  CONSTRAINT fk_SpecLineAll_specObjID_SpecObj 
;
ALTER TABLE SpecLineIndex  DROP  CONSTRAINT fk_SpecLineIndex_specobjID_SpecO 
;
ALTER TABLE SpecObjAll  DROP  CONSTRAINT fk_SpecObjAll_plateID_PlateX_pla 
;
ALTER TABLE SpecPhotoAll  DROP  CONSTRAINT fk_SpecPhotoAll_specObjID_SpecOb 
;
ALTER TABLE TargetInfo  DROP  CONSTRAINT fk_TargetInfo_targetID_Target_ta 
;
ALTER TABLE TileAll  DROP  CONSTRAINT fk_TileAll_tileRun_TilingRun_til 
;
ALTER TABLE TiledTargetAll  DROP  CONSTRAINT fk_TiledTargetAll_tile_TileAll_t 
;
ALTER TABLE TilingGeometry  DROP  CONSTRAINT fk_TilingGeometry_stripe_StripeD 
;
ALTER TABLE TilingGeometry  DROP  CONSTRAINT fk_TilingGeometry_tileRun_Tiling 
;
ALTER TABLE TilingInfo  DROP  CONSTRAINT fk_TilingInfo_tileRun_TilingRun_ 
;
ALTER TABLE TilingNote  DROP  CONSTRAINT fk_TilingNote_tileRun_TilingRun_ 
;
ALTER TABLE UberCal  DROP  CONSTRAINT fk_UberCal_objID_PhotoObjAll_obj 
;
ALTER TABLE USNO  DROP  CONSTRAINT fk_USNO_objID_PhotoObjAll_objID 
;
ALTER TABLE XCRedshift  DROP  CONSTRAINT fk_XCRedshift_specObjID_SpecObjA 
;
ALTER TABLE Zone  DROP  CONSTRAINT fk_Zone_objID_PhotoObjAll_objID 
;
