START TRANSACTION;

ALTER TABLE "Match"  ADD  CONSTRAINT fk_Match_matchHead_MatchHead_obj FOREIGN KEY(matchHead)
REFERENCES MatchHead (objID)
;
ALTER TABLE "Match"  ADD  CONSTRAINT fk_Match_objID1_PhotoObjAll_objI FOREIGN KEY(objID1)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE SpecObjAll  ADD  CONSTRAINT fk_SpecObjAll_plateID_PlateX_pla FOREIGN KEY(plateID)
REFERENCES PlateX (plateID)
;
ALTER TABLE HoleObj  ADD  CONSTRAINT fk_HoleObj_plateID_PlateX_plateI FOREIGN KEY(plateID)
REFERENCES PlateX (plateID)
;
ALTER TABLE SpecPhotoAll  ADD  CONSTRAINT fk_SpecPhotoAll_specObjID_SpecOb FOREIGN KEY(specObjID)
REFERENCES SpecObjAll (specObjID)
;
ALTER TABLE QuasarCatalog  ADD  CONSTRAINT fk_QuasarCatalog_specObjID_SpecO FOREIGN KEY(specObjId)
REFERENCES SpecObjAll (specObjID)
;
ALTER TABLE SpecLineIndex  ADD  CONSTRAINT fk_SpecLineIndex_specobjID_SpecO FOREIGN KEY(specobjID)
REFERENCES SpecObjAll (specObjID)
;
ALTER TABLE SpecLineAll  ADD  CONSTRAINT fk_SpecLineAll_specObjID_SpecObj FOREIGN KEY(specobjID)
REFERENCES SpecObjAll (specObjID)
;
ALTER TABLE XCRedshift  ADD  CONSTRAINT fk_XCRedshift_specObjID_SpecObjA FOREIGN KEY(specObjID)
REFERENCES SpecObjAll (specObjID)
;
ALTER TABLE ELRedShift  ADD  CONSTRAINT fk_ELRedshift_specObjID_SpecObjA FOREIGN KEY(specObjID)
REFERENCES SpecObjAll (specObjID)
;
ALTER TABLE TargetInfo  ADD  CONSTRAINT fk_TargetInfo_targetID_Target_ta FOREIGN KEY(targetID)
REFERENCES Target (targetID)
;
ALTER TABLE TiledTargetAll  ADD  CONSTRAINT fk_TiledTargetAll_tile_TileAll_t FOREIGN KEY(tile)
REFERENCES TileAll (tile)
;
ALTER TABLE PlateX  ADD  CONSTRAINT fk_PlateX_tile_TileAll_tile FOREIGN KEY(tile)
REFERENCES TileAll (tile)
;
ALTER TABLE Sector2Tile  ADD  CONSTRAINT fk_Sector2Tile_regionID_Region_r FOREIGN KEY(regionID)
REFERENCES Region (regionid)
;
ALTER TABLE Sector2Tile  ADD  CONSTRAINT fk_Sector2Tile_tile_TileAll_tile FOREIGN KEY(tile)
REFERENCES TileAll (tile)
;
ALTER TABLE TilingInfo  ADD  CONSTRAINT fk_TilingInfo_tileRun_TilingRun_ FOREIGN KEY(tileRun)
REFERENCES TilingRun (tileRun)
;
ALTER TABLE TileAll  ADD  CONSTRAINT fk_TileAll_tileRun_TilingRun_til FOREIGN KEY(tileRun)
REFERENCES TilingRun (tileRun)
;
ALTER TABLE TilingNote  ADD  CONSTRAINT fk_TilingNote_tileRun_TilingRun_ FOREIGN KEY(tileRun)
REFERENCES TilingRun (tileRun)
;
ALTER TABLE TilingGeometry  ADD  CONSTRAINT fk_TilingGeometry_stripe_StripeD FOREIGN KEY(stripe)
REFERENCES StripeDefs (stripe)
;
ALTER TABLE TilingGeometry  ADD  CONSTRAINT fk_TilingGeometry_tileRun_Tiling FOREIGN KEY(tileRun)
REFERENCES TilingRun (tileRun)
;
ALTER TABLE RegionConvex  ADD  CONSTRAINT fk_RegionConvex_regionID_Region_ FOREIGN KEY(regionID)
REFERENCES Region (regionid)
;
ALTER TABLE Region2Box  ADD  CONSTRAINT fk_Region2Box_boxID_Region_regio FOREIGN KEY(boxid)
REFERENCES Region (regionid)
;
ALTER TABLE Sector  ADD  CONSTRAINT fk_Sector_regionID_Region_region FOREIGN KEY(regionID)
REFERENCES Region (regionid)
;
ALTER TABLE RegionArcs  ADD  CONSTRAINT fk_RegionArcs_regionID_Region_re FOREIGN KEY(regionid)
REFERENCES Region (regionid)
;
ALTER TABLE HalfSpace  ADD  CONSTRAINT fk_HalfSpace_regionID_Region_reg FOREIGN KEY(regionid)
REFERENCES Region (regionid)
;
ALTER TABLE Inventory  ADD  CONSTRAINT fk_Inventory_name_DBObjects_name FOREIGN KEY(name)
REFERENCES DBObjects (name)
;
ALTER TABLE DBColumns  ADD  CONSTRAINT fk_DBColumns_tablename_DBObjects FOREIGN KEY(tablename)
REFERENCES DBObjects (name)
;
ALTER TABLE DBViewCols  ADD  CONSTRAINT fk_DBViewCols_viewname_DBObjects FOREIGN KEY(viewname)
REFERENCES DBObjects (name)
;
ALTER TABLE IndexMap  ADD  CONSTRAINT fk_IndexMap_tableName_DBObjects_ FOREIGN KEY(tableName)
REFERENCES DBObjects (name)
;
ALTER TABLE BestTarget2Sector  ADD  CONSTRAINT fk_BestTarget2Sector_objID_Photo FOREIGN KEY(objID)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE BestTarget2Sector  ADD  CONSTRAINT fk_BestTarget2Sector_regionID_Se FOREIGN KEY(regionID)
REFERENCES Sector (regionID)
;
ALTER TABLE Segment  ADD  CONSTRAINT fk_Segment_chunkId_Chunk_chunkId FOREIGN KEY(chunkID)
REFERENCES Chunk (chunkID)
;
ALTER TABLE Segment  ADD  CONSTRAINT fk_Segment_stripe_StripeDefs_str FOREIGN KEY(stripe)
REFERENCES StripeDefs (stripe)
;
ALTER TABLE Field  ADD  CONSTRAINT fk_Field_segmentID_Segment_segme FOREIGN KEY(segmentID)
REFERENCES Segment (segmentID)
;
ALTER TABLE PhotoObjAll  ADD  CONSTRAINT fk_PhotoObjAll_fieldID_Field_fie FOREIGN KEY(fieldID)
REFERENCES Field (fieldID)
;
ALTER TABLE RunQA  ADD  CONSTRAINT fk_RunQA_fieldID_Field_fieldID FOREIGN KEY(fieldID)
REFERENCES Field (fieldID)
;
ALTER TABLE FieldProfile  ADD  CONSTRAINT fk_FieldProfile_fieldID_Field_fi FOREIGN KEY(fieldID)
REFERENCES Field (fieldID)
;
ALTER TABLE PhotoTag  ADD  CONSTRAINT fk_PhotoTag_fieldID_Field_fieldI FOREIGN KEY(fieldID)
REFERENCES Field (fieldID)
;
ALTER TABLE PhotoTag  ADD  CONSTRAINT fk_PhotoTag_objID_PhotoObjAll_ob FOREIGN KEY(objID)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE Frame  ADD  CONSTRAINT fk_Frame_fieldID_Field_fieldID FOREIGN KEY(fieldID)
REFERENCES Field (fieldID)
;
ALTER TABLE ObjMask  ADD  CONSTRAINT fk_ObjMask_objID_PhotoObjAll_obj FOREIGN KEY(objID)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE MatchHead  ADD  CONSTRAINT fk_MatchHead_objID_PhotoObjAll_o FOREIGN KEY(objID)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE MaskedObject  ADD  CONSTRAINT fk_MaskedObject_maskID_Mask_mask FOREIGN KEY(maskID)
REFERENCES Mask (maskID)
;
ALTER TABLE MaskedObject  ADD  CONSTRAINT fk_MaskedObject_objID_PhotoObjAl FOREIGN KEY(objid)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE Neighbors  ADD  CONSTRAINT fk_Neighbors_objID_PhotoObjAll_o FOREIGN KEY(objID)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE Zone  ADD  CONSTRAINT fk_Zone_objID_PhotoObjAll_objID FOREIGN KEY(objID)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE USNOB  ADD  CONSTRAINT fk_USNOB_objID_PhotoObjAll_objID FOREIGN KEY(objid)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE USNO  ADD  CONSTRAINT fk_USNO_objID_PhotoObjAll_objID FOREIGN KEY(objID)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE PhotoAuxAll  ADD  CONSTRAINT fk_PhotoAuxAll_objID_PhotoObjAll FOREIGN KEY(objid)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE Photoz  ADD  CONSTRAINT fk_Photoz_objID_PhotoObjAll_objI FOREIGN KEY(objID)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE First  ADD  CONSTRAINT fk_First_objID_PhotoObjAll_objID FOREIGN KEY(objID)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE Rosat  ADD  CONSTRAINT fk_Rosat_objID_PhotoObjAll_objID FOREIGN KEY(objID)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE PhotoProfile  ADD  CONSTRAINT fk_PhotoProfile_objID_PhotoObjAl FOREIGN KEY(objID)
REFERENCES PhotoObjAll (objID)
;
ALTER TABLE FileGroupMap  ADD  CONSTRAINT fk_FileGroupMap_tableFileGroup_P FOREIGN KEY(tableFileGroup)
REFERENCES PartitionMap (fileGroupName)
;
ALTER TABLE Chunk  ADD  CONSTRAINT fk_Chunk_stripe_StripeDefs_strip FOREIGN KEY(stripe)
REFERENCES StripeDefs (stripe)
;

COMMIT;
