SELECT DISTINCT segment0.id
FROM VxSegment_tab segment0, 
   VxSegmentation_tab segmentation0, 
   VxSegmentStringFeature_tab feat0_0
WHERE (segment0.segmentationId = segmentation0.id) AND
   (segmentation0.name = 'demo') AND
   (feat0_0.segmentId = segment0.id) AND
   (feat0_0.fieldName = 'team') AND
   (feat0_0.value = 'Feyenoord');

