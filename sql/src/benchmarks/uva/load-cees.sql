
COPY 50000 RECORDS INTO VxSegmentation_tab from 'PWD/cees/VxSegmentation_tab.txt' USING DELIMITERS ',','\n';
COPY 50000 RECORDS INTO VxSegment_tab from 'PWD/cees/VxSegment_tab.txt' USING DELIMITERS ',','\n';
COPY 50000 RECORDS INTO VxSegmentStringFeature_tab from 'PWD/cees/VxSegmentStringFeature_tab.txt' USING DELIMITERS ',','\n';
COPY 50000 RECORDS INTO VxSegmentDoubleFeature_tab from 'PWD/cees/VxSegmentDoubleFeature_tab.txt' USING DELIMITERS ',','\n';
COPY 50000 RECORDS INTO VxSegmentIntFeature_tab from 'PWD/cees/VxSegmentIntFeature_tab.txt' USING DELIMITERS ',','\n';
