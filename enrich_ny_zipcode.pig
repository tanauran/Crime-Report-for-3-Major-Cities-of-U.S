set mapred.cache.files hdfs:///user/hdfs/US.txt#US.txt;
SET mapred.createsymlink YES;
REGISTER hdfs:///user/hdfs/reversegeocoding.py USING jython AS reversegeocoding;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
ny_data = load 'hdfs:///user/hdfs/NYPD_Complaint_Data_Historic.csv' USING CSVLoader(',') AS (CMPLNT_NUM:chararray,CMPLNT_FR_DT:chararray,CMPLNT_FR_TM:chararray,CMPLNT_TO_DT:chararray,CMPLNT_TO_TM:chararray,RPT_DT:chararray,KY_CD:chararray,OFNS_DESC:chararray,PD_CD:chararray,PD_DESC:chararray,CRM_ATPT_CPTD_CD:chararray,LAW_CAT_CD:chararray,JURIS_DESC:chararray,BORO_NM:chararray,ADDR_PCT_CD:chararray,LOC_OF_OCCUR_DESC:chararray,PREM_TYP_DESC:chararray,PARKS_NM:chararray,HADEVELOPT:chararray,X_COORD_CD:chararray,Y_COORD_CD:chararray,Latitude:chararray,Longitude:chararray,Lat_Lon:bytearray);
enriched_data = foreach ny_data generate *, reversegeocoding.zipcode_lookup(Lat_Lon) as zipcodeinfo:bag{(zipcode:chararray,city:chararray,state:chararray)};
final_data = foreach enriched_data generate CMPLNT_NUM..Lat_Lon, FLATTEN(BagToTuple(zipcodeinfo));
store final_data into 'ny_enriched_data' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');
