set mapred.cache.files hdfs:///user/hdfs/US.txt#US.txt;
SET mapred.createsymlink YES;
REGISTER hdfs:///user/hdfs/reversegeocoding.py USING jython AS reversegeocoding;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
chicago_data = load 'hdfs:///user/hdfs/Crimes_-_2001_to_present.csv' USING CSVLoader(',') AS (ID:chararray,Case_Number:chararray,Date:chararray,Block:chararray,IUCR:chararray,Primary_Type:chararray,Description:chararray,Location_Description:chararray,Arrest:chararray,Domestic:chararray,Beat:chararray,District:chararray,Ward:chararray,Community_Area:chararray,FBI_Code:chararray,X_Coordinate:chararray,Y_Coordinate:chararray,Year:chararray,Updated_On:chararray,Latitude:chararray,Longitude:chararray,Location:bytearray);
enriched_data = foreach chicago_data generate *, reversegeocoding.zipcode_lookup(Location) as zipcodeinfo:bag{(zipcode:chararray,city:chararray,state:chararray)};
final_data = foreach enriched_data generate ID..Location, FLATTEN(BagToTuple(zipcodeinfo));
store final_data into 'chicago_enriched_data' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');
