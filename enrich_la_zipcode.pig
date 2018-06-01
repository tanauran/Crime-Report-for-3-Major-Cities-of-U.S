set mapred.cache.files hdfs:///user/hdfs/US.txt#US.txt;
SET mapred.createsymlink YES;
REGISTER hdfs:///user/hdfs/reversegeocoding.py USING jython AS reversegeocoding;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
la_data = load 'hdfs:///user/hdfs/Crime_Data_from_2010_to_Present.csv' USING CSVLoader(',') AS (dr_no:chararray, date_rptd:chararray,date_occ:chararray, time_occ:chararray, area_id:chararray, area_name:chararray, rpt_dist_no:chararray,crm_cd:chararray,crm_cd_desc:chararray,mocodes:chararray,vict_age:chararray,vict_sex:chararray,vict_descent:chararray,premis_cd:chararray,premis_desc:chararray,weapon_used_cd:chararray,weapon_desc:chararray,status:chararray,status_desc:chararray,crm_cd_1:chararray,crm_cd_2:chararray,crm_cd_3:chararray,crm_cd_4:chararray,location:chararray,cross_street:chararray,location_1:bytearray);
enriched_data = foreach la_data generate *, reversegeocoding.zipcode_lookup(location_1) as zipcodeinfo:bag{(zipcode:chararray,city:chararray,state:chararray)};
final_data = foreach enriched_data generate dr_no..location_1, FLATTEN(BagToTuple(zipcodeinfo));
store final_data into 'la_enriched_data' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');
