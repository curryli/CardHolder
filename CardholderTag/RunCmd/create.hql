create external table tbl_cardholder_consumptionTag_raw_v2
(
card_num	 string,
md5		 string,
iss_ins_cd	 string,
iss_ins_name	 string,
card_level	 string,
cs_trx_amt_L6M	 double,
month_count      int,
avg_amt_L6M      double 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hdanaly/wangjun/cardholderTag/consumptionTag_v2'; 
