CREATE external TABLE IF NOT EXISTS GeneralAPI_last3(
card string,
total_cnt int,
total_money double,
has_trans_date int, 
has_trans_month int,
avg_amt_m double,
mcc_tps int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
stored as textfile;

load data inpath 'hdfs://nameservice1/user/hdanaly/xrli/CardholderTag/GeneralAPI_out_last3' into table GeneralAPI_last3;



select percentile_approx(total_cnt,0.8),percentile_approx(total_money,0.8),percentile_approx(has_trans_date,0.8),
percentile_approx(has_trans_month,0.8),percentile_approx(avg_amt_m,0.8),percentile_approx(mcc_tps,0.8)
from GeneralAPI_last3 where total_cnt is not null;


4.953634742970056       9953.237135011155       6.0     3.0     1357.28250352537        3.4110638112690785



CREATE external TABLE IF NOT EXISTS General_valuable(
card string,
total_cnt int,
total_money double,
has_trans_date int, 
has_trans_month int,
avg_amt_m double,
mcc_tps int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
stored as textfile;

insert overwrite table General_valuable
select * from GeneralAPI_last3 where total_cnt>=5 and total_money>=10000 and has_trans_date>=6 and has_trans_month>=3 and avg_amt_m>=1358 and mcc_tps>=4;


select count(distinct(card)) from GeneralAPI_last3;
select count(*) from General_valuable;     65027540


select 
count(case when total_cnt=1 then 1 else null end),
count(case when total_cnt>1 and total_cnt<=5 then 1 else null end),
count(case when total_cnt>5 and total_cnt<=10 then 1 else null end),
count(case when total_cnt>10 and total_cnt<=15 then 1 else null end),
count(case when total_cnt>15 and total_cnt<=20 then 1 else null end),
count(case when total_cnt>20 and total_cnt<=30 then 1 else null end),
count(case when total_cnt>30 and total_cnt<=40 then 1 else null end),
count(case when total_cnt>40 and total_cnt<=50 then 1 else null end),
count(case when total_cnt>50 and total_cnt<=100 then 1 else null end),
count(case when total_cnt>100 then 1 else null end) from GeneralAPI_last3;
225892071       312304425       107763586       39505287        17932754        15064142      5853642  2856592 3878923 1179514


select 
count(case when total_money>0 and total_money<=100  then 1 else null end),
count(case when total_money>100 and total_money<=1000  then 1 else null end),
count(case when total_money>1000 and total_money<=10000  then 1 else null end),
count(case when total_money>10000 and total_money<=100000  then 1 else null end),
count(case when total_money>100000 and total_money<=1000000  then 1 else null end),
count(case when total_money>1000000 and total_money<=10000000  then 1 else null end),
count(case when total_money>10000000 and total_money<=100000000  then 1 else null end),
count(case when total_money>100000000 then 1 else null end)
from GeneralAPI_last3;
  
70326699        183936119       260489822       194902436       23235919        925831  21363 313



select 
count(case when money_avg>0 and money_avg<=100  then 1 else null end),
count(case when money_avg>100 and money_avg<=1000  then 1 else null end),
count(case when money_avg>1000 and money_avg<=10000  then 1 else null end),
count(case when money_avg>10000 and money_avg<=100000  then 1 else null end),
count(case when money_avg>100000 and money_avg<=1000000  then 1 else null end),
count(case when money_avg>1000000 and money_avg<=10000000  then 1 else null end),
count(case when money_avg>10000000 and money_avg<=100000000  then 1 else null end),
count(case when money_avg>100000000 then 1 else null end)
from (select total_money/total_cnt as money_avg from GeneralAPI_last3)A;

113159072       265774098       286854178       61429238        5211553 177919  0       0

#ALTER TABLE GeneralAPI_last3 ADD COLUMNS (avg_money double);



select 
count(case when has_trans_date=1 then 1 else null end),
count(case when has_trans_date>1 and has_trans_date<=5 then 1 else null end),
count(case when has_trans_date>5 and has_trans_date<=10 then 1 else null end),
count(case when has_trans_date>10 and has_trans_date<=15 then 1 else null end),
count(case when has_trans_date>15 and has_trans_date<=20 then 1 else null end),
count(case when has_trans_date>20 and has_trans_date<=30 then 1 else null end),
count(case when has_trans_date>30 and has_trans_date<=40 then 1 else null end),
count(case when has_trans_date>40 and has_trans_date<=50 then 1 else null end),
count(case when has_trans_date>50 and has_trans_date<=60 then 1 else null end),
count(case when has_trans_date>60 and has_trans_date<=70 then 1 else null end),
count(case when has_trans_date>70 and has_trans_date<=80 then 1 else null end),
count(case when has_trans_date>80 and has_trans_date<=90 then 1 else null end),
count(case when has_trans_date>90 and has_trans_date<=100 then 1 else null end),
count(case when has_trans_date>100 then 1 else null end) from GeneralAPI_last3;
362408716       441593484       134365740       44385890        19029797        15550561        5922836 2834573 1587356 969810  493428  280402  51163   0

select 
count(case when has_trans_month=1 then 1 else null end),
count(case when has_trans_month=2 then 1 else null end),
count(case when has_trans_month=3 then 1 else null end) from GeneralAPI_last3;
482970792       275082812       268426941
 
select 
count(case when avg_amt_m>0 and avg_amt_m<=100  then 1 else null end),
count(case when avg_amt_m>100 and avg_amt_m<=1000  then 1 else null end),
count(case when avg_amt_m>1000 and avg_amt_m<=10000  then 1 else null end),
count(case when avg_amt_m>10000 and avg_amt_m<=100000  then 1 else null end),
count(case when avg_amt_m>100000 and avg_amt_m<=1000000  then 1 else null end),
count(case when avg_amt_m>1000000 and avg_amt_m<=10000000  then 1 else null end),
count(case when avg_amt_m>10000000 and avg_amt_m<=100000000  then 1 else null end),
count(case when avg_amt_m>100000000 then 1 else null end) from GeneralAPI_last3;
33202037        103676800       159326024       84808289        4037228 187597  3654    43


select 
count(case when mcc_tps=1 then 1 else null end),
count(case when mcc_tps>1 and mcc_tps<=5 then 1 else null end),
count(case when mcc_tps>5 and mcc_tps<=10 then 1 else null end),
count(case when mcc_tps>10 and mcc_tps<=15 then 1 else null end),
count(case when mcc_tps>15 and mcc_tps<=20 then 1 else null end),
count(case when mcc_tps>20 and mcc_tps<=25 then 1 else null end),
count(case when mcc_tps>25 and mcc_tps<=30 then 1 else null end),
count(case when mcc_tps>30 and mcc_tps<=35 then 1 else null end),
count(case when mcc_tps>35 and mcc_tps<=40 then 1 else null end),
count(case when mcc_tps>40 and mcc_tps<=45 then 1 else null end),
count(case when mcc_tps>45 and mcc_tps<=50 then 1 else null end),
count(case when mcc_tps>50 then 1 else null end) from GeneralAPI_last3;

514168988       418422218       78131178        14173910        3190426 894230  308251  111141  41239   15689   6614    9872
















CREATE external TABLE IF NOT EXISTS QRAPI_last3(
card string,
QR_cnt int,
QR_cnt_ratio double,
QR_avg_RMB double,
QR_date_cnt int,
QR_mcc_tps int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
stored as textfile;

load data inpath 'hdfs://nameservice1/user/hdanaly/xrli/CardholderTag/ValueAPI_QRCode' into table QRAPI_last3;



select percentile_approx(QR_cnt,0.8),percentile_approx(QR_cnt_ratio,0.8),percentile_approx(QR_avg_RMB,0.8),
percentile_approx(QR_date_cnt,0.8),percentile_approx(QR_mcc_tps,0.8) from QRAPI_last3;

8.0     0.8516613288516828      105.77580697700424      5.0     1.0







































CREATE external TABLE IF NOT EXISTS Metro_related(
card string,
trans_at double,
tfr_dt_tm string,
extend_inf string,
mchnt_tp string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
stored as textfile;

load data inpath 'hdfs://nameservice1/user/hdanaly/xrli/CardholderTag/metro_related_Data' into table Metro_related;
 
CREATE external TABLE IF NOT EXISTS Metro_stat(
card string,
QR_cnt int,
QR_cnt_ratio double,
QR_avg_RMB double,
QR_date_cnt int,
QR_mcc_tps int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
stored as textfile;

load data inpath 'hdfs://nameservice1/user/hdanaly/xrli/CardholderTag/ValueAPI_Metro' into table Metro_stat;




所有的闪付交易
select * from Metro_related where substring(extend_inf,27,1)>='1' and substring(extend_inf,27,1)<='9';


闪付交易中的优质统计
CREATE external TABLE IF NOT EXISTS Metro_valuable(
card string,
trans_at double,
tfr_dt_tm string,
extend_inf string,
mchnt_tp string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
stored as textfile;

insert overwrite table Metro_valuable
select * from Metro_stat where QR_cnt>=15 and QR_cnt_ratio>=0.5 and QR_avg_RMB>=100 and QR_date_cnt>=10 and QR_mcc_tps>=3;
 
所有优质卡号个数
select count(*) from Metro_valuable;
/
#所有卡号个数
select count(distinct(card)) from Metro_stat;


所有优质卡号的闪付交易的笔数
select count(A.*) from (select * from Metro_related where substring(extend_inf,27,1)>='1' and substring(extend_inf,27,1)<='9')A
left semi join Metro_valuable on A.card=Metro_valuable.card;
/
所有的闪付交易笔数
select count(*) from Metro_related where substring(extend_inf,27,1)>='1' and substring(extend_inf,27,1)<='9';



所有优质卡号的闪付交易的总金额
select sum(A.trans_at)/100 from (select * from Metro_related where substring(extend_inf,27,1)>='1' and substring(extend_inf,27,1)<='9')A
left semi join Metro_valuable on A.card=Metro_valuable.card;
/
所有的闪付交易总金额
select sum(trans_at) from Metro_related where substring(extend_inf,27,1)>='1' and substring(extend_inf,27,1)<='9';


所有闪付交易中，每种闪付交易的类型分布
select pay_tp,count(*) from (select substring(extend_inf,27,1) as pay_tp from Metro_related where substring(extend_inf,27,1)>='1' and substring(extend_inf,27,1)<='9')A group by pay_tp;


所有优质卡号的闪付交易中，每种闪付交易的类型分布
select pay_tp,count(*) from (
select A.* from(
(select substring(extend_inf,27,1) as pay_tp from Metro_related where substring(extend_inf,27,1)>='1' and substring(extend_inf,27,1)<='9')A
left semi join Metro_valuable on A.card=Metro_valuable.card))B group by B.pay_tp;