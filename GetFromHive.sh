


# hive -e "create table if not exists topcards (card string, count int)
# row format delimited fields terminated by '\001'
# lines terminated by '\n';

# insert overwrite table topcards
# select pri_acct_no, count(*) as count
# from tbl_common_his_trans
# where trans_id='S22' and pdate>=20150101 and pdate<=20151231
# group by pri_acct_no
# having count(*) > 200
# order by count desc
# "


hive -e "set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=2048000000;
set mapred.min.split.size.per.node=2048000000;
set mapred.min.split.size.per.rack=2048000000;
set hive.exec.max.created.files=200000;

insert overwrite directory 'xrli/GetFromHive'
select * from(
select t1.pri_acct_no, t1.trans_at,t1.tfr_dt_tm
from tbl_common_his_trans t1
left semi join(
select card, count from topcards
order by count desc
limit 5000
)t2
on t1.pri_acct_no=t2.card
where t1.trans_id='S22' and t1.pdate>=20150101 and t1.pdate<=20151231
distribute by t1.pri_acct_no
sort by t1.pri_acct_no,t1.tfr_dt_tm
) tmp2;"