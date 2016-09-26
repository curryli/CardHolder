insert overwrite directory 'xrli/GetFromHive'
select * from(
select t1.pri_acct_no, t1.trans_at,t1.tfr_dt_tm
from tbl_common_his_trans t1
left semi join(

select pri_acct_no, count(*) as count
from tbl_common_his_trans
where trans_id='S33' and pdate>=20150301 and pdate<=20150530
group by pri_acct_no
order by count desc
limit 1000

) t2
on t1.pri_acct_no=t2.pri_acct_no
where trans_id='S33' and pdate>=20150301 and pdate<=20150530
order by t1.pri_acct_no,t1.tfr_dt_tm
) tmp2;