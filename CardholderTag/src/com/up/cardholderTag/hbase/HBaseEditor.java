package com.up.cardholderTag.hbase;

import java.io.IOException; 
import java.util.ArrayList; 
import java.util.List; 
 


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration; 
import org.apache.hadoop.hbase.HColumnDescriptor; 
import org.apache.hadoop.hbase.HTableDescriptor; 
import org.apache.hadoop.hbase.KeyValue; 
import org.apache.hadoop.hbase.MasterNotRunningException; 
import org.apache.hadoop.hbase.ZooKeeperConnectionException; 
import org.apache.hadoop.hbase.client.Delete; 
import org.apache.hadoop.hbase.client.Get; 
import org.apache.hadoop.hbase.client.HBaseAdmin; 
import org.apache.hadoop.hbase.client.HTable; 
import org.apache.hadoop.hbase.client.HTablePool; 
import org.apache.hadoop.hbase.client.Put; 
import org.apache.hadoop.hbase.client.Result; 
import org.apache.hadoop.hbase.client.ResultScanner; 
import org.apache.hadoop.hbase.client.Scan; 
import org.apache.hadoop.hbase.filter.Filter; 
import org.apache.hadoop.hbase.filter.FilterList; 
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter; 
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp; 
import org.apache.hadoop.hbase.util.Bytes; 
 
public class HBaseEditor { 
 
    public static Configuration configuration; 
    private static final long WRITE_BUFFER_SIZE = 1024 * 1024 * 12;
    static { 
        configuration = HBaseConfiguration.create(); 
//        configuration.set("hbase.zookeeper.property.clientPort", "2181"); 
//        configuration.set("hbase.zookeeper.quorum", "172.16.24.135"); 
//        configuration.set("hbase.master", "172.16.24.135:60010"); 
    } 
 
    public static void main(String[] args) { 
    	//createTable("tbl_cardholder_sys_user"); 
        // insertData("wujintao"); 
    	//QueryAll("tbl_carTag_list"); 
    	QueryByCondition1("tbl_common_cardholder_car"); 
    	//insertUserData("tbl_cardholder_sys_user");
    	//insertData("tbl_common_cardholder_tag"); 
    	//insertData2("tbl_common_cardholder_car"); 
        // QueryByCondition1("wujintao"); 
        // QueryByCondition2("wujintao"); 
        //QueryByCondition3("wujintao"); 
        //deleteRow("wujintao","abcdef"); 
        //deleteByCondition("wujintao","abcdef"); 
    } 
 
     
    public static void createTable(String tableName) { 
        System.out.println("start create table ......"); 
        try { 
            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration); 
            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建 
                hBaseAdmin.disableTable(tableName); 
                hBaseAdmin.deleteTable(tableName); 
                System.out.println(tableName + " is exist,detele...."); 
            } 
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName); 
            tableDescriptor.addFamily(new HColumnDescriptor("d")); 
            hBaseAdmin.createTable(tableDescriptor); 
        } catch (MasterNotRunningException e) { 
            e.printStackTrace(); 
        } catch (ZooKeeperConnectionException e) { 
            e.printStackTrace(); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
        System.out.println("end create table ......"); 
    } 
    
    public static void insertUserData(String tableName) { 
        System.out.println("start insert data ......"); 
        //Put put = new Put("15370286001719373".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值 
        Put put = new Put("yjy-01".getBytes());
        
        put.add("d".getBytes(), "ins_cd".getBytes(), "00010000".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "user_pwd".getBytes(), "123456".getBytes());// 本行数据的第一列 
        try { 
        	HTable table = new HTable(configuration, tableName);
            table.put(put); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
        System.out.println("end insert data ......"); 
    } 
 
     
    public static void insertData(String tableName) { 
        System.out.println("start insert data ......"); 
        HTablePool pool = new HTablePool(configuration, 1000); 
        HTable table = (HTable) pool.getTable(tableName); 
        //Put put = new Put("15370286001719373".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值 
        Put put = new Put("166222530115357639".getBytes());
        
        put.add("d".getBytes(), "exist_travel".getBytes(), "1".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "rwy_tag".getBytes(), "2".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "plt_tag".getBytes(), "2".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "trv_tag".getBytes(), "2".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "htl_lvl_tag".getBytes(), "2".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "htl_concentration_tag".getBytes(), "1".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "cross_dist_tag".getBytes(), "3".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "huge_trans_tag".getBytes(), "-1".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "exist_car".getBytes(), "1".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "new_car_tag".getBytes(), "2".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "volume_tag".getBytes(), "1".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "fuelcard_tag".getBytes(), "1".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "fuel_time_tag".getBytes(), "2".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "rent_tag".getBytes(), "1".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "drive_degree_tag".getBytes(), "1".getBytes());// 本行数据的第一列 
        put.add("d".getBytes(), "drive_scope_tag".getBytes(), "2".getBytes());// 本行数据的第一列 
//          put.add("d".getBytes(), "drive_degree".getBytes(), "3".getBytes());// 本行数据的第三列 short_refuel_day_interval not_agency_times_pct
//          put.add("d".getBytes(), "short_refuel_day_interval".getBytes(), "12".getBytes());
//          put.add("d".getBytes(), "agency_times_pct".getBytes(), "0.667".getBytes());
//          put.add("d".getBytes(), "not_agency_times_pct".getBytes(), "0.333".getBytes());
          //put.add("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列 
        try { 
            table.put(put); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
        System.out.println("end insert data ......"); 
    } 
    
    public static void insertData2(String tableName) { 
        System.out.println("start insert data ......"); 
        HTablePool pool = new HTablePool(configuration, 1000); 
        HTable table = (HTable) pool.getTable(tableName); 
        //Put put = new Put("15370286001719373@2014".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值 
        Put put = new Put("166222530115357639@2014".getBytes());
        put.add("d".getBytes(), "iss_ins_cd".getBytes(), "03010000".getBytes());
        put.add("d".getBytes(), "iss_ins_name".getBytes(), "中国交通银行".getBytes());
        put.add("d".getBytes(), "card_level".getBytes(), "0".getBytes());
        put.add("d".getBytes(), "confidence".getBytes(), "2".getBytes());
        put.add("d".getBytes(), "is_new_car".getBytes(), "0".getBytes());
        put.add("d".getBytes(), "purchase_month".getBytes(), "null".getBytes());
        put.add("d".getBytes(), "refuel_times".getBytes(), "35".getBytes());
        put.add("d".getBytes(), "refuel_sum".getBytes(), "9722.8".getBytes());
        put.add("d".getBytes(), "avg_refuel_amount".getBytes(), "256.25882".getBytes());
        put.add("d".getBytes(), "low_refuel_times_pct".getBytes(), "1.0".getBytes());
        put.add("d".getBytes(), "media_refuel_times_pct".getBytes(), "0.0".getBytes());
        put.add("d".getBytes(), "high_refuel_times_pct".getBytes(), "0.0".getBytes());
        put.add("d".getBytes(), "abnormal_refuel_times".getBytes(), "0".getBytes());
        put.add("d".getBytes(), "fuel_card_times".getBytes(), "2".getBytes());
        put.add("d".getBytes(), "refuel_loc_cd".getBytes(), "2900".getBytes());
        put.add("d".getBytes(), "refuel_loc_city".getBytes(), "上海市".getBytes());
        put.add("d".getBytes(), "refuel_loc_province".getBytes(), "上海市".getBytes());
        put.add("d".getBytes(), "refuel_record".getBytes(), "20141217213600@中油杨思加油站@100.0}20141212185546@中国石油天然气股份有限公司上海销售分公司@500.0}20141128081055@中油杨思加油站@300.0}20141118153443@中国石油天然气股份有限公司浙江杭州销售分@200.0}20141113124743@中国石油天然气股份有限公司安徽合肥销售分@58.0}20141016164700@中石油常熟东张加油站@216.5}20141015111924@江苏宁沪高速公路股份有限公司@305.0}20141015094153@中油杨思加油站@100.0}20140919123955@常州长江石油有限公司@283.0}20140905192102@桐乡东恒加油站有限公司@200.0}20140822114240@中国石油天然气股份有限公司上海销售分公司@500.0}20140820171110@常熟中油江南三峰加油站@300.0}20140808183316@中国石油天然气股份有限公司上海销售分公司@500.0}20140712074949@桐乡东恒加油站有限公司@340.0}20140707171754@中石油常熟虞山加油站@200.0}20140704194930@中石油浙江杭州销售分公司（高翔）@171.5}20140704074247@中油杨思加油站@350.0}20140627151510@中国石油天然气股份有限公司上海销售分公司@500.0}20140620184911@中国石油天然气股份有限公司上海销售分公司@500.0}20140606214257@桐乡市崇福常新加油站@220.0}20140430155357@江苏宁沪高速公路股份有限公司@112.0}20140415181918@桐乡东恒加油站有限公司@180.0}20140407134228@绍兴县越盛加油站有限公司@278.0}20140331140140@绍兴县越盛加油站有限公司@302.0}20140326115845@绍兴县越盛加油站有限公司@200.0}20140321174427@中国石油天然气股份有限公司桐乡美孚加油站@245.0}20140206200445@中油杨思加油站@100.0}20140203170026@桐乡东恒加油站有限公司@1010.0}20140127175434@桐乡东恒加油站有限公司@230.0}20140126145658@桐乡东恒加油站有限公司@250.0}20140117091811@中油杨思加油站@240.0}20140116185948@上海炼星加油站有限公司@100.0}20140115143319@江苏省燃料总公司东晟加油站@271.8}20140114095054@中油杨思加油站@190.0}20140108205028@中国石油天然气股份有限公司浙江杭州销售分@170.0".getBytes());
        put.add("d".getBytes(), "is_other_city_refuel".getBytes(), "1".getBytes());
        put.add("d".getBytes(), "is_other_province_refuel".getBytes(), "1".getBytes());
        put.add("d".getBytes(), "often_refuel_time".getBytes(), "17-24".getBytes());
        put.add("d".getBytes(), "refuel_time_record".getBytes(), "9-12@7}12-13@3}ERROR@0}13-17@7}6-9@2}17-24@16}0-6@0".getBytes());
        put.add("d".getBytes(), "often_refuel_day".getBytes(), "FRI".getBytes());
        put.add("d".getBytes(), "refuel_day_record".getBytes(), "FRI@13}TUE@3}MON@6}SAT@1}SUM@1}THU@4}WED@9".getBytes());
        put.add("d".getBytes(), "avg_refuel_day_interval".getBytes(), "7".getBytes());
        put.add("d".getBytes(), "long_refuel_day_interval".getBytes(), "23".getBytes());
        put.add("d".getBytes(), "short_refuel_day_interval".getBytes(), "0".getBytes());
        put.add("d".getBytes(), "often_refuel_amount_interval".getBytes(), "200-300元".getBytes());
        put.add("d".getBytes(), "refuel_amount_interval_record".getBytes(), "200-300@13}500-600@5}300-400@6}1000+@1}0-200@10".getBytes());
        put.add("d".getBytes(), "often_refuel_mchnt".getBytes(), "中油杨思加油站".getBytes());
        put.add("d".getBytes(), "often_refuel_brand".getBytes(), "中石油".getBytes());
        put.add("d".getBytes(), "often_fuelcard_mchnt".getBytes(), "桐乡东恒加油站有限公司".getBytes());
        put.add("d".getBytes(), "often_fuelcard_brand".getBytes(), "民营".getBytes());
        put.add("d".getBytes(), "is_fraud".getBytes(), "0".getBytes());
        put.add("d".getBytes(), "maintain_sum".getBytes(), "1231.9".getBytes());
        put.add("d".getBytes(), "maintain_times".getBytes(), "3".getBytes());
        put.add("d".getBytes(), "avg_maintain_amount".getBytes(), "392.475".getBytes());
        put.add("d".getBytes(), "agency_sum_pct".getBytes(), "0.0".getBytes());
        put.add("d".getBytes(), "not_agency_sum_pct".getBytes(), "1.0".getBytes());
        put.add("d".getBytes(), "lacquer_sum_pct".getBytes(), "0.0".getBytes());
        put.add("d".getBytes(), "wash_sum_pct".getBytes(), "0.0".getBytes());
        put.add("d".getBytes(), "agency_times_pct".getBytes(), "0.0".getBytes());
        put.add("d".getBytes(), "not_agency_times_pct".getBytes(), "1.0".getBytes());
        put.add("d".getBytes(), "lacquer_times_pct".getBytes(), "0.0".getBytes());
        put.add("d".getBytes(), "wash_times_pct".getBytes(), "0.0".getBytes());
        put.add("d".getBytes(), "maintain_loc_cd".getBytes(), "2900".getBytes());
        put.add("d".getBytes(), "maintain_city".getBytes(), "上海市".getBytes());
        put.add("d".getBytes(), "maintain_province".getBytes(), "上海市".getBytes());
        put.add("d".getBytes(), "parking_sum".getBytes(), "0.0".getBytes());
        put.add("d".getBytes(), "parking_times".getBytes(), "0".getBytes());
        put.add("d".getBytes(), "toll_sum".getBytes(), "0.0".getBytes());
        put.add("d".getBytes(), "toll_times".getBytes(), "0".getBytes());
        put.add("d".getBytes(), "rent_sum".getBytes(), "2074.0".getBytes());
        put.add("d".getBytes(), "rent_times".getBytes(), "6".getBytes());
        put.add("d".getBytes(), "is_other_city_rent".getBytes(), "1".getBytes());
        put.add("d".getBytes(), "is_other_province_rent".getBytes(), "1".getBytes());
        put.add("d".getBytes(), "rent_record".getBytes(), "20141113130839@北京市@北京市}20140919130050@常州市@江苏省}20140704210959@杭州市@浙江省}20140507124942@北京市@北京市}20140109140451@上海市@上海市}20140107123103@上海市@上海市".getBytes());
        put.add("d".getBytes(), "drive_degree".getBytes(), "3".getBytes());
        put.add("d".getBytes(), "drive_record".getBytes(), "杭州市@浙江省@4}上海市@上海市@15}绍兴市@浙江省@3}南京市@江苏省@3}北京市@北京市@2}常州市@江苏省@2}苏州市@江苏省@3}合肥市@安徽省@1}嘉兴市@浙江省@10".getBytes());
        put.add("d".getBytes(), "drive_city_count".getBytes(), "9".getBytes());
        put.add("d".getBytes(), "drive_province_count".getBytes(), "5".getBytes());
        
        try { 
            table.put(put); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
        System.out.println("end insert data ......"); 
    } 
 
     
    public static void dropTable(String tableName) { 
        try { 
            HBaseAdmin admin = new HBaseAdmin(configuration); 
            admin.disableTable(tableName); 
            admin.deleteTable(tableName); 
        } catch (MasterNotRunningException e) { 
            e.printStackTrace(); 
        } catch (ZooKeeperConnectionException e) { 
            e.printStackTrace(); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
 
    } 
     
     public static void deleteRow(String tablename, String rowkey)  { 
        try { 
            HTable table = new HTable(configuration, tablename); 
            List list = new ArrayList(); 
            Delete d1 = new Delete(rowkey.getBytes()); 
            list.add(d1); 
             
            table.delete(list); 
            System.out.println("删除行成功!"); 
             
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
         
 
    } 
 
      
     public static void deleteByCondition(String tablename, String rowkey)  { 
            //目前还没有发现有效的API能够实现根据非rowkey的条件删除这个功能能，还有清空表全部数据的API操作 
 
    } 
 
 
     
    public static void QueryAll(String tableName) { 
        
        HTable table;
        
        try { 
			table = new HTable(configuration, tableName);
	        table.setAutoFlush(false);
	        table.setWriteBufferSize(WRITE_BUFFER_SIZE);
            ResultScanner rs = table.getScanner(new Scan()); 
            int count = 0;
            if(rs==null)
            	System.out.println("I am null!");
            for (Result r : rs) { 
                System.out.println("获得到rowkey:" + new String(r.getRow())); 
                for (KeyValue keyValue : r.raw()) { 
                    System.out.println("列：" + new String(keyValue.getFamily()) 
                    		+ "\n属性:"+ new String(keyValue.getQualifier()) 
                            + "\n值:" + new String(keyValue.getValue())); 
                } 
                count++;
                if(count >100)
                	break;
            } 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    } 
 
     
    public static void QueryByCondition1(String tableName) { 
 
        //HTablePool pool = new HTablePool(configuration, 1000); 
        //HTable table = (HTable) pool.getTable(tableName); 
    	
        try { 
        	HTable table = new HTable(configuration, tableName);
            //Get scan = new Get("26dd19731d43642cb4e1694746d6bd0d,2013".getBytes());// 根据rowkey查询 1436088402900800
            //Get scan = new Get("166222530115357639@2014".getBytes());// 根据rowkey查询 
            Get scan = new Get("1436088309805219@2014".getBytes());// 根据rowkey查询 
            Result r = table.get(scan); 
            System.out.println("获得到rowkey:" + new String(r.getRow())); 
            for (KeyValue keyValue : r.raw()) { 
                System.out.println("列：" + new String(keyValue.getFamily()) 
                		+ "\n属性:"+ new String(keyValue.getQualifier()) 
                        + "\n值:" + new String(keyValue.getValue())); 
            } 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    } 
 
     
    public static void QueryByCondition2(String tableName) { 
 
        try { 
            HTablePool pool = new HTablePool(configuration, 1000); 
            HTable table = (HTable) pool.getTable(tableName); 
            Filter filter = new SingleColumnValueFilter(Bytes 
                    .toBytes("column1"), null, CompareOp.EQUAL, Bytes 
                    .toBytes("aaa")); // 当列column1的值为aaa时进行查询 
            Scan s = new Scan(); 
            s.setFilter(filter); 
            ResultScanner rs = table.getScanner(s); 
            for (Result r : rs) { 
                System.out.println("获得到rowkey:" + new String(r.getRow())); 
                for (KeyValue keyValue : r.raw()) { 
                    System.out.println("列：" + new String(keyValue.getFamily()) 
                            + "====值:" + new String(keyValue.getValue())); 
                } 
            } 
        } catch (Exception e) { 
            e.printStackTrace(); 
        } 
 
    } 
 
     
    public static void QueryByCondition3(String tableName) { 
 
        try { 
            HTablePool pool = new HTablePool(configuration, 1000); 
            HTable table = (HTable) pool.getTable(tableName); 
 
            List<Filter> filters = new ArrayList<Filter>(); 
 
            Filter filter1 = new SingleColumnValueFilter(Bytes 
                    .toBytes("column1"), null, CompareOp.EQUAL, Bytes 
                    .toBytes("aaa")); 
            filters.add(filter1); 
 
            Filter filter2 = new SingleColumnValueFilter(Bytes 
                    .toBytes("column2"), null, CompareOp.EQUAL, Bytes 
                    .toBytes("bbb")); 
            filters.add(filter2); 
 
            Filter filter3 = new SingleColumnValueFilter(Bytes 
                    .toBytes("column3"), null, CompareOp.EQUAL, Bytes 
                    .toBytes("ccc")); 
            filters.add(filter3); 
 
            FilterList filterList1 = new FilterList(filters); 
 
            Scan scan = new Scan(); 
            scan.setFilter(filterList1); 
            ResultScanner rs = table.getScanner(scan); 
            for (Result r : rs) { 
                System.out.println("获得到rowkey:" + new String(r.getRow())); 
                for (KeyValue keyValue : r.raw()) { 
                    System.out.println("列：" + new String(keyValue.getFamily()) 
                            + "====值:" + new String(keyValue.getValue())); 
                } 
            } 
            rs.close(); 
 
        } catch (Exception e) { 
            e.printStackTrace(); 
        } 
 
    } 
 
}