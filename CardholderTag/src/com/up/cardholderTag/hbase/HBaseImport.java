package com.up.cardholderTag.hbase;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;

public class HBaseImport
{

	public static Configuration configuration;
	public static String columnName = "d";
	public static String keySpliter = ",";
	static
	{
		configuration = HBaseConfiguration.create();
		/*configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "172.16.24.136");
		configuration.set("hbase.master", "172.16.24.136:60000");*/
	}

	public static void createTable(String tableName)
	{
		System.out.println("start create table ......");
		try
		{
			@SuppressWarnings("resource")
			HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
			if (hBaseAdmin.tableExists(tableName))
				return;
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor(columnName));
			hBaseAdmin.createTable(tableDescriptor);
		}
		catch (MasterNotRunningException e)
		{
			e.printStackTrace();
		}
		catch (ZooKeeperConnectionException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		System.out.println("end create table ......");
	}
	
	public static void importCarFeature(String tableName, Path infoPath)
	{
		try
		{
			HTablePool pool = new HTablePool(configuration, 1000);
			HTable table = (HTable) pool.getTable(tableName);

			FileSystem fs = FileSystem.get(infoPath.toUri(), configuration);
			FSDataInputStream fin = fs.open(infoPath);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fin, "UTF-8"));
			String line = reader.readLine();
			int number = 0;
			while (line != null)
			{
				String[] info = line.split(",");
				if (!info[0].equals(""))
				{
					try
					{
						String key = info[0] + keySpliter + "2014";
						Put put = new Put(key.getBytes());
						put.add(columnName.getBytes(), "md5".getBytes(),
								info[1].getBytes());
						put.add(columnName.getBytes(), "iss_ins_cd".getBytes(),
								info[2].getBytes());
						put.add(columnName.getBytes(), "iss_ins_name".getBytes(),
								info[3].getBytes());
						put.add(columnName.getBytes(),"card_level".getBytes(), 
								info[4].getBytes());
						put.add(columnName.getBytes(), "confidence".getBytes(),
								info[5].getBytes());
						put.add(columnName.getBytes(), "is_new_car".getBytes(),
								info[6].getBytes());
						put.add(columnName.getBytes(), "purchase_month".getBytes(),
								info[7].getBytes());
						put.add(columnName.getBytes(), "refuel_times".getBytes(),
								info[8].getBytes());
						put.add(columnName.getBytes(), "refuel_sum".getBytes(),
								info[9].getBytes());
						put.add(columnName.getBytes(), "avg_refuel_amount".getBytes(),
								info[10].getBytes());
						put.add(columnName.getBytes(), "low_refuel_times_pct".getBytes(),
								info[11].getBytes());
						put.add(columnName.getBytes(), "media_refuel_times_pct".getBytes(),
								info[12].getBytes());
						put.add(columnName.getBytes(), "high_refuel_times_pct".getBytes(),
								info[13].getBytes());
						put.add(columnName.getBytes(), "abnormal_refuel_times".getBytes(),
								info[14].getBytes());
						put.add(columnName.getBytes(), "fuel_card_times".getBytes(),
								info[15].getBytes());
						put.add(columnName.getBytes(), "refuel_loc_cd".getBytes(),
								info[16].getBytes());
						put.add(columnName.getBytes(), "refuel_loc_city".getBytes(),
								info[17].getBytes());
						put.add(columnName.getBytes(), "refuel_loc_province".getBytes(),
								info[18].getBytes());
						put.add(columnName.getBytes(), "refuel_record".getBytes(),
								info[19].getBytes());
						put.add(columnName.getBytes(), "is_other_city_refuel".getBytes(),
								info[20].getBytes());
						put.add(columnName.getBytes(), "is_other_province_refuel".getBytes(),
								info[21].getBytes());
						put.add(columnName.getBytes(), "often_refuel_time".getBytes(),
								info[22].getBytes());
						put.add(columnName.getBytes(), "refuel_time_record".getBytes(),
								info[23].getBytes());
						put.add(columnName.getBytes(), "often_refuel_day".getBytes(),
								info[24].getBytes());
						put.add(columnName.getBytes(), "refuel_day_record".getBytes(),
								info[25].getBytes());
						put.add(columnName.getBytes(), "avg_refuel_day_interval".getBytes(),
								info[26].getBytes());
						put.add(columnName.getBytes(), "long_refuel_day_interval".getBytes(),
								info[27].getBytes());
						put.add(columnName.getBytes(), "short_refuel_day_interval".getBytes(),
								info[28].getBytes());
						put.add(columnName.getBytes(), "often_refuel_amount_interval".getBytes(),
								info[29].getBytes());
						put.add(columnName.getBytes(), "refule_amount_interval_record".getBytes(),
								info[30].getBytes());
						put.add(columnName.getBytes(), "often_refuel_mchnt".getBytes(),
								info[31].getBytes());
						put.add(columnName.getBytes(), "often_refuel_brand".getBytes(),
								info[32].getBytes());
						put.add(columnName.getBytes(), "often_fuelCard_mchnt".getBytes(),
								info[33].getBytes());
						put.add(columnName.getBytes(), "often_fuelCard_brand".getBytes(),
								info[34].getBytes());
						put.add(columnName.getBytes(), "is_fraud".getBytes(),
								info[35].getBytes());
						put.add(columnName.getBytes(), "maintain_sum".getBytes(),
								info[36].getBytes());
						put.add(columnName.getBytes(), "maintain_times".getBytes(),
								info[37].getBytes());
						put.add(columnName.getBytes(), "avg_maintain_amount".getBytes(),
								info[38].getBytes());
						put.add(columnName.getBytes(), "agency_sum_pct".getBytes(),
								info[39].getBytes());
						put.add(columnName.getBytes(), "not_agency_sum_pct".getBytes(),
								info[40].getBytes());
						put.add(columnName.getBytes(), "lacquer_sum_pct".getBytes(),
								info[41].getBytes());
						put.add(columnName.getBytes(), "wash_sum_pct".getBytes(),
								info[42].getBytes());
						put.add(columnName.getBytes(), "agency_times_pct".getBytes(),
								info[43].getBytes());
						put.add(columnName.getBytes(), "not_agency_times_pct".getBytes(),
								info[44].getBytes());
						put.add(columnName.getBytes(), "lacquer_times_pct".getBytes(),
								info[45].getBytes());
						put.add(columnName.getBytes(), "wash_times_pct".getBytes(),
								info[46].getBytes());
						put.add(columnName.getBytes(), "maintain_loc_cd".getBytes(),
								info[47].getBytes());
						put.add(columnName.getBytes(), "maintain_city".getBytes(),
								info[48].getBytes());
						put.add(columnName.getBytes(), "maintain_province".getBytes(),
								info[49].getBytes());
						put.add(columnName.getBytes(), "parking_sum".getBytes(),
								info[50].getBytes());
						put.add(columnName.getBytes(), "parking_times".getBytes(),
								info[51].getBytes());
						put.add(columnName.getBytes(), "toll_sum".getBytes(),
								info[52].getBytes());
						put.add(columnName.getBytes(), "toll_times".getBytes(),
								info[53].getBytes());
						put.add(columnName.getBytes(), "rent_sum".getBytes(),
								info[54].getBytes());
						put.add(columnName.getBytes(), "rent_times".getBytes(),
								info[55].getBytes());
						put.add(columnName.getBytes(), "is_other_city_rent".getBytes(),
								info[56].getBytes());
						put.add(columnName.getBytes(), "is_other_province_rent".getBytes(),
								info[57].getBytes());
						put.add(columnName.getBytes(), "rent_record".getBytes(),
								info[58].getBytes());
						put.add(columnName.getBytes(), "drive_degree".getBytes(),
								info[59].getBytes());
						put.add(columnName.getBytes(), "drive_record".getBytes(),
								info[60].getBytes());
						put.add(columnName.getBytes(), "drive_city_count".getBytes(),
								info[61].getBytes());
						put.add(columnName.getBytes(), "drive_province_count".getBytes(),
								info[62].getBytes());
						
						table.put(put);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
					number++;
				}
				line = reader.readLine();
				if (number % 10000 == 0)
				{
					System.out.println("Processing the " + number + "th data");
				}
			}
			reader.close();
			pool.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public static void importTag(String tableName, Path infoPath)
	{
		try
		{
			HTablePool pool = new HTablePool(configuration, 1000);
			HTable table = (HTable) pool.getTable(tableName);

			FileSystem fs = FileSystem.get(infoPath.toUri(), configuration);
			FSDataInputStream fin = fs.open(infoPath);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fin, "UTF-8"));
			String line = reader.readLine();
			int number = 0;
			while (line != null)
			{
				String[] info = line.split(",");
				if (!info[0].equals(""))
				{
					try
					{
						String key = info[0];
						Put put = new Put(key.getBytes());
						put.add(columnName.getBytes(), "exist_travel".getBytes(),
								info[1].getBytes());
						put.add(columnName.getBytes(), "rwy_tag".getBytes(),
								info[2].getBytes());
						put.add(columnName.getBytes(), "plt_tag".getBytes(),
								info[3].getBytes());
						put.add(columnName.getBytes(),"trv_tag".getBytes(), 
								info[4].getBytes());
						put.add(columnName.getBytes(), "htl_lvl_tag".getBytes(),
								info[5].getBytes());
						put.add(columnName.getBytes(), "htl_concentration_tag".getBytes(),
								info[6].getBytes());
						put.add(columnName.getBytes(), "cross_dist_tag".getBytes(),
								info[7].getBytes());
						put.add(columnName.getBytes(), "huge_trans_tag".getBytes(),
								info[8].getBytes());
						put.add(columnName.getBytes(), "exist_car".getBytes(),
								info[9].getBytes());
						put.add(columnName.getBytes(), "new_car_tag".getBytes(),
								info[10].getBytes());
						put.add(columnName.getBytes(), "volume_tag".getBytes(),
								info[11].getBytes());
						put.add(columnName.getBytes(), "fuelcard_tag".getBytes(),
								info[12].getBytes());
						put.add(columnName.getBytes(), "fuel_time_tag".getBytes(),
								info[13].getBytes());
						put.add(columnName.getBytes(), "rent_tag".getBytes(),
								info[14].getBytes());
						put.add(columnName.getBytes(), "drive_degree_tag".getBytes(),
								info[15].getBytes());
						put.add(columnName.getBytes(), "drive_scope_tag".getBytes(),
								info[16].getBytes());
						table.put(put);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
					number++;
				}
				line = reader.readLine();
				if (number % 10000 == 0)
				{
					System.out.println("Processing the " + number + "th data");
				}
			}
			reader.close();
			pool.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public static void importDailyInfo(String tableName, Path infoPath, String location, String time)
	{
		try
		{
			HTablePool pool = new HTablePool(configuration, 1000);
			HTable table = (HTable) pool.getTable(tableName);

			FileSystem fs = FileSystem.get(infoPath.toUri(), configuration);
			FSDataInputStream fin = fs.open(infoPath);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fin, "UTF-8"));
			String line = reader.readLine();
			int number = 0;
			while (line != null)
			{
				String[] info = line.split(",");
				if (info.length == 63)
				{
					try
					{
						String name = info[0].trim();
						String key = time + keySpliter + location + keySpliter
								+ name;
						Put put = new Put(key.getBytes());
						for(int i = 1; i <= 31; i++)						
							put.add(columnName.getBytes(), ("a"+i+"").getBytes(),
									info[i].getBytes());
						for(int i = 32; i <= 62; i++)					
							put.add(columnName.getBytes(), ("t"+(i-31)).getBytes(),
									info[i].getBytes());
						table.put(put);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
					number++;
				}
				line = reader.readLine();
				if (number % 10000 == 0)
				{
					System.out.println("Processing the " + number + "th data");
				}
			}
			reader.close();
			pool.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public static void importMerchantFeature(String tableName, Path infoPath, String year, String location)
	{
		try
		{
			HTablePool pool = new HTablePool(configuration, 1000);
			HTable table = (HTable) pool.getTable(tableName);

			FileSystem fs = FileSystem.get(infoPath.toUri(), configuration);
			FSDataInputStream fin = fs.open(infoPath);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fin, "UTF-8"));
			String line = reader.readLine();
			int number = 0;
			while (line != null)
			{
				String[] tokens = line.trim().split(",");
				if (tokens.length == 14)
				{
					try
					{
						String month = tokens[1].trim();
						String name = tokens[0].trim();

						String key = year + month + keySpliter + location
								+ keySpliter + name;
						Put put = new Put(key.getBytes());
						put.add(columnName.getBytes(), "total_card_num"
								.getBytes(), tokens[5].trim().getBytes());
						put.add(columnName.getBytes(), "high_card_num"
								.getBytes(), tokens[6].trim().getBytes());
						put.add(columnName.getBytes(), "high_card_amount"
								.getBytes(), new BigInteger(tokens[7].trim())
								.divide(new BigInteger("100")).toString()
								.getBytes());
						put.add(columnName.getBytes(), "credict_card_num"
								.getBytes(), tokens[8].trim().getBytes());
						put.add(columnName.getBytes(),
								"credict_card_amount".getBytes(),
								new BigInteger(tokens[9].trim())
										.divide(new BigInteger("100"))
										.toString().getBytes());
						put.add(columnName.getBytes(), "return_card_num"
								.getBytes(), tokens[10].trim().getBytes());
						put.add(columnName.getBytes(),
								"return_card_amount".getBytes(),
								new BigInteger(tokens[11].trim())
										.divide(new BigInteger("100"))
										.toString().getBytes());
						put.add(columnName.getBytes(), "nonlocal_card_num"
								.getBytes(), tokens[12].trim().getBytes());
						put.add(columnName.getBytes(),
								"nonlocal_card_amount".getBytes(),
								new BigInteger(tokens[13].trim())
										.divide(new BigInteger("100"))
										.toString().getBytes());
						table.put(put);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
					number++;
				}
				line = reader.readLine();
				if (number % 10000 == 0)
				{
					System.out.println("Processing the " + number + "th data");
				}
			}
			reader.close();
			pool.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/*static class BatchImportMapper extends
			Mapper<LongWritable, Text, Text, Text>
	{
		private HTable table;
		private HTablePool pool;

		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			Configuration conf = context.getConfiguration();
			pool = new HTablePool(conf, 1000);
			table = (HTable) pool.getTable("merchant_feature");
		}

		protected void map(LongWritable key, Text value, Context context)
				throws java.io.IOException, InterruptedException
		{

			String line = value.toString();
			String[] info = line.split(",");

			if (info.length >= 9 && info[1].length() == 6
					&& info[1].contains("20"))
			{
				try
				{
					String time = info[1];
					String name = info[0];
					String hKey = time + keySpliter + location + keySpliter
							+ name;
					Put put = new Put(hKey.getBytes());
					put.add(columnName.getBytes(), "mcc".getBytes(),
							info[2].getBytes());
					put.add(columnName.getBytes(), "amount".getBytes(),
							info[3].getBytes());
					put.add(columnName.getBytes(), "amount_rank".getBytes(),
							info[4].getBytes());
					put.add(columnName.getBytes(), "amount_exc".getBytes(),
							info[5].getBytes());
					put.add(columnName.getBytes(), "times".getBytes(),
							info[6].getBytes());
					put.add(columnName.getBytes(), "times_rank".getBytes(),
							info[7].getBytes());
					put.add(columnName.getBytes(), "times_exc".getBytes(),
							info[8].getBytes());
					table.put(put);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		};
		
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
			pool.close();
			table.close();
		}	
	}

	public static void runJob() throws IOException, InterruptedException, ClassNotFoundException
	{
		Job job = new Job(configuration, "Merchant Features Import Driver");

		((JobConf)job.getConfiguration()).setJar(Configurations.JARNAME);
		configuration.set("mapred.jar", Configurations.JARNAME);
		    
	    job.setMapperClass(BatchImportMapper.class); 
	    job.setNumReduceTasks(0);
	    job.setJarByClass(DataImporter4Hbase.class);
		    
	    String[] locations = {"shanghai", "jiangsu", "zhejiang", "ningbo",
				"anhui", "fujian", "guangdong", "shenzhen", "xiamen",
				"jiangxi", "guangxi", "yunnan", "guizhou", "hunan", "hubei",
				"henan", "hebei", "shandong", "shanxi", "shaanxi", "beijing",
				"tianjin", "qingdao", "dalian", "liaoning", "jilin",
				"heilongjiang", "neimeng", "ningxia", "sichuan", "chongqing",
				"qinghai", "xizang", "xinjiang", "gansu", "hainan",
				"all_locations"};
	    String pathHead = "/user/hdanaly/association_model/";
	    String inputs =  pathHead + year + "/" + locations[0] + "/monthly_statistics";
	    for(int i = 1; i < locations.length; i++)
	    	inputs = inputs + "," + pathHead + year + "/" + locations[i] + "/monthly_statistics";
	    	    
	    FileInputFormat.addInputPaths(job, inputs);
		    
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) 
			throw new IllegalStateException("Job failed!");	
	}
*/
	
	public static void main(String[] args) throws IOException
	{
		createTable("tbl_common_cardHolder_car");
		importCarFeature(
				"tbl_common_cardHolder_car",
				new Path(args[0]));
		
		
		createTable("tbl_common_cardHolder_tag");
		importTag(
				"tbl_common_cardHolder_tag",
				new Path(args[1]));
		
	}
}
