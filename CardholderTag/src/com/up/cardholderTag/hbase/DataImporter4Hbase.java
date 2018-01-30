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

public class DataImporter4Hbase
{

	public static Configuration configuration;
	public static String columnName = "d";
	public static String keySpliter = "\001";
//	private static String location = "guangdong";
//	private static String year = "2013";
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

	public static void importMerchantInfo(String tableName, Path infoPath, String location)
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
				if (info.length >= 9 && info[1].length() == 6
						&& info[1].contains("20"))
				{
					try
					{
						String time = info[1];
						String name = info[0];
						String key = time + keySpliter + location + keySpliter
								+ name;
						Put put = new Put(key.getBytes());
						put.add(columnName.getBytes(), "mcc".getBytes(),
								info[2].getBytes());
						put.add(columnName.getBytes(), "amount".getBytes(),
								info[3].getBytes());
						put.add(columnName.getBytes(),
								"amount_rank".getBytes(), info[4].getBytes());
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
		createTable("tbl_common_merchant_features");
		for(int i = 0; i < args.length; i++)
		{
			System.out.println("Insert: "+args[i]);
			importMerchantInfo(
					"tbl_common_merchant_features",
					new Path(
							"/user/hdanaly/association_model/2014/" + args[i] + "/monthly_statistics"), args[i]);
		}
		
		createTable("tbl_common_daily_merchant_data");
		for(int i = 0; i < args.length; i++)
		{
			System.out.println("Insert: "+args[i]);
			importDailyInfo(
					"tbl_common_daily_merchant_data",
					new Path(
							"/user/hdanaly/association_model/2014/" + args[i] + "/daily_data"), args[i], "201411");
		}
		
	}
}
