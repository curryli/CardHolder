package com.up.cardholderTag.classifier;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Random;

import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

public class TrainingDriver implements Serializable{

	private void readRecord(String inputPath){
		try{
			BufferedReader reader = new BufferedReader(new FileReader(inputPath));
			String attrName = reader.readLine();
			String line = reader.readLine();
			int count =  0;
			while(line!=null && count<200){
				System.out.println(line);
				line=reader.readLine();
				count++;
			}
			reader.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private void getRandomTrainFile(String normalPath, int normalNum, String fraudPath, int fraudNum, String outputFileName){
		Random ran = new Random();
		int normalCount = 0;
		int fraudCount = 0;
		//String outputFileName = outputPath + "/sample.csv";
		File outputFile = new File(outputFileName);
		if(outputFile.isFile() && outputFile.exists())
			outputFile.delete();
		try{
			BufferedReader normalFileReader = new BufferedReader(new FileReader(normalPath));
			BufferedReader fraudFileReader = new BufferedReader(new FileReader(fraudPath));
			BufferedWriter output = new BufferedWriter(new FileWriter(outputFileName));
			String line = normalFileReader.readLine();
			output.write(line);
			output.newLine();
			
			while(fraudCount <= fraudNum && (line=fraudFileReader.readLine())!=null){
				if(ran.nextInt()%2==1){
					output.write(line);
					output.newLine();
					fraudCount++;
				}
			}
			
			line = normalFileReader.readLine();            //跳过属性名这一行
			while(normalCount <= normalNum && (line=normalFileReader.readLine())!=null){
				if(ran.nextInt()%2==1){
					output.write(line);
					output.newLine();
					normalCount++;
				}
			}
			
			output.flush();
			output.close();
			normalFileReader.close();
			fraudFileReader.close();
			System.out.println("done!");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 分别取正常数据和欺诈数据合成训练集
	 * 
	 * */
	private void getRandomTestFile(String normalPath, int normalNum, String fraudPath, int fraudNum, String outputFileName){
		Random ran = new Random();
		int normalCount = 0;
		int fraudCount = 0;
		//String outputFileName = outputPath + "/sample.csv";
		File outputFile = new File(outputFileName);
		if(outputFile.isFile() && outputFile.exists())
			outputFile.delete();
		try{
			BufferedReader normalFileReader = new BufferedReader(new FileReader(normalPath));
			BufferedReader fraudFileReader = new BufferedReader(new FileReader(fraudPath));
			BufferedWriter output = new BufferedWriter(new FileWriter(outputFileName));
			String line = normalFileReader.readLine();
			output.write(line);
			output.newLine();
			
			int pre = 0;
			while(++pre<20000 && (line=fraudFileReader.readLine())!=null){}
			//while(++pre<fraudNum && (line=fraudFileReader.readLine())!=null){}
			while(fraudCount <= fraudNum && (line=fraudFileReader.readLine())!=null){
				if(ran.nextInt()%2==1){
					output.write(line);
					output.newLine();
					fraudCount++;
				}
			}
			
			line = normalFileReader.readLine();     //跳过属性名这一行
			pre = 0;
			while(++pre<normalNum && (line=normalFileReader.readLine())!=null){}
			while(normalCount <= normalNum && (line=normalFileReader.readLine())!=null){
				if(ran.nextInt()%2==1){
					output.write(line);
					output.newLine();
					normalCount++;
				}
			}
			
			output.flush();
			output.close();
			normalFileReader.close();
			fraudFileReader.close();
			System.out.println("done!");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private int getRecordCount(String inputPath){
		int count = 0;
		try{
			BufferedReader reader = new BufferedReader(new FileReader(inputPath));
			String line = reader.readLine();
			while(line != null){
				count++;
				line=reader.readLine();
			}
			reader.close();
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return count;
	}
	
	 /**
     * 将训练结果保存到硬盘
     * @param classifier 要保存的分类器
     * @param modelname 保存的名称
     */
    public void SaveModel(Object classifier,String modelname){
        try{
            ObjectOutputStream oos=new ObjectOutputStream(new FileOutputStream("model/"+modelname));
            oos.writeObject(classifier);
            oos.flush();
            oos.close();    
        }catch(IOException e){
            e.printStackTrace();
        }

    }
    
    /**
     * 加载硬盘上的分类器
     * @param file 分类器文件
     * @return 分类器
     */
    public Object LoadModel(String file){
        try{
            ObjectInputStream ois=new ObjectInputStream(new FileInputStream(file));
            Object classifier=ois.readObject();
            ois.close();
            return classifier;
        }catch(IOException e){
            e.printStackTrace();
            return null;
        }catch(ClassNotFoundException e){
            e.printStackTrace();
            return null;
        }
    }
	
	private void trainRandomForest(String trainingSetPath, String testSetPath){
		RandomForest rf = new RandomForest();
		File trainingFile = new File(trainingSetPath);
		CSVLoader csvLoader = new CSVLoader();
		
		try{
			csvLoader.setFile(trainingFile);
			Instances instancesTrain = csvLoader.getDataSet();
			instancesTrain.setClassIndex(instancesTrain.numAttributes()-1);
			rf.setNumTrees(200);
			rf.buildClassifier(instancesTrain);
			
			csvLoader.setFile(new File(testSetPath));
			Instances instancesTest = csvLoader.getDataSet();
			instancesTest.setClassIndex(instancesTest.numAttributes()-1);
			
			double num = instancesTest.numInstances();
			double right = 0.0f;                      //欺诈非欺诈判断对的记录总数
			double negativeRightCount = 0.0f;        //所有判为欺诈的记录里判断对的记录数
			double negativeAllCount = 0.0f;         //判断为欺诈的总数
			for(int i = 0; i<num; i++){
				double judge = rf.classifyInstance(instancesTest.instance(i));
				if(judge!=1.0f)
					negativeAllCount++;
				if(judge==instancesTest.instance(i).classValue())
				{
					right++;
					//System.out.println(instancesTest.instance(i).classValue());
					if(instancesTest.instance(i).classValue()!=1.0f){
						negativeRightCount++;
					}
				}
			}
			System.out.println("Random Forest all classification precision: " + (right/num));
			System.out.println("fraud precision: " + (negativeRightCount/negativeAllCount));
			System.out.println("Random Forest recall rate: " + (negativeRightCount/100));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		TrainingDriver driver = new TrainingDriver();
		//driver.readRecord("d://credit_2013_1.csv");
		//driver.getRandomTrainFile("D://fraud/credit_2013_1.csv", 50000, "D://fraud/fraud_34w.csv", 10000, "D://fraud/sample1.csv");
		//driver.getRandomMergeFile("D://fraud/credit_2013_1.csv", 200000, "D://fraud/fraud_34w.csv", 20000, "D://fraud/sample2.csv");
		driver.getRandomTestFile("D://fraud/credit_2013_1.csv", 800000, "D://fraud/fraud_34w.csv", 100, "D://fraud/sample3.csv");
		driver.trainRandomForest("D://fraud/sample1.csv", "D://fraud/sample3.csv");
	}

}
