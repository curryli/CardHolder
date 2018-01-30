package com.up.cardholderTag.classifier;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import weka.classifiers.Classifier;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.core.converters.CSVLoader;

public class CarClassifier {

	/**
     * 将训练结果保存到硬盘
     * @param classifier 要保存的分类器
     * @param modelname 保存的名称
     */
    public static void SaveModel(Object classifier,String modelname){
        try{
            ObjectOutputStream oos=new ObjectOutputStream(new FileOutputStream(modelname));
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
    public static Object LoadModel(String file){
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
    
    public void trainClassifier(String trainingSetPath){
    	Classifier m_classifier = new J48();
    	File trainingFile = new File(trainingSetPath);
		CSVLoader csvLoader = new CSVLoader();
		
		try{
			csvLoader.setFile(trainingFile);
			Instances instancesTrain = csvLoader.getDataSet();
			instancesTrain.setClassIndex(instancesTrain.numAttributes()-1);
		}
		catch(Exception e){
			e.printStackTrace();
		}
    }
	
	public static void main(String[] args) {
		try
		{
			Classifier m_classifier = new J48();
			File inputFile = new File("C://Program Files//Weka-3-6//data//cpu.with.vendor.arff");
			ArffLoader atf = new ArffLoader();
			atf.setFile(inputFile);
			Instances instancesTrain = atf.getDataSet();
			
			inputFile = new File("C://Program Files//Weka-3-6//data//cpu.with.vendor.arff");
			atf.setFile(inputFile);
			Instances instancesTest = atf.getDataSet();
			instancesTest.setClassIndex(0);
			
			double sum = instancesTest.numInstances();
			double right = 0.0f;
			instancesTrain.setClassIndex(0);
			
			m_classifier.buildClassifier(instancesTrain);
			SaveModel(m_classifier, "D://j48.model");
			Classifier j48 = (Classifier)LoadModel("D://j48.model");
	
			for(int i = 0; i < sum; i++){
				if(j48.classifyInstance(instancesTest.instance(i))==instancesTest.instance(i).classValue())
					right++;
			}
			System.out.println(instancesTrain);
			System.out.println("J48 classification precision:" + (right/sum));
			//System.out.println(m_classifier.listOptions().);
			
			RandomForest rf = new RandomForest();
			rf.buildClassifier(instancesTrain);
			double rfRight = 0.0f;
			for(int i = 0; i < sum ; i++ ){
				if(rf.classifyInstance(instancesTest.instance(i))==instancesTest.instance(i).classValue())
					rfRight++;
			}
			System.out.println("Random Forest classification precision: " +(rfRight/sum));
		
		}catch(Exception e){
			e.printStackTrace();
		}
		

	}

}
