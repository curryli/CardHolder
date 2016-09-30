import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.mahout.classifier.sequencelearning.hmm.HmmEvaluator;
import org.apache.mahout.classifier.sequencelearning.hmm.HmmModel;
import org.apache.mahout.classifier.sequencelearning.hmm.HmmTrainer;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
 

	
public class HMMTrainerSave {
	private static HashMap<String,HmmModelPara> modelDict =  new HashMap<String,HmmModelPara>();
	
	public static void main(String[] argsx) throws FileNotFoundException, IOException {
	
		System.out.println("start");
		HmmModel model;
		 
		int hiddenNum =4;
		int obnum = 6;
		

	    // initialize the initial probability vector  5个隐藏状态
	    double[] initialP = {0.2, 0.1, 0.4, 0.3};
	    
//		double[] initialP = new double[hiddenNum];
//	    for(int i=0;i<hiddenNum;i++)
//	    	initialP[i] = (double)1/hiddenNum;		
		
	    // initialize the transition matrix	
	    double[][] transitionA =  {{0.5, 0.1, 0.1, 0.3}, {0.4, 0.4, 0.1, 0.1},
	        {0.1, 0.0, 0.8, 0.1}, {0.1, 0.1, 0.1, 0.7}};
		
//		 double[][] transitionA = new double[hiddenNum][hiddenNum];
//		    for(int i=0;i<hiddenNum;i++){
//		    	double mrange =1;
//		    	for(int j=0;j<hiddenNum;j++){
//		    		if(j==hiddenNum-1)
//		    			transitionA[i][j] = mrange;
//		    		else{
//		    		transitionA[i][j] = Math.random()*mrange;
//		    		mrange = mrange-transitionA[i][j];
//		    		}
//		    	}
//		    }

	    
	    // initialize the emission matrix  //6个观测状态
		    double[][] emissionB = {{0.4, 0.1, 0.1, 0.1,0.1,0.2}, {0.3, 0.1, 0.2, 0.1,0.1,0.2},{0.1, 0.1, 0.4, 0.1,0.1,0.2},{0.2, 0.1, 0.2, 0.1,0.1,0.3}};
		
//		    double[][] emissionB = new double[hiddenNum][obnum];
//		    for(int i=0;i<hiddenNum;i++)
//		    	for(int j=0;j<obnum;j++){
//		    		emissionB[i][j] = (double)1/obnum;
//		    	}
//	    
	    
	
	    // now generate the model
	    model = new HmmModel(new DenseMatrix(transitionA), new DenseMatrix(
	        emissionB), new DenseVector(initialP));

		
	    String fileName = "SeqDictNew.txt";
	    File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String templine = null;
            //int count = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((templine = reader.readLine()) != null) {
                // 显示行号
                //System.out.println("line " + count + ": " + templine);
            	String tempCard = templine.split(":")[0];
                String[] tempArray = templine.split(":")[1].split(",");
                int[] tempObSeq = StrArray2IntArray(tempArray);
               
                HmmModel tempTrained = HmmTrainer.trainBaumWelch(model, tempObSeq, 0.1, 20, false);
                String tempPstr = tempTrained.getInitialProbabilities().asFormatString();
                String tempAstr = tempTrained.getTransitionMatrix().asFormatString();
                String tempBstr = tempTrained.getEmissionMatrix().asFormatString();
                
                HmmModelPara tempModel = new HmmModelPara();
                tempModel.setA(tempAstr);
                tempModel.setB(tempBstr);
                tempModel.setP(tempPstr);
                
                 
                modelDict.put(tempCard,tempModel);
                //count++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
	    
        
        writeObjectToFile(modelDict, "HMMDictObj");
       
        HashMap<String,HmmModelPara> loadedlDict =  new HashMap<String,HmmModelPara>();
        		
        loadedlDict =	(HashMap<String, HmmModelPara>) readObjectFromFile("HMMDictObj");
        
        
        Iterator iter = loadedlDict.entrySet().iterator();  
        while (iter.hasNext()) {  
            Map.Entry entry = (Map.Entry) iter.next();  
            //Object testCard = entry.getKey();  
            Object testmodelPara = entry.getValue();  
            
            System.out.println(((HmmModelPara) testmodelPara).getP());
            System.out.println(((HmmModelPara) testmodelPara).getA());
            System.out.println(((HmmModelPara) testmodelPara).getB());
           
            
            Matrix testA = HmmModelPara.ParseMatrix(((HmmModelPara) testmodelPara).getA()); 
            Matrix testB = HmmModelPara.ParseMatrix(((HmmModelPara) testmodelPara).getB()); 
            Vector testP =  HmmModelPara.ParseVector(((HmmModelPara) testmodelPara).getP()); 
            
            
            HmmModel testModel = new HmmModel(testA, testB, testP);
        
            int[] decode1 = HmmEvaluator.predict(testModel, 3);    
            System.out.println(Arrays.toString(decode1));
            
            //观测序列 0,1,2的概率
            double d2 = HmmEvaluator.modelLikelihood(testModel, new int[]{0,0,0}, false);
            System.out.println(d2);
                
        }  
        
        String testCard = "473c5a44ca7abfb6fb3ff4da8017f7af";
        HmmModelPara testmodelPara = loadedlDict.get(testCard);
        
        System.out.println(testmodelPara.getP());
        System.out.println(testmodelPara.getA());
        System.out.println(testmodelPara.getB());
       
        
        Matrix testA = HmmModelPara.ParseMatrix(testmodelPara.getA()); 
        Matrix testB = HmmModelPara.ParseMatrix(testmodelPara.getB()); 
        Vector testP =  HmmModelPara.ParseVector(testmodelPara.getP()); 
        
        
        HmmModel testModel = new HmmModel(testA, testB, testP);
    
        int[] decode1 = HmmEvaluator.predict(testModel, 3);    
        System.out.println(Arrays.toString(decode1));
        
        //观测序列 0,1,2的概率
        double d2 = HmmEvaluator.modelLikelihood(testModel, new int[]{0,0,0}, false);
        System.out.println(d2);

        
	}


	
	public static int[] StrArray2IntArray(String[] arr){
        int[] intArr = new int[arr.length];
        for (int i=0; i<arr.length; i++) {
            intArr[i] = Integer.parseInt(arr[i]);
        }
        return intArr;
    }


	public static void writeObjectToFile(Object obj, String filename)
    {
        File file =new File(filename);
        FileOutputStream out;
        try {
            out = new FileOutputStream(file);
            ObjectOutputStream objOut=new ObjectOutputStream(out);
            objOut.writeObject(obj);
            objOut.flush();
            objOut.close();
            System.out.println("write object success!");
        } catch (IOException e) {
            System.out.println("write object failed");
            e.printStackTrace();
        }
    }

	public static Object readObjectFromFile(String filename)
    {
        Object temp=null;
        File file =new File(filename);
        FileInputStream in;
        try {
            in = new FileInputStream(file);
            ObjectInputStream objIn=new ObjectInputStream(in);
            temp=objIn.readObject();
            objIn.close();
            System.out.println("read object success!");
        } catch (IOException e) {
            System.out.println("read object failed");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return temp;
    }

}

 