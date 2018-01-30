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
 

	
public class HMMTrainerTest {
	
	public static void main(String[] argsx) throws FileNotFoundException, IOException {
	
		System.out.println("start");
		HmmModel model;
		 
		int hiddenNum =4;
		int obnum = 10;
		

		double[] initialP = new double[hiddenNum];
	    for(int i=0;i<hiddenNum;i++)
	    	initialP[i] = (double)1/hiddenNum;		
		
	 
		
		 double[][] transitionA = new double[hiddenNum][hiddenNum];
		    for(int i=0;i<hiddenNum;i++){
		    	double mrange =1;
		    	for(int j=0;j<hiddenNum;j++){
		    		if(j==hiddenNum-1)
		    			transitionA[i][j] = mrange;
		    		else{
		    		transitionA[i][j] = Math.random()*mrange;
		    		mrange = mrange-transitionA[i][j];
		    		}
		    	}
		    }

	
		    double[][] emissionB = new double[hiddenNum][obnum];
		    for(int i=0;i<hiddenNum;i++)
		    	for(int j=0;j<obnum;j++){
		    		emissionB[i][j] = (double)1/obnum;
		    	}
	    
 
	
	    // now generate the model
	    model = new HmmModel(new DenseMatrix(transitionA), new DenseMatrix(
	        emissionB), new DenseVector(initialP));

		
	    String fileName = "10test.txt";
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
                String[] tempArray = templine.split(":")[1].split(",");
                int[] tempObSeq = StrArray2IntArray(tempArray);
               
                HmmModel ModelTrained = HmmTrainer.trainBaumWelch(model, tempObSeq, 0.05, 50, false);
                
                
                Vector P =  ModelTrained.getInitialProbabilities();
                Matrix B =  ModelTrained.getEmissionMatrix();
                Matrix A =  ModelTrained.getTransitionMatrix();

                System.out.println("P is:"); 
                System.out.println(P); 
                System.out.println("A is:"); 
                System.out.println(A); 
                System.out.println("B is:"); 
                System.out.println(B); 
                
                
                int[] decode1 = HmmEvaluator.predict(ModelTrained, 3);    
                System.out.println(Arrays.toString(decode1));
                
                //观测序列 0,1,2的概率
                double d2 = HmmEvaluator.modelLikelihood(ModelTrained, new int[]{0,0,0}, false);
                System.out.println(d2);
             
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

        
	}


	
	public static int[] StrArray2IntArray(String[] arr){
        int[] intArr = new int[arr.length];
        for (int i=0; i<arr.length; i++) {
            intArr[i] = Integer.parseInt(arr[i]);
        }
        return intArr;
    }
	
}


 