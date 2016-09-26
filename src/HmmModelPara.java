import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

public class HmmModelPara implements Serializable {
	private String transitionA;
	
	private String emissionB;
	
	private String initialP;
	
	public void setA(String transitionA) {
		this.transitionA = transitionA;
	}
	
	public String getA() {
		return  transitionA;
	}
	
	public void setB(String emissionB) {
		this.emissionB = emissionB;
	}
	
	public String getB() {
		return  emissionB;
	}
	
	public void setP(String initialP) {
		this.initialP = initialP;
	}
	
	public String getP() {
		return  initialP;
	}

	public static Matrix ParseMatrix(String FormatedStr){
		String[] tempArray = FormatedStr.split("\\n");
		int rownum = tempArray.length-2;
		int column =  tempArray[1].split(",").length;
		String[] tempArray1 = new String[rownum];
		System.arraycopy(tempArray, 1, tempArray1, 0, rownum);

		double[][] tempArray2 = new double[rownum][column];
		
		for(int i=0; i < tempArray1.length; i++){
			String effectStr = tempArray1[i].split("\\{|\\}")[1];
			String[] lineArray =  effectStr.split(",");
			
			for(int j=0; j < lineArray.length; j++){
				
				if((lineArray[j].split(":")[0]).equals(Integer.toString(j)))
					tempArray2[i][j] = Double.valueOf(lineArray[j].split(":")[1]);
				else
					tempArray2[i][j] = 0.0;
			}
		}
		return  new DenseMatrix(tempArray2);

	}
	
	public static Vector ParseVector(String FormatedStr){
		String effectStr = FormatedStr.split("\\{|\\}")[1];
		String[] tempArray1 = effectStr.split(",");
		double[] tempArray2 = new double[tempArray1.length];
	 
		for(int i=0; i < tempArray1.length; i++){
			if((tempArray1[i].split(":")[0]).equals(Integer.toString(i)))
			  tempArray2[i] = Double.valueOf(tempArray1[i].split(":")[1]);
			else
			  tempArray2[i] = 0.0;
		}
		return  new DenseVector(tempArray2);
	}
}

 

/*
{
  0  => {0:0.5000009086242492,1:0.09999959275945677,2:0.0999973751588311,3:0.300002123457463}
  1  => {0:0.400001800515289,1:0.39999952266700256,2:0.09999772718234633,3:0.10000094963536213}
  2  => {0:0.10000226932270588,2:0.7999949417831475,3:0.10000278889414663}
  3  => {0:0.0999999638803211,1:0.0999993716275411,2:0.09999717425819715,3:0.7000034902339406}
}
{
  0  => {0:0.1514357639812143,1:0.23114746508783418,2:0.3229627861683426,3:0.2944539847626088}
  1  => {0:0.1512649435955553,1:0.23161710662780083,2:0.3227765330887219,3:0.2943414166879221}
  2  => {0:0.15133452996912686,1:0.23235602436405148,2:0.32269258232261555,3:0.2936168633442061}
  3  => {0:0.1515559598936729,1:0.23067870296016596,2:0.32296928986482437,3:0.2947960472813368}
}
{0:0.19981883632445577,1:0.10001737914262077,2:0.4007629541294943,3:0.29940083040342913}
*/