package com.up.merchant.customerFlow;

public class CaclFeatureDriver {

	public static void main(String[] args) {
		
		try {
			//DeriveForHotelTrans.execute(args);
			//System.exit(DeriveForCustomerRecord.execute(args) ? 0 : 1);
			
			/**
			 * args[0]:joinData
			 * args[1]:Output
			 * 
			 * */
			System.exit(CaclCustomerFlowFeature.execute(args) ? 0 : 1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
