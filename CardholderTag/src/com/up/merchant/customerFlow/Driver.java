package com.up.merchant.customerFlow;

public class Driver {

	public static void main(String[] args) {
		
		try {
			if(args.length!=8)
				System.exit(1);
			/**
			 * args[0]:cupsdata
			 * args[1]:hotelTrans
			 * args[2]:customerRecord
			 * args[3]:filterOuput
			 * args[4]:joinOutput
			 * args[5]:startDate
			 * args[6]:endDate
			 * args[7]:featureOutput
			 * 
			 * */
			String[] transArgs = new String[4];
			transArgs[0] = args[0];
			transArgs[1] = args[1];
			transArgs[2] = args[5];
			transArgs[3] = args[6];
			DeriveForHotelTrans.execute(transArgs);
			
			String[] customerRecordArgs = new String[2];
			customerRecordArgs[0] = args[1];
			customerRecordArgs[1] = args[2];
			DeriveForCustomerRecord.execute(customerRecordArgs);
			
			/**
			 * args[0]:hotelTrans
			 * args[1]:filterOutput
			 * args[2]:customerRecord
			 * args[3]:joinOutput
			 * 
			 * */
			String[] filterArgs = new String[2];
			filterArgs[0] = args[1];
			filterArgs[1] = args[3];
			MiddleMerchantFilter.execute(filterArgs);
			
			String[] joinArgs = new String[3];
			joinArgs[0] = args[3];
			joinArgs[1] = args[2];
			joinArgs[2] = args[4];
			MiddleCustomerRecordJoin.execute(joinArgs);
			
			String[] caclFeatureArgs = new String[2];
			caclFeatureArgs[0] = args[4];
			caclFeatureArgs[1] = args[7];
			System.exit(CaclCustomerFlowFeature.execute(args) ? 0 : 1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
