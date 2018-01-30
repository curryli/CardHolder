package com.up.cardholderTag;

public class Driver {

	public static void main(String[] args) {
		
		try {
			if(args.length!=4)
				System.exit(1);
			/**
			 * args[0]:cupsdata
			 * args[1]:hotelTrans
			 * args[2]:customerRecord
			 * args[3]:startDate
			 * args[4]:endDate
			 * 
			 * */
			String[] transArgs = new String[4];
			transArgs[0] = args[0];
			transArgs[1] = args[1];
			transArgs[2] = args[2];
			transArgs[3] = args[3];
			
			System.exit(DeriveForHotelTrans.execute(transArgs) ? 0 : 1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
