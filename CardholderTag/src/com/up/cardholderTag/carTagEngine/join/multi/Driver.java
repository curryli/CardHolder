package com.up.cardholderTag.carTagEngine.join.multi;

public class Driver {

	public static void main(String[] args) {
		 
		try {
			if(args.length!=6)
				System.exit(1);
			/**
			 * args[0]:baseDir
			 * args[1]:carDir
			 * args[2]:entmtDir
			 * args[3]:consumptionDir
			 * args[4]:tempOutput
			 * args[5]:finalOutput
			 * 
			 * */
			String[] transArgs = new String[4];
			transArgs[0] = args[0];
			transArgs[1] = args[1];
			transArgs[2] = args[2];
			transArgs[3] = args[4];
			TagListJoinFirst.execute(transArgs);
			
			String[] secondArgs = new String[3];
			secondArgs[0] = args[4];
			secondArgs[1] = args[3];
			secondArgs[2] = args[5];
			System.exit(TagListJoinSecond.execute(secondArgs) ? 0 : 1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
