hadoop jar ConsumptionScore.jar cardScore.ConsumptionDriven /user/hddtmn/in_common_his_trans xrli/CardholderTag/consumption_out xrli/CardholderTag/cardbin/inCardBin xrli/CardholderTag/cardbin/inPlatinumCardPath 20150101 20150201
#这边的文件路径都是HDFS上的

hadoop jar ConsumptionScore.jar cardScore.ScoreDriven xrli/CardholderTag/consumption_out xrli/CardholderTag/Score_out
