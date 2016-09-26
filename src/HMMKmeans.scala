import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeansModel
 
object HMMKmeans {
  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    // 设置运行环境
    val conf = new SparkConf().setAppName("Kmeans")
    val sc = new SparkContext(conf)
 
      // 装载数据集
    val data = sc.textFile("xrli/AmountDedup/*")
    val parsedData = data.map(s => Vectors.dense(s.toDouble))
    
    // 将数据集聚类，5个类，1000次迭代，进行模型训练形成数据模型
    val numClusters = 5
    val numIterations = 1000
    val model = KMeans.train(parsedData, numClusters, numIterations)
 
    model.save(sc, "xrli/kmeans_model");
    
    // 打印数据模型的中心点
    println("Cluster centers:")
    for (c <- model.clusterCenters) {
      println("  " + c.toString)
    }

    
  
  //  val modelnew = KMeansModel.load(sc, "xrli/kmeans_model")
    
    val originfile = sc.textFile("xrli/GetFromHive/*") 
    val sp = originfile.map{ line => 
      val fields = line.split("\\001")      
      val card = fields(0)
      val amount = model.predict(Vectors.dense(fields(1).toDouble))
      (card,amount)        
    }
    
    val listRDD = sp.combineByKey(
        (v : Int) => List(v),
        (c : List[Int], v : Int) => v :: c,
        (c1 : List[Int], c2 : List[Int]) => c1 ++ c2
    )
      //RDD(00017a3a14f2a77220a5788aaaba1d3d,List(0, 0, 0, 0, 0, 0, 0))
    
    val result  =  listRDD.map{case(key,value) => Array(key, value.mkString(",")).mkString(":")}
    
    result.repartition(1).saveAsTextFile("xrli/SeqDict")
    //00017a3a14f2a77220a5788aaaba1d3d:0,0,0,0,0,0,0
    
    //result.repartition(1).saveAsTextFile("xrli/testhmm2")  //合并为一个文件
    
    
    
    // 交叉评估，返回数据集和结果
//    val result2 = data.map {
//      line =>
//        val linevectore = Vectors.dense(line.toDouble)
//        val prediction = model.predict(linevectore)
//        line + " " + prediction
//    }.saveAsTextFile("xrli/price_kmeans.txt")
 
    sc.stop()
  }
}

