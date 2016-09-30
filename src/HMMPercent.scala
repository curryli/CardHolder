

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeansModel
 
object HMMPercent {
  
  def rangeDict(amount:Double, Q:Array[Double]):Int ={
    var level=999
    for (i <- 0 to Q.length-2) {
       if(Q(i)<=amount && amount<Q(i+1))
        level=i
    }
    
    if(amount>=Q(Q.length-1))
      level= Q.length-1
      return level
  }

 
  
  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    // 设置运行环境
    val conf = new SparkConf().setAppName("HMMPercent")
    val sc = new SparkContext(conf)
 
      // 装载数据集
    val data = sc.textFile("xrli/AmountDedup/*")
    val amountList = sc.makeRDD(data.map(_.toDouble).collect)
    
    //val amountList=sc.makeRDD(Array(1,3,5,7,9,11,13,15))
    val amountSorted = amountList.sortBy(identity).zipWithIndex().map {
     case (v, idx) => (idx, v)
    }
    
    
    val count = amountSorted.count() 
    val splitnum = 4
    val Q = new Array[Double](splitnum)

    
    //求splitnum分位数
    Q(0) = 0;
    for (i <- 1 to splitnum-1) {
        Q(i) = if (count % splitnum*i == 0) {
                  val l = count/splitnum*i - 1
                  val r = l + 1
                 (amountSorted.lookup(l).head + amountSorted.lookup(r).head).toDouble / 2
                 } else
                  amountSorted.lookup(count/splitnum*i).head.toDouble     
    }
    


  
  //  val modelnew = KMeansModel.load(sc, "xrli/kmeans_model")
    
    val originfile = sc.textFile("xrli/GetFromHiveFile") 
    val sp = originfile.map{ line => 
    val fields = line.split("\\001") 
    if(fields.length==3 && !fields(1).contains("N")){                      //考虑数据不完整性以及NAN的数字情况
      val card = fields(0)
      val amount = rangeDict(fields(1).toDouble,Q)
         (card,amount)
      }
      else
         ("",0)
    }
    
    val listRDD = sp.combineByKey(
        (v : Int) => List(v),
        (c : List[Int], v : Int) => v :: c,
        (c1 : List[Int], c2 : List[Int]) => c1 ++ c2
    )
      //RDD(00017a3a14f2a77220a5788aaaba1d3d,List(0, 0, 0, 0, 0, 0, 0))
    
    val result  =  listRDD.map{case(key,value) => Array(key, value.mkString(",")).mkString(":")}
    
    //println("tran ino string done.")
    
    result.saveAsTextFile("xrli/SeqDict4")
    //result.repartition(1).saveAsTextFile("xrli/NewSeqDict")
    //println("successfully.")
    sc.stop()
  }
  
}

