package cse512

import org.apache.spark.sql.SparkSession


object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(stand_Cont(pointString, queryRectangle)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(stand_Cont(pointString, queryRectangle)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(stand_With(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(stand_With(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
}

def calcEucld(coord_x2:Double,coord_x1:Double,coord_y1:Double,coord_y2:Double) : Double = {

    return scala.math.sqrt(scala.math.pow((coord_x2 - coord_x1), 2) + scala.math.pow((coord_y2 - coord_y1), 2))
  }

def check(dist_eucli:Double, distance: Double):Boolean=
  {
    return dist_eucli<=distance
  }
  

  def stand_With(pointString1:String, pointString2:String, distance:Double) : Boolean = {
    val coord2 = pointString2.split(",")
    val coord_x2 = coord2(0).trim().toDouble
    val coord_y2 = coord2(1).trim().toDouble

    val coord1 = pointString1.split(",")
    val coord_x1 = coord1(0).trim().toDouble
    val coord_y1 = coord1(1).trim().toDouble
    
    val dist_eucli = calcEucld(coord_x2,coord_x1,coord_y2,coord_y1)
    return check(dist_eucli,distance)
  }
def stand_Cont(pointString:String, queryRectangle:String) : Boolean = {
    val coord_rect = queryRectangle.split(",")
    val coord_rect_x1 = coord_rect(0).trim().toDouble
    val coord_rect_y1 = coord_rect(1).trim().toDouble
    val coord_rect_x2 = coord_rect(2).trim().toDouble
    val coord_rect_y2 = coord_rect(3).trim().toDouble

    val coord = pointString.split(",")
    val coord_x = coord(0).trim().toDouble
    val coord_y = coord(1).trim().toDouble
    
    
    

    var minimum_y: Double = 0
    var maximum_y: Double = 0
    
    if(coord_rect_y1 >= coord_rect_y2) {
      minimum_y = coord_rect_y2
      maximum_y = coord_rect_y1
      
    } else {
      minimum_y = coord_rect_y1
      maximum_y = coord_rect_y2
      
    }
    
    var minimum_x: Double = 0
    var maximum_x: Double = 0
    
    if(coord_rect_x1 >= coord_rect_x2) {
      minimum_x = coord_rect_x2
      maximum_x = coord_rect_x1
      
    } else {
      minimum_x = coord_rect_x1
      maximum_x = coord_rect_x2
      
    }
    
  
    
    if(coord_x >= minimum_x && coord_x <= maximum_x && coord_y >= minimum_y && coord_y <= maximum_y) {
      return true
    } else {
      return false
    }
  }

  
}
