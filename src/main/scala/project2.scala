import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._


object project2 {
    def main(args:Array[String]):Unit = {
        val spark =
            SparkSession
            .builder
            .appName("SparkHelloWorld")
            .config("spark.master", "local")
            .config("spark.eventLog.enabled", "false")
            .getOrCreate()

        import spark.implicits._   
        import spark.sqlContext.implicits._ 

        //What is the top selling category of items? Per Country?
        //val topSellingCategory = topSel(spark)
        //topSellingCategory.show
       // topSellingCategory.write.csv("outputs/topSellingCategory")

        //How does the popularity of products change throughout the year? Per Country?
        val produtPopularityPerCountry = prodPop(spark).coalesce(6)
        //produtPopularityPerCountry.show
        produtPopularityPerCountry.write.csv("outputs/produtPopularityPerCountry")

        //Which locations see the highest traffic of sales?
        //val highestTraffic=highTruf(spark).coalesce(1)
        //highestTraffic.show
       //highestTraffic.write.csv("outputs/highestTraffic")
    
        //What times have the highest traffic of sales? Per Country?
        //val highestTime=timeHighest(spark).coalesce(1)
        //highestTime.show
      // highestTime.write.csv("outputs/highestTime")

        spark.stop()
    }

    def topSel(spark:SparkSession):DataFrame = {

        val csv_data = spark.read
            .format("csv")
            .option("inferSchema","true")
            .option("header", "true")
            .load("file.csv")
        val countries = Seq("Australia", "Germany", "China", "Canada", "Japan", "United States")
        //created sae DATA TYPE FOR COLUMNS
        val data = csv_data.withColumn("QTY", col("QTY").cast("int")).withColumn("Country", col("Country").cast("string")).withColumn("Product_Category", col("Product_Category").cast("string"))
        
        val cleanData=data.select("Product_Category","QTY","Country").filter(col("QTY").cast("int").isNotNull && col("Country").isin(countries:_*) && col("Product_Category").rlike("^[A-Z]") && col("Product_Category").cast("string") =!= "NULL") 
        val maxQTY = cleanData.select("QTY","Country").groupBy("Country").max("QTY").as("max")
    
        cleanData.createOrReplaceTempView("cleanData")
        maxQTY.createOrReplaceTempView("maxQTY")

        val maxProdPerCountry = spark.sql("SELECT `Product_Category`, sum(`QTY`),`Country` FROM cleanData WHERE (`QTY`,`Country`) IN (SELECT `max(QTY)`,`Country` FROM maxQTY) group by `Country`, `Product_Category` order by `Country`")
        
        //REMOVE PLURAL 
        val rdd1=maxProdPerCountry.rdd.map(_.toSeq.toArray)
        val rdd2 = rdd1.map(x => {
            var cat = x(0).toString()
            var q = x(1).toString()
            var country = x(2).toString()
            if(cat.split("")(cat.length-1) ==  "s") {(cat.slice(0, cat.length-1), q, country)}
            else {(cat, q, country)}
        })


        val rdd3 = spark.createDataFrame(rdd2).toDF("Category", "QTY", "Country")
        rdd3.createOrReplaceTempView("rdd3")
        val finalQ = spark.sql("SELECT `Category`, sum(`QTY`) as qty,`Country` FROM rdd3 group by `Country`, `Category` order by `Country`")
        finalQ.createOrReplaceTempView("finalQ")
        val justMax = spark.sql("SELECT `Country`, MAX(`qty`) as qty from finalQ group by `Country`")
        justMax.createOrReplaceTempView("justMax")

        val justMaxWithProduct = spark.sql("SELECT `Country`, `qty`, `Category` from finalQ where (`Country`, `qty`) in (select * from justMax)")

    return justMaxWithProduct
    }

    def prodPop(spark:SparkSession):DataFrame ={

          val csv_data = spark.read
            .format("csv")
            .option("inferSchema","true")
            .option("header", "true")
            .load("file.csv")


        val countries = Seq("Australia", "Germany", "China", "Canada", "Japan", "United States")
        //created sae DATA TYPE FOR COLUMNS
        val data = csv_data.withColumn("QTY", col("QTY").cast("int"))
        .withColumn("Country", col("Country").cast("string"))
        .withColumn("Product_Name", col("Product_Name").cast("string"))
        
        val cleanData=data.select("Country", "Product_Name", "Datetime", "QTY")
            .filter(
                col("QTY").cast("int").isNotNull && 
                col("Country").isin(countries:_*) && 
                col("Product_Name").rlike("^[A-Z]") && 
                col("Product_Name").cast("string") =!= "NULL"
                //col("Datetime").isNull 
                //col("Datetime").rlike("^[0-9]") &&
                //col("Datetime").cast("string") == "null"
                )
            .sort("Datetime")
        cleanData.createOrReplaceTempView("cleanData")
        
        val dateConverted = spark.sql("select `Country`, `Product_Name`, to_date(`Datetime`, 'MM-dd-yyyy') as date, `QTY` from cleanData order by `date`")
        dateConverted.createOrReplaceTempView("dateConverted") 
        val removeNull  = spark.sql("select * from dateConverted where `date` is NOT NULL")
        removeNull.createOrReplaceTempView("removeNull") 
        val groupedByMonth = spark.sql("SELECT `Country`, `Product_Name`, min(`date`), MONTH(`date`) as month, sum(`QTY`) from removeNull group by `Country`, `Product_Name`, `month` order by `Country`,`Product_Name`, `month`")
        
        // val groupedData = dateConverted.select("Country", "Product_Name", "", "QTY").groupBy("Country", "Product_Name", "Datetime").max("QTY").sort("Country", "Product_Name", "Datetime")
        // groupedData.createOrReplaceTempView("groupedData")
        // val cutYear = groupedData.select(
        //         col("Country"),
        //         col("Product_Name"), 
        //         col("Datetime"),
        //         to_date(col("Datetime"), "MM-dd-yyyy")
        //         .as("date"),
        //         col("max(QTY)")
        //         )

        // cutYear.createOrReplaceTempView("cutYear")
        // val groupedByMonth = spark.sql("SELECT `Country`, `Product_Name`, MONTH(`date`) as month, sum(`max(QTY)`) from cutYear group by `Country`, `Product_Name`, `month` order by `Country`,`Product_Name`, `month`")
        
        
        // val dateConverted = spark.sql("select `Country`, `Product_Name`, date_format(to_date(`Datetime`, 'dd-MM-yy'), 'dd-MM-yy') as date, `QTY` from cleanData ")
        // dateConverted.createOrReplaceTempView("dateConverted")
        // val groupedData = dateConverted.select("Country", "Product_Name", "date", "QTY").groupBy("Country", "Product_Name", "date").max("QTY").sort("Country", "Product_Name", "date")
        // groupedData.createOrReplaceTempView("groupedData")
       
        // val groupedByMonth = spark.sql("SELECT `Country`, `Product_Name`, min(`date`), MONTH(`date`) as month, sum(`max(QTY)`) from groupedData group by `Country`, `Product_Name`, `month` order by `Country`,`Product_Name`, `month`")
        
    return groupedByMonth
    }

    def highTruf(spark:SparkSession):DataFrame ={

          val csv_data = spark.read
            .format("csv")
            .option("inferSchema","true")
            .option("header", "true")
            .load("file.csv")


        val countries = Seq("Australia", "Germany", "China", "Canada", "Japan", "United States")
        //created sae DATA TYPE FOR COLUMNS
        val data = csv_data.withColumn("Country", col("Country").cast("string"))
        .withColumn("City", col("City").cast("string"))
        .withColumn("QTY", col("QTY").cast("int"))
   
        val cleanData=data.select("Country", "City", "QTY")
            .filter(
                col("QTY").cast("int").isNotNull && 
                col("Country").isin(countries:_*) && 
                col("City").rlike("^[A-Z]")
                )
            .sort("Country")
        
        val groupedData = cleanData.select("Country", "City", "QTY")
            .groupBy("Country", "City").sum("QTY").sort(desc("sum(QTY)"))
        
        return groupedData
    }

    def timeHighest(spark:SparkSession):DataFrame ={
        val csv_data = spark.read
            .format("csv")
            .option("inferSchema","true")
            .option("header", "true")
            .load("file.csv")

        val countries = Seq("Australia", "Germany", "China", "Canada", "Japan", "United States")
        //created sae DATA TYPE FOR COLUMNS
        val data = csv_data.withColumn("QTY", col("QTY").cast("int"))
        .withColumn("Country", col("Country").cast("string"))
        
        val cleanData=data.select("Country", "Datetime", "QTY")
            .filter(
                col("QTY").cast("int").isNotNull && 
                col("Country").isin(countries:_*) && 
                col("Datetime").isNotNull &&
                col("Datetime").rlike("^[0-9]")
                )
        cleanData.createOrReplaceTempView("cleanData")
        val dateConverted = spark.sql("select `Country`, date_format(to_timestamp(`Datetime`, 'dd-MM-yy HH:mm'), 'yyyy-MM-dd HH:mm') as date, `QTY` from cleanData ")
        
        dateConverted.createOrReplaceTempView("dateConverted")
        val groupedHour = spark.sql("SELECT `Country`, MIN(`date`), HOUR(`date`) as hour, sum(`QTY`) as sum from dateConverted WHERE `date` IS NOT NULL group by `Country`, `hour` order by  `Country` asc, `sum` desc ")
        
    return groupedHour
    
    }
}
