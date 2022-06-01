import java.io.File
import java.io.FileWriter
import java.util.Scanner
import scala.util.Random
import scala.collection.mutable.ListBuffer
import java.time.LocalDate
import java.time.LocalTime
import java.time.temporal.ChronoUnit.DAYS
import scala.util.Random
import java.time.ZonedDateTime 
import java.util.Calendar
import java.time.LocalDateTime
import java.time.{DateTimeException, LocalTime, LocalDate}
import java.time.format.DateTimeFormatter
import java.util.UUID


object Database{
    def main(args:Array[String]):Unit = {
        var random = new Random
    
        //GET LOCAL TIME
        var time = LocalTime.now();
        var formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
       
        //READING CITY AND COUNTRY
        var city_country = new File("city_country.csv")
        var sc_city_country = new Scanner(city_country)
        var lst_city_country = new ListBuffer[String]()
        //ADD CITY AND COUNTRY TO LIST
        while(sc_city_country.hasNext) {
            lst_city_country += sc_city_country.nextLine
        }


        //READING ID AND NAME TO LIST
        var id_names = new File("names.csv")
        var sc_id_names = new Scanner(id_names)
        var lst_id_names = new ListBuffer[String]()
        //ADD ID AND NAMES TO LIST
         while(sc_id_names.hasNext) {
            lst_id_names += sc_id_names.nextLine
        }

        //PRODUCT iD, PRODUCT NAME, CATEGORY
        var product_name = new File("productname.csv")
        var sc_product_name = new Scanner(product_name)
        var lst_product_name = new ListBuffer[String]()
     
        while(sc_product_name.hasNext) {
            lst_product_name += sc_product_name.nextLine
        }

        //WEBSITE
        var websites = new File("Websites.csv")
        var sc_websites = new Scanner(websites)
        var lst_websites = new ListBuffer[String]()
        while(sc_websites.hasNext) {
            lst_websites += sc_websites.nextLine
        }
      
        //READING PAYMENT AND QTY
        var pay_qty_price = new File("payment_type_qty_price.csv")
        var sc_pay_qty_price = new Scanner(pay_qty_price)
        var lst_pay_qty_price = new ListBuffer[String]()
        //ADD CITY AND COUNTRY TO LIST
        while(sc_pay_qty_price.hasNext) {
            lst_pay_qty_price += sc_pay_qty_price.nextLine
        }

        //WEBSITE PAYMNET_ID PAY_SUCCESS PAY_FAIL
       
        var fields = List("ecommerce_website_name", "payment_txn_id", "payment_txn_success", "failure_reason")
        var payment_id = random.shuffle(10000 to 99999).toList
        var failed = List("Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N")
        var reasons = List("Insufficient Funds", "Invalid CVV", "Invalid Card #", "Expired Card", "Invalid Expiration Date", "Invalid Account #", "Limit Exceeded")
        var ifFailed = ""
        var failedReason = ""
       

        var out = new File("out.csv")
        var fw = new FileWriter(out, false)
    
        for(i <- 0 to 9999) {
            //GENERATE UNIQUE TIME ON EACH LOOP
            var uniqueTime = time.minusHours(1 + random.nextInt(10)).format(formatter)

            //GENERATE UNIQUE ORDER_ID, CUSTOMER_ID AND NAME
            var orderID = random.shuffle(10000 to 99999)
            var o = orderID(i)

            var custID = random.shuffle(10000 to 99999)
            var c = custID(i)

           //$orderID(i),$custID(i),
            var name=lst_id_names(random.nextInt(lst_id_names.length))

            //PRODUCT iD, PRODUCT NAME, CATEGORY
            var prod_id_prod_name_prod_cat = lst_product_name(random.nextInt(lst_product_name.length))

            //UNIQUE PAY QTY PRICE
            var lstPQP=lst_pay_qty_price(random.nextInt(lst_pay_qty_price.length)).split(",")
            var pay_type=lstPQP(0)
            var qty = lstPQP(1)
            var price = lstPQP(2)


            //UNIQUE CITY AND COUNTRY
            var lstCC = lst_city_country(random.nextInt(lst_city_country.length)).split(",")
            var city = lstCC(0)
            var country=lstCC(1)

            //WEBSITE 
            var web = lst_websites(random.nextInt(lst_websites.length))

            //PAYMENT_ID PAY_SUCCESS PAY_FAIL
            ifFailed = failed(random.nextInt(failed.length))
            
            if(ifFailed == "N"){
                failedReason = reasons(random.nextInt(reasons.length))
            } else {
                failedReason = " "
            }

            var payment_txn_id = payment_id(i)
            fw.write(s"$o,$c,$name,$prod_id_prod_name_prod_cat,$pay_type,$qty,$price,${random_date(LocalDate.of(2021, 1, 1), LocalDate.of(2022, 1, 1))} $uniqueTime,$country,$city,$web,$payment_txn_id,$ifFailed,$failedReason\n")        



         
         }

        //CLOSE SCANNERS
        sc_city_country.close()
        sc_pay_qty_price.close()
        sc_id_names.close()
        sc_product_name.close()
        sc_websites.close()
        fw.close()
    }

    //GENERATE DATE
    def random_date(from: LocalDate, to: LocalDate): LocalDate = {
        val diff = DAYS.between(from, to)
        val random = new Random(System.currentTimeMillis) // You may want a different seed
        from.plusDays(random.nextInt(diff.toInt))
    }
}

