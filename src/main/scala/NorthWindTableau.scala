import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.commons.io.FilenameUtils
import scala.collection.mutable.Map
import org.apache.hadoop.fs.{FileSystem,Path}
/**
  * Created by Swapna on 12/18/2017.
  */
object NorthWindTableau {

  def main(args: Array[String]): Unit = {

    /*******************************************************
      * Log 4 J Configuration Properties
      *******************************************************/
    import org.apache.log4j.{Level, Logger}

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    /*******************************************************
      * Initializing Spark Session
      ******************************************************/
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("NorthWindApplication")
      .config("spark.cassandra.connection.host", "10.1.51.42")
      .getOrCreate()

    /*******************************************************
      * passing arguments
      ******************************************************/
    val dir =  args(0).toString


    /*******************************************************
      * Creating dataframes for all files
      ******************************************************/

    FileSystem.get( spark.sparkContext.hadoopConfiguration ).listStatus( new Path(dir)).foreach( x => {
      val df = spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .load(x.getPath.toString)
      val fileName=(x.getPath.getName)
      val fileNameWithoutExt = FilenameUtils.removeExtension(fileName)
      df.createOrReplaceTempView(fileNameWithoutExt)
      println("created view for "+ fileNameWithoutExt)
    }
    )

        /*******************************************************
          * Aggregations on Views
          ******************************************************/

        println("++++++++printing Customer_and_Suppliers_by_CityDF +++++++++++++++")
        val Customer_and_Suppliers_by_CityDF=spark.sql("SELECT city, companyname, contactname, 'Customers' AS relationship FROM Customers UNION SELECT city, companyname, contactname, 'Suppliers'FROM Suppliers")
        Customer_and_Suppliers_by_CityDF.write.mode("overwrite").saveAsTable("Customer_and_Suppliers_by_City")
        Customer_and_Suppliers_by_CityDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "customer_and_suppliers_by_city")).save()

        println("++++++++printing Alphabetical_list_of_productsDF +++++++++++++++")
        val Alphabetical_list_of_productsDF=spark.sql("SELECT Products.productid,Products.productname,Products.supplierid,Products.categoryid,Products.quantityperunit,Products.unitprice,Products.unitsinstock,Products.unitsonorder,Products.reorderlevel,Products.discontinued, Categories.categoryname FROM Categories INNER JOIN Products ON Categories.categoryid = Products.categoryid WHERE (((Products.discontinued)=0))")
        Alphabetical_list_of_productsDF.write.mode("overwrite").saveAsTable("Alphabetical_list_of_products")
        Alphabetical_list_of_productsDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "alphabetical_list_of_products")).save()

        println("++++++++printing Current_Product_ListDF +++++++++++++++")
        val Current_Product_ListDF=spark.sql("SELECT Product_List.productid, Product_List.productname FROM Products AS Product_List WHERE (((Product_List.discontinued)=0))")
        Current_Product_ListDF.write.mode("overwrite").saveAsTable("Current_Product_List")
        Current_Product_ListDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "product_list")).save()

        println("++++++++printing Orders_QryDF +++++++++++++++")
        val Orders_QryDF=spark.sql("SELECT Orders.orderid, Orders.customerid, Orders.employeeid, Orders.orderdate, Orders.requireddate, Orders.shippeddate, Orders.shipvia, Orders.freight, Orders.shipname, Orders.shipaddress, Orders.shipcity, Orders.shipregion, Orders.shippostalcode, Orders.shipcountry, Customers.companyname, Customers.address, Customers.city, Customers.region, Customers.postalcode, Customers.country FROM Customers INNER JOIN Orders ON Customers.customerid = Orders.customerid")
        Orders_QryDF.write.mode("overwrite").saveAsTable("Orders_Qry")
        Orders_QryDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "orders_qry")).save()

        println("++++++++printing Products_Above_Average_PriceDF +++++++++++++++")
        val Products_Above_Average_PriceDF=spark.sql("SELECT Products.productname, Products.unitprice FROM Products WHERE Products.unitprice>(SELECT AVG(unitprice) From Products)")
        Products_Above_Average_PriceDF.write.mode("overwrite").saveAsTable("Products_Above_Average_Price")
        Products_Above_Average_PriceDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "products_above_average_price")).save()

        println("++++++++printing Products_by_CategoryDF +++++++++++++++")
        val Products_by_CategoryDF=spark.sql("SELECT Categories.categoryname, Products.productname, Products.quantityperunit, Products.unitsinstock, Products.discontinued FROM Categories INNER JOIN Products ON Categories.categoryid = Products.categoryid WHERE Products.discontinued <> 1 ")
        Products_by_CategoryDF.write.mode("overwrite").saveAsTable("Products_by_Category")
        Products_by_CategoryDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "products_by_category")).save()

        println("++++++++printing Quarterly_OrdersDF +++++++++++++++")
        val Quarterly_OrdersDF=spark.sql("SELECT DISTINCT Customers.customerid, Customers.companyname, Customers.city, Customers.country FROM Customers RIGHT JOIN Orders ON Customers.customerid = Orders.customerid WHERE Orders.orderdate BETWEEN '1997-01-01' And '1997-12-31'")
        Quarterly_OrdersDF.write.mode("overwrite").saveAsTable("Quarterly_Orders")
        Quarterly_OrdersDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "quarterly_orders")).save()

        println("++++++++printing InvoicesDF +++++++++++++++")
        val InvoicesDF=spark.sql("SELECT Orders.shipname, Orders.shipaddress, Orders.shipcity, Orders.shipregion, Orders.shippostalcode, Orders.shipcountry, Orders.customerid, Customers.companyname AS customername, Customers.address, Customers.city, Customers.region, Customers.postalcode, Customers.country, (firstname + ' ' + lastname) AS salesperson, Orders.orderid, Orders.orderdate, Orders.requireddate, Orders.shippeddate, Shippers.companyname As shippername, OrderDetails.productid, Products.productname, OrderDetails.unitprice, OrderDetails.quantity, OrderDetails.discount, ((OrderDetails.unitprice*quantity*(1-discount)/100)*100) AS extendedprice, Orders.freight FROM Shippers INNER JOIN (Products INNER JOIN ((Employees INNER JOIN (Customers INNER JOIN Orders ON Customers.customerid = Orders.customerid) ON Employees.employeeid = Orders.employeeid) INNER JOIN OrderDetails ON Orders.orderid = OrderDetails.orderid) ON Products.productid = OrderDetails.productid) ON Shippers.shipperid = Orders.shipvia")
        InvoicesDF.write.mode("overwrite").saveAsTable("Invoices")
        InvoicesDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "invoices")).save()

        println("++++++++printing Order_Details_ExtendedDF +++++++++++++++")
        val Order_Details_ExtendedDF=spark.sql("SELECT OrderDetails.orderid, OrderDetails.productid, Products.productname, OrderDetails.unitprice, OrderDetails.quantity, OrderDetails.discount,((OrderDetails.unitprice*quantity*(1-discount)/100)*100) AS extendedprice FROM Products INNER JOIN OrderDetails ON Products.productid = OrderDetails.productid")
        Order_Details_ExtendedDF.createOrReplaceTempView("Order_Details_Extended")
        Order_Details_ExtendedDF.write.mode("overwrite").saveAsTable("Order_Details_Extended")
        Order_Details_ExtendedDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "order_details_extended")).save()

        println("++++++++printing Order_SubtotalsDF +++++++++++++++")
        val Order_SubtotalsDF=spark.sql("SELECT OrderDetails.orderid, Sum((OrderDetails.unitprice*quantity*(1-discount)/100)*100) AS subtotal FROM OrderDetails GROUP BY OrderDetails.orderid")
        Order_SubtotalsDF.createOrReplaceTempView("Order_Subtotals")
        Order_SubtotalsDF.write.mode("overwrite").saveAsTable("Order_Subtotals")
        Order_SubtotalsDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "order_subtotals")).save()

        println("++++++++printing Product_Sales_for_1997DF +++++++++++++++")
        val Product_Sales_for_1997DF=spark.sql(" SELECT Categories.categoryname, Products.productname, Sum((OrderDetails.unitprice*quantity*(1-discount)/100)*100) AS productsales FROM (Categories INNER JOIN Products ON Categories.categoryid = Products.categoryid) INNER JOIN (Orders INNER JOIN OrderDetails ON Orders.orderid = OrderDetails.orderid) ON Products.productid = OrderDetails.productid WHERE (((Orders.shippeddate) Between '1997-01-01' And '1997-12-31')) GROUP BY Categories.categoryname, Products.productname")
        Product_Sales_for_1997DF.createOrReplaceTempView("Product_Sales_for_1997")
        Product_Sales_for_1997DF.write.mode("overwrite").saveAsTable("Product_Sales_for_1997")
        Product_Sales_for_1997DF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "product_sales_for_1997")).save()

        println("++++++++printing Category_Sales_for_1997DF +++++++++++++++")
        val Category_Sales_for_1997DF=spark.sql("SELECT Product_Sales_for_1997.categoryname, Sum(Product_Sales_for_1997.productsales) AS categorysales FROM Product_Sales_for_1997 GROUP BY Product_Sales_for_1997.categoryname")
        Category_Sales_for_1997DF.write.mode("overwrite").saveAsTable("Category_Sales_for_1997")
        Category_Sales_for_1997DF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "category_sales_for_1997")).save()

        println("++++++++printing Sales_by_CategoryDF +++++++++++++++")
        val Sales_by_CategoryDF=spark.sql("SELECT Categories.categoryid, Categories.categoryname, Products.productname, Sum(Order_Details_Extended.extendedprice) AS productsales FROM Categories INNER JOIN (Products INNER JOIN (Orders INNER JOIN Order_Details_Extended ON Orders.orderid = Order_Details_Extended.orderid) ON Products.productid = Order_Details_Extended.productid) ON Categories.categoryid = Products.categoryid WHERE Orders.orderdate BETWEEN '1997-01-01' And '1997-12-31' GROUP BY Categories.categoryid, Categories.categoryname, Products.productname")
        Sales_by_CategoryDF.write.mode("overwrite").saveAsTable("Sales_by_Category")
        Sales_by_CategoryDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "sales_by_category")).save()

        println("++++++++printing Sales_Totals_by_AmountDF +++++++++++++++")
        val Sales_Totals_by_AmountDF=spark.sql("SELECT Order_Subtotals.subtotal AS saleamount, Orders.orderid, Customers.companyname, Orders.shippeddate FROM Customers INNER JOIN (Orders INNER JOIN Order_Subtotals ON Orders.orderid = Order_Subtotals.orderid) ON Customers.customerid = Orders.customerid WHERE (Order_Subtotals.subtotal >2500) AND (Orders.shippeddate BETWEEN '1997-01-01' And '1997-12-31')")
        Sales_Totals_by_AmountDF.write.mode("overwrite").saveAsTable("Sales_Totals_by_Amount")
        Sales_Totals_by_AmountDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "sales_totals_by_amount")).save()

        println("++++++++printing Summary_of_Sales_by_QuarterDF +++++++++++++++")
        val Summary_of_Sales_by_QuarterDF=spark.sql("SELECT Orders.shippeddate, Orders.orderid, Order_Subtotals.subtotal FROM Orders INNER JOIN Order_Subtotals ON Orders.orderid = Order_Subtotals.orderid WHERE Orders.shippeddate IS NOT NULL")
        Summary_of_Sales_by_QuarterDF.write.mode("overwrite").saveAsTable("Summary_of_Sales_by_Quarter")
        Summary_of_Sales_by_QuarterDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "summary_of_sales_by_quarter")).save()

        println("++++++++printing Summary_of_Sales_by_YearDF +++++++++++++++")
        val Summary_of_Sales_by_YearDF=spark.sql("SELECT Orders.shippeddate, Orders.orderid, Order_Subtotals.subtotal FROM Orders INNER JOIN Order_Subtotals ON Orders.orderid = Order_Subtotals.orderid WHERE Orders.shippeddate IS NOT NULL")
        Summary_of_Sales_by_YearDF.write.mode("overwrite").saveAsTable("Summary_of_Sales_by_Year")
        Summary_of_Sales_by_YearDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "northwind", "table" -> "summary_of_sales_by_year")).save()

    }

}
