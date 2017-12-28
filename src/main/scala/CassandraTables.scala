import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf

/**
  * Created by Swapna on 12/19/2017.
  */
  object CassandraTables {

    def main(args: Array[String]) {

      val appName="CassandraTable"

      //Initialising Spark Context
      val conf=new SparkConf()
        .setAppName(appName)
        .setMaster("local[*]")
        .set("spark.cassandra.connection.host", "10.1.51.42")//"10.1.51.42"


      //Initialising Cassandra Connector
      val cassandraConnector = CassandraConnector.apply(conf)

      cassandraConnector.withSessionDo(session => {
        //Creating Keyspace northwind in Cassandra
        session.execute(s"""DROP KEYSPACE IF EXISTS northwind;""")
        session.execute("CREATE KEYSPACE IF NOT EXISTS northwind WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} ")

        //Creating table for Customer_and_Suppliers_by_City
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Customer_and_Suppliers_by_City (
						City text,
						CompanyName text,
						ContactName text,
						Relationship text,
						PRIMARY KEY (City,CompanyName,ContactName,Relationship)
					)
					;""")

        //Creating table for Alphabetical list of products
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Alphabetical_list_of_products (
							ProductID  int,
							ProductName text,
							SupplierID int,
							CategoryID int,
							QuantityPerUnit text,
							UnitPrice float,
							UnitsInStock int,
							UnitsOnOrder int,
							ReorderLevel int,
							Discontinued boolean,
							CategoryName text,
							PRIMARY KEY (ProductID, CategoryID,SupplierID)
					)
					;""")

        //Creating table for Product List
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Product_List (
						ProductID int PRIMARY KEY,
						ProductName text
					)
					;""")

        //Creating table for Orders Qry
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Orders_Qry (
						OrderID int,
						CustomerID text,
						EmployeeID int,
						OrderDate date,
						RequiredDate date,
						ShippedDate date,
						ShipVia int,
						Freight float,
						ShipName text,
						ShipAddress text,
						ShipCity text,
						ShipRegion text,
						ShipPostalCode text,
						ShipCountry text,
						CompanyName text,
						Address text,
						City text,
						Region text,
						PostalCode text,
						Country text,
						PRIMARY KEY (OrderID, CustomerID,EmployeeID)
					)
					;""")

        //Creating table for Products Above Average Price
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Products_Above_Average_Price (
						ProductName text PRIMARY KEY,
						UnitPrice float
					)
					;""")

        //Creating table for Products by Category
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Products_by_Category (
						CategoryName text,
						ProductName text,
						QuantityPerUnit text,
						UnitsInStock int,
						Discontinued boolean,
						PRIMARY KEY(CategoryName,ProductName)
					)
					;""")

        //Creating table for Quarterly Orders
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Quarterly_Orders (
						CustomerID text PRIMARY KEY,
						CompanyName text,
						City text,
						Country text
					)
					;""")


        //Creating table for Invoices
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Invoices (
						ShipName text,
						ShipAddress text,
						ShipCity text,
						ShipRegion text,
						ShipPostalCode text,
						ShipCountry text,
						CustomerID text,
						CustomerName text,
						Address text,
						City text,
						Region text,
						PostalCode text,
						Country text,
						Salesperson text,
						OrderID int,
						OrderDate date,
						RequiredDate date,
						ShippedDate date,
						ShipperName text,
						ProductID int,
						ProductName text,
						UnitPrice float,
						Quantity int,
						Discount float,
						ExtendedPrice float,
						Freight float,
						PRIMARY KEY (CustomerID,OrderID,ProductID)
					)
					;""")

        //Creating table for Order Details Extended
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Order_Details_Extended (
						OrderID int,
						ProductID int,
						ProductName text,
						UnitPrice float,
						Quantity int,
						Discount float,
						ExtendedPrice float,
						PRIMARY KEY (OrderID, ProductID)
					)
					;""")

        //Creating table for Order Subtotals
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Order_Subtotals (
						OrderID int PRIMARY KEY,
						Subtotal float
					)
					;""")

        //Creating table for Product Sales for 1997
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Product_Sales_for_1997 (
						CategoryName text,
						ProductName text,
						ProductSales float,
						PRIMARY KEY (CategoryName,ProductName)
					)
					;""")

        //Creating table for Category Sales for 1997
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Category_Sales_for_1997 (
						CategoryName text PRIMARY KEY,
						CategorySales float
					)
					;""")


        //Creating table for Sales by Category
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Sales_by_Category (
						CategoryID int PRIMARY KEY,
						CategoryName text,
						ProductName text,
						ProductSales float
					)
					;""")

        //Creating table for Sales Totals by Amount
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Sales_Totals_by_Amount (
						SaleAmount float,
            OrderID int PRIMARY KEY,
						CompanyName text,
						ShippedDate date
					)
					;""")


        //Creating table for Summary of Sales by Quarter
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Summary_of_Sales_by_Quarter(
						ShippedDate date,
						OrderID int PRIMARY KEY,
						Subtotal float
					)
					;""")

        //Summary of Sales by Year
        session.execute(s"""CREATE TABLE IF NOT EXISTS northwind.Summary_of_Sales_by_Year (
						ShippedDate date,
						OrderID int PRIMARY KEY,
						Subtotal float
					)
					;""")

        //Truncating the records
        session.execute(s"""TRUNCATE TABLE northwind.Customer_and_Suppliers_by_City""")
        session.execute(s"""TRUNCATE TABLE northwind.Alphabetical_list_of_products""")
        session.execute(s"""TRUNCATE TABLE northwind.Product_List""")
        session.execute(s"""TRUNCATE TABLE northwind.Orders_Qry""")
        session.execute(s"""TRUNCATE TABLE northwind.Products_Above_Average_Price""")
        session.execute(s"""TRUNCATE TABLE northwind.Products_by_Category""")
        session.execute(s"""TRUNCATE TABLE northwind.Quarterly_Orders;""")
        session.execute(s"""TRUNCATE TABLE northwind.Invoices""")
        session.execute(s"""TRUNCATE TABLE northwind.Order_Details_Extended""")
        session.execute(s"""TRUNCATE TABLE northwind.Order_Subtotals""")
        session.execute(s"""TRUNCATE TABLE northwind.Product_Sales_for_1997""")
        session.execute(s"""TRUNCATE TABLE northwind.Category_Sales_for_1997""")
        session.execute(s"""TRUNCATE TABLE northwind.Sales_by_Category""")
        session.execute(s"""TRUNCATE TABLE northwind.Sales_Totals_by_Amount""")
        session.execute(s"""TRUNCATE TABLE northwind.Summary_of_Sales_by_Quarter""")
        session.execute(s"""TRUNCATE TABLE northwind.Summary_of_Sales_by_Year""")
      }
      )
      System.out.println("==================================================================================")

//
    }



}
