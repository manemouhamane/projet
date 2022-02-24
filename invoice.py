from pyspark.sql import SparkSession
import json
import pandas
import unittest
import pymongo

class Invoice:
    
    spark=None
    pdf=None
    df=None
    
    
    def __init__(self, path,uri):
        
    
        self.spark = SparkSession.\
        builder.\
        appName("pyspark-notebook2").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "1g").\
        config("spark.mongodb.input.uri",uri).\
        config("spark.mongodb.output.uri",uri).\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").\
        getOrCreate()
        
        self.pdf = pandas.read_excel(path)
    
    
    def insertDatabase(self,nameDataBase, nameCollection):
        """
        Cette methode permet d'inserrer en base les données d'un fichier excel
        """
        
        datas=[]
        for i in self.pdf.index:
            data="{"+"'InvoiceNo': {}, 'StockCode': '{}', 'Description': '{}', 'Quantity': {}, 'InvoiceDate': '{}', 'UnitPrice': {}, 'CustomerID': {}, 'Country': '{}'".format(self.pdf[self.pdf.columns[0]][i],self.pdf[self.pdf.columns[1]][i],self.pdf[self.pdf.columns[2]][i],self.pdf[self.pdf.columns[3]][i],self.pdf[self.pdf.columns[4]][i],self.pdf[self.pdf.columns[5]][i],self.pdf[self.pdf.columns[6]][i],self.pdf[self.pdf.columns[7]][i])
            data=data+"}"
            datas.append(data)
        
        self.df = self.spark.read.json(self.spark.sparkContext.parallelize(datas))
        display(self.df)


        self.df.write.format("mongo").mode("append").option("database",nameDataBase).option("collection", nameCollection).save()
        
        return str(len(datas))

    def groupAllTransactionsByInvoice(self,uri):
        """
        Cette methode permet de retourner le nombre de transaction par facture
        """
        invoices = self.spark.read.format("mongo").option("uri",uri).load()
        invoices.createOrReplaceTempView('invoices')
        invoices = self.spark.sql("SELECT InvoiceNo, count(*) FROM invoices group by InvoiceNo")
        print(" Le nombre de transaction par facture")
        invoices.show()
        return invoices
        

    def whichProductSoldTheMost(self,uri):
        """
        Cette methode permet de retourner le nom du produit le plus vendu
        """
        invoices = self.spark.read.format("mongo").option("uri",uri).load()
        invoices.createOrReplaceTempView('invoices')
        invoices = self.spark.sql("select max(Quantity) as Quantity from (SELECT StockCode, Sum(Quantity) as Quantity FROM invoices group by StockCode)  ")

        for col in invoices.collect():
            value =col["Quantity"]
    
        invoices = self.spark.sql("SELECT StockCode, Sum(Quantity) as Quantity FROM invoices group by StockCode  ")
        product = invoices.where("Quantity="+str(value))

        for col in product.collect():
            nameProduct =col["StockCode"]

        print("Le produit le plus vendu est {} ".format(nameProduct))
        
        return nameProduct 
    
    
    def whichCustomerSpentTheMostMoney(self,uri):
        """
        Cette methode permet de retourner l'id du client qui a depensé le plus
        """
        invoices = self.spark.read.format("mongo").option("uri",uri).load()
        invoices.createOrReplaceTempView('invoices')
    
    
        invoices = self.spark.sql("SELECT CustomerID, sum(Quantity*UnitPrice) as total  FROM invoices group by CustomerID having total =(select max(total) as total from (SELECT CustomerID, sum(Quantity*UnitPrice) as total  FROM invoices group by CustomerID))")
        invoices.show()

        for col in invoices.collect():
            customerID =col["CustomerID"]

        print("L'Id du client qui a depensé le plus est {} et a depensé {} ".format(customerID, value))
        return customerID


class TestInvoice(unittest.TestCase):
  
    
    def test_insertDatabase(self):
        myclient = pymongo.MongoClient("mongodb://mongo1:27017")
        mydb = myclient["baseInvoice"]
        mycol = mydb["invoices"]
        mycol.drop()
        Invoice.__init__(self,"Online Retail.xlsx","mongodb://mongo1:27017")
        self.assertEqual(Invoice.insertDatabase(self,"baseInvoice","invoices"), str(541909))

    def test_groupAllTransactionsByInvoice(self):
        Invoice.__init__(self,"Online Retail.xlsx","mongodb://mongo1:27017")
        self.assertNotEqual(Invoice.groupAllTransactionsByInvoice(self,"mongodb://mongo1:27017/baseInvoice.invoices"),None)

    def test_whichProductSoldTheMost(self):
        Invoice.__init__(self,"Online Retail.xlsx","mongodb://mongo1:27017")
        self.assertEqual(Invoice.whichProductSoldTheMost(self,"mongodb://mongo1:27017/baseInvoice.invoices"),str("23843"))
    
    def test_whichProductSoldTheMost(self):
        Invoice.__init__(self,"Online Retail.xlsx","mongodb://mongo1:27017")
        self.assertEqual(Invoice.whichCustomerSpentTheMostMoney(self,"mongodb://mongo1:27017/baseInvoice.invoices"),14646.0)
        
if __name__ == "__main__":
    #unittest.main()
    
    Invoice = Invoice("Online Retail.xlsx","mongodb://mongo1:27017")
    Invoice.insertDatabase("baseInvoice","invoices")
    Invoice.groupAllTransactionsByInvoice("mongodb://mongo1:27017/baseInvoice.invoices")
    Invoice.whichProductSoldTheMost("mongodb://mongo1:27017/baseInvoice.invoices")
    Invoice.whichCustomerSpentTheMostMoney("mongodb://mongo1:27017/baseInvoice.invoices")
    