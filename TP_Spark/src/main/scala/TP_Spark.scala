import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

// Création d'une classe pour représenter un Client
case class Customer(id: Integer, name: String, city: String, province: String, zipcode :String)
  
object WordCount  {

  def main(args: Array[String]) {

  // Création SQLContext
  val conf = new SparkConf()
  conf.setAppName("SparSQLO").setMaster("local")

  println(""); println("*** Début exécution TP ***"); println("")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sc = new SparkContext(conf)
  // SQLContext entry point pour les donnees structures
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  
  // conversion RDD vers DataFrame.
  import sqlContext.implicits._
  val r = sc.textFile("/home/cloudera/clients_TP.csv") 
  val records = r.map(_.split(',')).map(r=>Customer(r(0).trim.toInt,r(1).trim,r(2).trim,r(3).trim,r(4).trim))
     
  // Création d'un dataframe qui va contenir les objets clients lus à partir d’un dataset représentant le fichier des clients
  val dfClients = records.toDF()

  // Enregistrer le dataframe comme une table "clients"
  dfClients.createOrReplaceTempView("clients")

  // Afficher le contenu du dataframe dfClients.
  println("Contenu du dataframe: "); print(dfClients.show(Int.MaxValue)) 
 
  // Afficher le schéma du dataframe dfClients.
  println("Schéma du dataframe: "); dfClients.printSchema()
  
  // Procéder à un SELECT de la colonne –nom- seulement
  var results = sqlContext.sql("SELECT name FROM clients")
  println("Liste des clients (nom): "); results.show()

  // Procéder à un SELECT des colonnes –nom- et -ville-
  results = sqlContext.sql("SELECT name, city FROM clients")
  println("Liste des clients (nom et ville): "); results.show()

  // Afficher le détail du client ayant un ID 30
  results = sqlContext.sql("SELECT * FROM clients WHERE id = 30")
  println("Client ayant pour ID 30: "); results.show()

  // Procéder au groupage des clients par code postal 
  results = sqlContext.sql("SELECT zipcode, city, count(name) as Nb_of_customers FROM clients GROUP BY zipcode, city ORDER BY city")
  println("Groupage des clients par code postal: "); results.show()
  println("*** Fin exécution TP ***")
  }
}