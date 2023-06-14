import pyspark

MYSQL_CONNECTOR_VERSION = '5.1.49'

HOST = 'localhost'
USER = 'root'
PASSWORD = ''
DATABASE = 'a_database'
TABLE = 'some_db_table'

spark = (pyspark.sql.SparkSession.builder
 .config(f'spark.jars.packages', 'mysql:mysql-connector-java:{MYSQL_CONNECTOR_VERSION}')
 .getOrCreate())

df = (spark.read
 .option('driver', 'com.mysql.jdbc.Driver')
 .option('url', f'jdbc:mysql://{HOST}/{DATABASE}')
 .option('user', USER)
 .option('password', PASSWORD)
 .option('dbtable', TABLE)
 .format('jdbc')
 .load())

df.show()