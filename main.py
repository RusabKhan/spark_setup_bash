from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import os
import sys
import girder_worker
from configparser import NoOptionError, NoSectionError
import time


class Lyftrondata_Spark_Bigquery():
    global Connector_name
    Connector_name = "Lyftrondata_spark_bigquery_connector"  
    LicenseKey = ""
    
    def __init__(self,licenseKey):
        self.licenseKey = LicenseKey
        self.setup_spark_env()
    
    def setup_spark_env(self):
        """Setup your spark env. This method will look for spark on your
        system and verify if it is working properly. Method will check 
        for spark_home/SPARK_HOME. Make sure your environment variables are set.

        Raises:
            Exception: spark_home not found. Make sure your environment variables are set.
            Exception: SPARK_HOME not found. Make sure your environment variables are set.
        """
        spark_home = None
    
        try:
            spark_home = girder_worker.config.get('spark', 'spark_home')
        except (NoOptionError, NoSectionError):
            pass
    
        # If not configured try the environment
        if not spark_home:
            spark_home = os.environ.get('SPARK_HOME')
    
        if not spark_home:
            raise Exception('spark_home must be set or SPARK_HOME must be set in '
                            'the environment')
    
        # Need to set SPARK_HOME
        os.environ['SPARK_HOME'] = spark_home
    
        if not os.path.exists(spark_home):
            raise Exception('spark_home is not a valid directory')
    
        sys.path.append(os.path.join(spark_home, 'python'))
        sys.path.append(os.path.join(spark_home, 'bin'))
    
        # Check that we can import SparkContext
        from pyspark import SparkConf, SparkContext  # noqa
        
    def initialize(self,appName,config):
        """Initialize your spark instant to be used with the flow. The instance
        will work with your provided config dict. SparkSession has been used to
        create a spark working instance.
        

        Args:
            appName (string): The name you want to give this app. E.g LyftrondataSpark. 
            
            config (dict): See https://spark.apache.org/docs/latest/configuration.html for more.

        Returns:
            object: Your spark session.
        """
       
        spark_conf = SparkConf()
 
        if girder_worker.config.has_section('spark'):
            for (name, value) in girder_worker.config.items('spark'):
                spark_conf.set(name, value)
    
        # Override with any task specific configuration
        for (name, value) in config.items():
            spark_conf.set(name, value)

        spark = SparkSession \
        .builder \
        .appName(appName) \
        .config(conf=spark_conf) \
        .getOrCreate()
        return spark
    
    def sethadoop(self,spark,config):
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        for (name, value) in config.items():
            conf.set(name, value)
        
    def fetchdata(self,format,spark,connectionstring,table,user,password,driver):
        """Fetch data from source using your spark session. The .show() method shows you
        the data in a dataframe E.g fetchdata().show().

        Args:
            format (string): Format in which you want to receive your data. E.g: JDBC
            spark (object): Your spark session object
            connectionstring (string): Your connection string
            table (string): Your database table name
            user (string): Your database username
            password (string): Your database password
            driver (string): Your driver jar location

        Returns:
            spark.Dataframe: Your data fetched from source in a spark dataframe.
        """
        
        df = spark.read \
        .format(format) \
        .option("url", format+":"+connectionstring) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .option("fetchsize",5).load()
        
        return df
    
    def loadtotarget(self,df,table,mode,bucket):
        """Load your sparks Dataframe to target.

        Args:
            df (spark.dataframe): Your sparks dataframe to load
            table (string): Target table
            mode (string): Load type. E.g: Overwrite, Append
            bucket (string): Your dataproc/Storage bucket

        Returns:
            string: return message
        """
        val = df.write.format('bigquery') \
        .mode(mode) \
        .option("credentialsFile", "creds.json") \
        .option('table', table) \
        .option("temporaryGcsBucket",bucket) \
        .save()
            
        return val
    
    def s(self,data):
        print(data)



if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'creds.json'
    LicenseKey = "QWERTY-ZXCVB-6W4HD-DQCRG"
    dic={"fs.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "fs.AbstractFileSystem.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
            }
    lc = Lyftrondata_Spark_Bigquery(LicenseKey)
    ss = lc.initialize('rusab',{})
    lc.sethadoop(ss,dic)
    d = lc.fetchdata('jdbc',ss,'postgresql://satao.db.elephantsql.com:5432/ntaqwfpq','payasyougo','ntaqwfpq','qA9_GBtA4wqd2ocIjl013qeVIGOs7oD7','org.postgresql.Driver')
    d.createOrReplaceTempView("temp")
    d.show()

    