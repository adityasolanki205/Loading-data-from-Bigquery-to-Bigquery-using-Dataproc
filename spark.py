import findspark
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql import functions as sf

InitialSchema='Existing_account:STRING,Duration_month:INTEGER,Credit_history:STRING,Purpose:STRING,Credit_amount:STRING,Saving:STRING,Employment_duration:STRING,Installment_rate:INTEGER,Personal_status:STRING,Debtors:STRING,Residential_Duration:INTEGER,Property:STRING,Age:INTEGER,Installment_plans:STRING,Housing:STRING,Number_of_credits:INTEGER,Job:STRING,Liable_People:INTEGER,Telephone:STRING,Foreign_worker:STRING,Classification:INTEGER'
    
FinalSchema='Duration_month:INTEGER,Credit_history:STRING,Credit_amount:FLOAT,Saving:STRING,Employment_duration:STRING,Installment_rate:INTEGER,Personal_status:STRING,Debtors:STRING,Residential_Duration:INTEGER,Property:STRING,Age:INTEGER,Installment_plans:STRING,Housing:STRING,Number_of_credits:INTEGER,Job:STRING,Liable_People:INTEGER,Telephone:STRING,Foreign_worker:STRING,Classification:INTEGER,Month:STRING,Days:STRING,File_month:STRING,Version:STRING'

#Initializing spark Session  builder
spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
# Reading the data from input dataset
#df = spark.read.csv("/content/drive/MyDrive/german_data.csv", header=True, inferSchema=True)
bucket = "dataproc-testing-pyspark"
spark.conf.set('temporaryGcsBucket', bucket)
df = spark.read.format('com.google.cloud.spark.bigquery') \
      .option('table', 'silver-argon-320416:GermanCredit.German_Credit_initial') \
      .load()

# Filtering data where Value is invalid
df = df.filter((df.Purpose != 'NULL') & (df.Existing_account != 'NULL') & (df.Property !=  'NULL') & (df.Personal_status != 'NULL') & (df.Existing_account != 'NULL')  & (df.Credit_amount != 'NULL' ) & (df.Installment_plans != 'NULL'))

# Changing the Datatype of Credit Amount from string to Float
df = df.withColumn("Credit_amount", df['Credit_amount'].cast('float'))

# Converting data into better readable format. Here Existing amount column is segregated into 2 columns Months and days
split_col= pyspark.sql.functions.split(df['Existing_account'], '')
df = df.withColumn('Month', split_col.getItem(0))
df = df.withColumn('day1', split_col.getItem(1))
df = df.withColumn('day2', split_col.getItem(2))

df = df.withColumn('Days', sf.concat(sf.col('day1'),sf.col('day2')))

# Converting data into better readable format. Here Purpose column is segregated into 2 columns File Month and Version
split_purpose= pyspark.sql.functions.split(df['Purpose'], '')
df = df.withColumn('File_month', split_purpose.getItem(0))
df = df.withColumn('ver1', split_purpose.getItem(1))
df = df.withColumn('ver2', split_purpose.getItem(2))

df=df.withColumn('Version', sf.concat(sf.col('ver1'),sf.col('ver2')))

Month_Dict = {
    'A':'January',
    'B':'February',
    'C':'March',
    'D':'April',
    'E':'May',
    'F':'June',
    'G':'July',
    'H':'August',
    'I':'September',
    'J':'October',
    'K':'November',
    'L':'December'
    }

df= df.replace(Month_Dict,subset=['File_month'])
df = df.replace(Month_Dict,subset=['Month'])

#Dropping unwanted columns from the dataframe.
df = df.drop('day1')
df = df.drop('day2')
df = df.drop('ver1')
df = df.drop('ver2')
df = df.drop('Purpose')
df = df.drop('Existing_account')

df.write.format('com.google.cloud.spark.bigquery').option('table', 'GermanCredit.German_Credit_final').mode('append').save()
