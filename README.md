# Loading data from one Bigquery dataset to another using Dataproc
This is one of the part of **Introduction to Dataproc using PySpark** Repository. Here we will try to learn basics of Apache Spark to create **Batch** job to load a Bigquery table by fetching data from another Bigquery table and transforming it as per our requirement. Here We will learn step by step how to create a batch job using [German Credit Risk](https://www.kaggle.com/uciml/german-credit). The complete process is divided into 5 parts:

1. **Creating a Dataproc Cluster**
2. **Creating a Dataproc Job**
3. **Reading data a Bigquery dataset**
4. **Performing certain Transforms**
5. **Storing the Records in Bigquery**

## Motivation
For the last two years, I have been part of a great learning curve wherein I have upskilled myself to move into a Machine Learning and Cloud Computing. This project was practice project for all the learnings I have had. This is first of the many more to come. 
 

## Libraries/frameworks used

<b>Built with</b>
- [Apache Spark](https://spark.apache.org/)
- [Anaconda](https://www.anaconda.com/)
- [Python](https://www.python.org/)
- [Google Dataproc](https://cloud.google.com/dataproc)
- [Google Cloud Storage](https://cloud.google.com/storage)
- [Google Bigquery](https://cloud.google.com/bigquery)

## Cloning Repository

```bash
    # clone this repo:
    git clone https://github.com/adityasolanki205/Loading-data-from-Bigquery-to-Bigquery-using-Dataproc.git
```

## Job Construction

Below are the steps to setup the enviroment and run the codes:

1. **Setup**: First we will have to setup free google cloud account which can be done [here](https://cloud.google.com/free). Then we need to Download the data from [German Credit Risk](https://www.kaggle.com/uciml/german-credit). Included in the input folder. 

2. **Cloning the Repository to Cloud SDK**: We will have to copy the repository on Cloud SDK using below command:

```bash
    # clone this repo:
    git clone https://github.com/adityasolanki205/Loading-data-from-Bigquery-to-Bigquery-using-Dataproc.git
```

3. **Creating a Dataproc cluster**: Now we create a dataproc cluster to run Pyspark Jobs. The simple command to create a basic cluster is given below.

```bash
   gcloud dataproc clusters create <cluster-name> \
   --project=<project name> \
   --region=<region> \
   --single-node 
``` 

4. **Reading the data from input Dataset**: To read the data we will use pyspark code. Here we will use SparkSession to create a dataframe by reading from a input dataset. 

```python
    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as fn
    from pyspark.sql import functions as sf

    #Initializing spark Session builder
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    # Reading the data from input dataset
    bucket = "dataproc-testing-pyspark"
    spark.conf.set('temporaryGcsBucket', bucket)
    df = spark.read.format('com.google.cloud.spark.bigquery') \
          .option('table', '<Project name>:GermanCredit.German_Credit_initial') \
          .load()
``` 

5. **Filtering out unwanted data using Filter()**: Here we will filter out data with Null values

```python
    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as fn
    from pyspark.sql import functions as sf

    #Initializing spark Session builder
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    ...
    
    df = df.filter((df.Purpose != 'NULL') 
                   & (df.Existing_account != 'NULL') 
                   & (df.Property !=  'NULL') 
                   & (df.Personal_status != 'NULL') 
                   & (df.Existing_account != 'NULL')  
                   & (df.Credit_amount != 'NULL' ) 
                   & (df.Installment_plans != 'NULL'))

``` 

6. **Changeing Datatype of certain columns**: Here we will change the datatype of a complete column data using withcolumn().

```python
    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as fn
    from pyspark.sql import functions as sf

    #Initializing spark Session builder
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    ...
    df = df.withColumn("Credit_amount", df['Credit_amount'].cast('float'))

``` 

7. **Converting Encrpyted data to a more readable form**: Here we will decrypt data that us not human readable.

```python
    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as fn
    from pyspark.sql import functions as sf

    #Initializing spark Session builder
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    ...
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

``` 

8. **Dropping redundant Columns**: Here we will remove columns which have been decrypted or are of no use.

```python
    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as fn
    from pyspark.sql import functions as sf

    #Initializing spark Session builder
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    ...
    df = df.drop('day1')
    df = df.drop('day2')
    df = df.drop('ver1')
    df = df.drop('ver2')
    df = df.drop('Purpose')
    df = df.drop('Existing_account')
``` 

9. **Loading data into different Bigquery table**: Here we will load transformed data back to Bigquery.

```python
    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as fn
    from pyspark.sql import functions as sf

    #Initializing spark Session builder
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    ...
    df.write.format('com.google.cloud.spark.bigquery').option('table', 'GermanCredit.German_Credit_final').mode('append').save()
``` 


## Tests
To test the code we need to do the following:

    1. Copy the repository in Cloud SDK using below command:
        git clone https://github.com/adityasolanki205/Loading-data-from-Bigquery-to-Bigquery-using-Dataproc.git
    
    2. Create a US Multiregional Storage Bucket by the name dataproc-testing-pyspark.
    
    3. Create Temporary variables to hold GCP values
        PROJECT=<project name>
        BUCKET_NAME=dataproc-testing-pyspark
        CLUSTER=testing-dataproc
        REGION=us-central1
        
    4. Create a Biquery dataset with the name GermanCredit and a table named German_Credit_initial. This 
       table should be created using input file german_data.csv provided in the repository.(While creating the 
       table let it autodetect the schema. No Schema Definition required explicitly).

    5. Create a another Biquery table named German_Credit_final. This should be an empty table with
       schema as given below:
       
       Duration_month:INTEGER,
       Credit_history:STRING,
       Credit_amount:FLOAT,
       Saving:STRING,
       Employment_duration:STRING,
       Installment_rate:INTEGER,
       Personal_status:STRING,
       Debtors:STRING,
       Residential_Duration:INTEGER,
       Property:STRING,
       Age:INTEGER,
       Installment_plans:STRING,
       Housing:STRING,
       Number_of_credits:INTEGER,
       Job:STRING,
       Liable_People:INTEGER,
       Telephone:STRING,
       Foreign_worker:STRING,
       Classification:INTEGER,
       Month:STRING,
       Days:STRING,
       File_month:STRING,
       Version:STRING
    
    6. Create a Dataproc cluster by using the command:
        gcloud dataproc clusters create ${CLUSTER} \
        --project=${PROJECT} \
        --region=${REGION} \
        --single-node 
    
    7. Create a PySpark Job to run the code:
        gcloud dataproc jobs submit pyspark spark.py \
        --cluster=${CLUSTER} \
        --region=${REGION} \
        --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar


## Credits
1. Akash Nimare's [README.md](https://gist.github.com/akashnimare/7b065c12d9750578de8e705fb4771d2f#file-readme-md)
2. [Apache Spark](https://spark.apache.org/)
