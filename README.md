# Batch Processing using Dataproc
This is one of the part of **Introduction to Dataproc using PySpark** Repository. Here we will try to learn basics of Apache Spark to create **Batch** jobs. Here We will learn step by step how to create a batch job using [Titanic Dataset](https://www.kaggle.com/c/titanic). The complete process is divided into 4 parts:

1. **Creating a Dataproc Cluster**
2. **Creating a Dataproc Job**
3. **Reading from a File in Google Cloud Storage**
4. **Printing few records**
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
    git clone https://github.com/adityasolanki205/Batch-Processing-using-Dataproc.git
```

## Job Construction

Below are the steps to setup the enviroment and run the codes:

1. **Setup**: First we will have to setup free google cloud account which can be done [here](https://cloud.google.com/free). Then we need to Download the data from [Titanic Dataset](https://www.kaggle.com/c/titanic/data). It will include 2 csv files, train.csv and test.csv. We will rename either of the files as titanic.csv. 

2. **Cloning the Repository to Cloud SDK**: We will have to copy the repository on Cloud SDK using below command:

```bash
    # clone this repo:
    git clone https://github.com/adityasolanki205/Batch-Processing-using-Dataproc.git
```

3. **Creating a Dataproc cluster**: Now we create a dataproc cluster to run Pyspark Jobs. The simple command to create a basic cluster is given below.

```bash
   gcloud dataproc clusters create <cluster-name> \
   --project=<project name> \
   --region=<region> \
   --single-node 
``` 

4. **Creating a PySpark Job to read Google Cloud Storage and printing the data**: After reading the input file we will use a small code. Here we will use SparkSession to create a dataframe by reading from a GCS bucket. Here we will read from the bucket, create a temporary view and print the details of the dataframe

```python
    import pyspark
    from pyspark.sql import SparkSession

    appName = "DataProc testing"
    master = "local"
    spark = SparkSession.builder.\
            appName(appName).\
            master(master).\
            getOrCreate()     

    bucket = "dataproc-testing-pyspark"
    spark.conf.set('temporaryGcsBucket', bucket)
    df = spark.read.option( "inferSchema" , "true" ).option("header","true").\
        csv("gs://dataproc-testing-pyspark/titanic.csv")
    
    df.createOrReplaceTempView('Titanic')

    complete_data = spark.sql('Select * from Titanic')
    complete_data.show()

``` 
5. **Saving the data in Bigquery**: At last we will save the data in the Bigquery table using the below command
```python
    import pyspark
    from pyspark.sql import SparkSession

    appName = "DataProc testing"
    master = "local"
    spark = SparkSession.builder.\
            appName(appName).\
            master(master).\
            getOrCreate()     

    ...
    complete_data.write.format('com.google.cloud.spark.bigquery').\
        option('table', 'titanic.titanic_data').mode('append').save()

``` 

The output will be available inside one of the buckets and is attached here by the name job_output.txt. 


## Tests
To test the code we need to do the following:

    1. Copy the repository in Cloud SDK using below command:
        git clone https://github.com/adityasolanki205/Read-file-from-GCS-using-Dataproc.git
    
    2. Create a US Multiregional Storage Bucket by the name dataproc-testing-pyspark.
    
    3. Copy the data file in the cloud Bucket using the below command
        cd Read-file-from-GCS-using-Dataproc/data
        gsutil cp titanic.csv gs://dataproc-testing-pyspark/
        cd ..

    4. Create Temporary variables to hold GCP values
        PROJECT=<project name>
        BUCKET_NAME=dataproc-testing-pyspark
        CLUSTER=testing-dataproc
        REGION=us-central1
        
    5. Create a Biquery dataset with the name titanic and a table in this dataset by the name 
       titanic_data with the schema
        
        PassengerId:INTEGER,
        Survived:INTEGER,
        Pclass:INTEGER,
        Name:STRING,
        Sex:STRING,
        Age:FLOAT,
        SibSp:INTEGER,
        Parch:INTEGER,
        Ticket:STRING,
        Fare:FLOAT,
        Cabin:STRING,
        Embarked:STRING
    
    6. Create a Dataproc cluster by using the command:
        gcloud dataproc clusters create ${CLUSTER} \
        --project=${PROJECT} \
        --region=${REGION} \
        --single-node 
    
    7. Create a PySpark Job to run the code:
        gcloud dataproc jobs submit pyspark bq_write.py \
        --cluster=${CLUSTER} \
        --region=${REGION} \
        --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar


## Credits
1. Akash Nimare's [README.md](https://gist.github.com/akashnimare/7b065c12d9750578de8e705fb4771d2f#file-readme-md)
2. [Apache Spark](https://spark.apache.org/)
