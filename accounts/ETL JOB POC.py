import sys
import json
import logging
import boto3
import pyspark

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.transforms import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'COLLECTION_NAME', 'DATABASE_NAME', 'OUTPUT_PREFIX', 'OUTPUT_FILENAME', 'PREFIX', 'REGION_NAME', 'SECRET_NAME'])

sc = pyspark.SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

logger = glueContext.get_logger()
logger.info("Starting job")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def get_secret(logger):
    logger.info("Setting up the MongoDB Credentials")
    secret_name = args['SECRET_NAME']
    region_name = args['REGION_NAME']

    logger.info("Creating Secrets Manager client")
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    else:
        logger.info("Decrypts secret using the associated KMS key")
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            secrets_json = json.loads(secret)
            return (secrets_json['USERNAME'], secrets_json['PASSWORD'], secrets_json['SERVER_ADDR'])
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret

user_name, password, server_addr = get_secret(logger)

logger.info("Creating Atlas MongoDB connection")

uri = "mongodb+srv://{}.mongodb.net/?retryWrites=true&w=majority&authSource=%24external&authMechanism=MONGODB-AWS&readPreference=secondary".format(server_addr) 

BUCKET_NAME=args['BUCKET_NAME'], 
PREFIX=args['PREFIX']

read_mongo_options = {
    "connection.uri": uri,
    "database":args['DATABASE_NAME'],
    "collection": args['COLLECTION_NAME'],   
    "username": user_name,   
    "password": password
}

logger.info('Reading data from Atlas MongoDB')
ds = glueContext.create_dynamic_frame_from_options(connection_type="mongodb", connection_options= read_mongo_options)

logger.info('Writing DynamicFrame to s3 bucket in json format')
path = "s3://{}/{}".format(args['BUCKET_NAME'], args['PREFIX'])
glueContext.write_dynamic_frame.from_options(ds, connection_type = "s3", connection_options={"path": path}, format="json")

logger.info('Writing DynamicFrame to s3 bucket in parquet format')
path = "s3://{}/{}".format(args['BUCKET_NAME'], args['PREFIX'])
glueContext.write_dynamic_frame.from_options(ds, connection_type = "s3", connection_options={"path": path}, format="parquet")

logger.info('Renaming files on S3')
import boto3

client = boto3.client('s3')
s3 = boto3.resource('s3')

bucket = s3.Bucket(args['BUCKET_NAME'])
for obj in bucket.objects.all():
    logger.info(obj.key)
    copy_source = {
        'Bucket': args['BUCKET_NAME'], 
        'Key': obj.key
    }
    if obj.key.startswith('{}/{}'.format(args['PREFIX'], 'run-')):
        client.copy_object(
            Bucket=args['BUCKET_NAME'],
            CopySource=copy_source,
            Key='{}/{}.json'.format(args['PREFIX'], args['OUTPUT_FILENAME']),
        )
        response = client.delete_object(
            Bucket=args['BUCKET_NAME'],
            Key=obj.key,
        )
    if obj.key.startswith('{}/{}'.format(args['PREFIX'], 'part-')):
        client.copy_object(
            Bucket=args['BUCKET_NAME'],
            CopySource=copy_source,
            Key='{}/{}.parquet'.format(args['PREFIX'], args['OUTPUT_FILENAME']),
        )
        response = client.delete_object(
            Bucket=args['BUCKET_NAME'],
            Key=obj.key,
        )

logger.info("Finished processing!")
