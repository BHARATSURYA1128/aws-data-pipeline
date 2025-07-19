import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, [])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Replace these with actual bucket paths
input_path = "s3://s3-jbsbucket-stack-output/"
output_path = "s3://s3-jbsbucket-stack-output/transformed/"

df = spark.read.json(input_path)
df.write.mode("overwrite").parquet(output_path)
