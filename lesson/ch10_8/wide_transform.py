from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast,count
import time

spark = SparkSession \
        .builder \
        .appName('wide_transform') \
        .config('spark.executor.memory','2g') \
        .config('spark.executor.cores', '2') \
        .config('spark.executor.instances','3') \
        .config('spark.sql.adaptive.enabled','false')\
        .getOrCreate()

job_path = 'hdfs:///home/spark/sample/linkedin_jobs/jobs/job_skills.csv'
job_schema = 'job_id LONG, skill_abr STRING'

skill_path = 'hdfs:///home/spark/sample/linkedin_jobs/mappings/skills.csv'
skill_schema = 'skill_abr STRING, skill_name STRING'


job_df = spark.read \
        .option('header','true') \
        .option('multiLine','true') \
        .schema(job_schema) \
        .csv(job_path)

job_df.persist()
job_df.count()

skill_df = spark.read \
           .option('header', 'true') \
           .option('multiLine', 'true') \
           .schema(skill_schema) \
           .csv(skill_path)

skill_df.persist()
skill_df.count()

join_df=job_df.join(
    other=broadcast(skill_df),
    on='skill_abr',
    how='inner'
).select('job_id','skill_name') \
    .groupBy('skill_name') \
    .agg(count('job_id').alias('job_count')) \
    .sort('job_count',ascending=False)

print(join_df.count())

time.sleep(1200)