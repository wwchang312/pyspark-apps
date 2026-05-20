from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
import time

spark = SparkSession \
        .builder \
        .appName('wide_transform') \
        .config('spark.executor.memory','2g') \
        .config('spark.executor.cores', '2') \
        .config('spark.executor.instances','3') \
        .config('spark.sql.adaptive.enabled',False)\
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
    on=['skill_abr'],
    how='inner'
)

join_df.persist()
join_df.count()

join_group_df = join_df.groupBy('skill_name').count().alias('job_count')
join_group_df.persist()

join_group_df.sort('job_count',ascending=False).show()

time.sleep(1200)