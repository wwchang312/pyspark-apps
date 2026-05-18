from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import time

spark=SparkSession \
        .builder \
        .appName('dataframe_cache') \
        .getOrCreate()

print(f'spark application start')

comp_path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv'
comp_schema = 'company_id STRING,industry STRING'
emp_path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv'
emp_schema = 'company_id STRING,employee_count INT, follower_count INT, time_recorded TIMESTAMP'



comp_df = spark\
          .read \
          .option("header",'true') \
          .option("multiline",'true') \
          .schema(comp_schema) \
          .csv(comp_path)

emp_df = spark\
        .read \
        .option("header",'true') \
        .option('multiline','true') \
        .schema(emp_schema) \
        .csv(emp_path)

emp_df=emp_df.dropDuplicates(['company_id'])

comp_df.persist()
emp_df.persist()

print(f'company_cnt:{comp_df.count()}')
print(f'employees_cnt:{emp_df.count()}')

comp_it_df = comp_df.filter(col('industry')=='IT Services and IT Consulting')

comp_it_emp_df = (comp_it_df.join(
                    other= emp_df,
                    on = 'company_id',
                    how = 'inner'
                ).select('company_id','employee_count').sort('employee_count',ascending=False))

comp_it_emp_df= comp_it_emp_df.filter(col("employee_count") >= 1000)

comp_it_emp_df.show()

time.sleep(300)