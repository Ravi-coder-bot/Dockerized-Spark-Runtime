import sys
import json
import base64
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def run_job(inputs, output_path, job_id):
    """
    Reads data, performs joins and aggregates, and writes results.
    """
   
    spark = SparkSession.builder \
        .appName(f"TopCustomersRevenue-{job_id}") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .master("local[*]") \
        .getOrCreate()
    
   
    try:
        customers_df = spark.read.csv(inputs['customers'], header=True, inferSchema=True)
        orders_df = spark.read.csv(inputs['orders'], header=True, inferSchema=True)
        order_items_df = spark.read.csv(inputs['order_items'], header=True, inferSchema=True)
        products_df = spark.read.csv(inputs['products'], header=True, inferSchema=True)
    except Exception as e:
        print(f"Error reading input files: {e}", file=sys.stderr)
        spark.stop()
        return 1

    
    order_items_df = order_items_df.withColumn(
        "revenue", 
        F.col("quantity") * F.col("unit_price").cast(DoubleType())
    )

    
    order_rev_df = orders_df.join(order_items_df, "order_id")
    customer_rev_df = order_rev_df.join(customers_df, "customer_id")
    
   
    top_customers_df = customer_rev_df.groupBy("customer_id", "name") \
        .agg(F.sum("revenue").alias("total_revenue")) \
        .orderBy(F.col("total_revenue").desc()) \
        .limit(10)
    
    top_customers_list = top_customers_df.toJSON().collect()
    
    
    monthly_rev_df = customer_rev_df.withColumn(
        "month", 
        F.date_format(F.col("order_date"), "yyyy-MM")
    ).groupBy("month") \
     .agg(F.sum("revenue").alias("monthly_revenue")) \
     .orderBy("month")
     
    monthly_rev_list = monthly_rev_df.toJSON().collect()

    
    result_data = {
        "job_id": job_id,
        "top_customers": [json.loads(row) for row in top_customers_list],
        "monthly_revenue": [json.loads(row) for row in monthly_rev_list],
    }

   
    output_file_path = os.path.join(output_path, f"{job_id}_result.json") 
    with open(output_file_path, 'w') as f:
        json.dump(result_data, f, indent=4)
        
    spark.stop()
    print(f"Job SUCCEEDED. Output written to: {output_file_path}")
    return 0

if __name__ == "__main__":
   
    if len(sys.argv) != 4:
        print("Usage: python job_file.py <job_id> <inputs_base64> <output_path>", file=sys.stderr)
        sys.exit(1)

    job_id = sys.argv[1]
    base64_inputs = sys.argv[2]
    
    
    try:
        inputs_bytes = base64.b64decode(base64_inputs)
        inputs = json.loads(inputs_bytes.decode('utf-8')) 
    except Exception as e:
        print(f"Error decoding or loading JSON inputs: {e}", file=sys.stderr)
        sys.exit(1)
    
    
    output_path = sys.argv[3]

    exit_code = run_job(inputs, output_path, job_id)
    sys.exit(exit_code)