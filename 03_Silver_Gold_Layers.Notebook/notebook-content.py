# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2688be44-6756-4db4-a82d-bc52a1331970",
# META       "default_lakehouse_name": "InvoiceLakehouse",
# META       "default_lakehouse_workspace_id": "20f14893-f400-4f50-8fab-e0a884e724ae",
# META       "known_lakehouses": [
# META         {
# META           "id": "2688be44-6756-4db4-a82d-bc52a1331970"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()
print('STEP 1: Spark ready')

# CELL ********************

df_bronze_inv = spark.read.table('bronze_extracted_invoices')
df_bronze_lines = spark.read.table('bronze_extracted_line_items')
df_bronze_meta = spark.read.table('bronze_invoice_metadata')
print(f'STEP 2: Bronze inv={df_bronze_inv.count()}, lines={df_bronze_lines.count()}, meta={df_bronze_meta.count()}')
df_bronze_inv.printSchema()
df_bronze_lines.printSchema()

# CELL ********************

# Silver vendors
df_sv = df_bronze_inv.select(col('vendor_name'), col('vendor_address')).distinct().filter(col('vendor_name').isNotNull()).withColumn('vendor_id', monotonically_increasing_id() + 1)
df_sv.write.mode('overwrite').format('delta').saveAsTable('silver_vendors')
print(f'STEP 3: Silver vendors={df_sv.count()}')

# CELL ********************

# Silver customers
df_sc = df_bronze_inv.select(col('customer_name'), col('customer_address')).distinct().filter(col('customer_name').isNotNull()).withColumn('customer_id', monotonically_increasing_id() + 1)
df_sc.write.mode('overwrite').format('delta').saveAsTable('silver_customers')
print(f'STEP 4: Silver customers={df_sc.count()}')

# CELL ********************

# Silver invoices - read fresh from tables for stable IDs
df_sv2 = spark.read.table('silver_vendors')
df_sc2 = spark.read.table('silver_customers')

df_si = df_bronze_inv.join(df_sv2, 'vendor_name', 'left').join(df_sc2, 'customer_name', 'left').select(
    df_bronze_inv['invoice_number'],
    to_date(df_bronze_inv['invoice_date']).alias('invoice_date'),
    df_sv2['vendor_id'],
    df_bronze_inv['vendor_name'],
    df_sc2['customer_id'],
    df_bronze_inv['customer_name'],
    df_bronze_inv['subtotal'].cast('decimal(18,2)').alias('subtotal'),
    df_bronze_inv['tax_amount'].cast('decimal(18,2)').alias('tax_amount'),
    df_bronze_inv['total'].cast('decimal(18,2)').alias('total_amount'),
    coalesce(df_bronze_inv['currency'], lit('USD')).alias('currency'),
    df_bronze_inv['extraction_confidence'],
    df_bronze_inv['extraction_status'],
    when(df_bronze_inv['total'].isNull(), True).otherwise(False).alias('is_total_missing'),
    when(df_bronze_inv['extraction_status'] == 'FAILED', True).otherwise(False).alias('is_extraction_failed'),
    df_bronze_inv['extracted_at'].alias('processed_at')
)
df_si.write.mode('overwrite').format('delta').saveAsTable('silver_invoices')
print(f'STEP 5: Silver invoices={df_si.count()}')

# CELL ********************

# Silver line items
df_sli = df_bronze_lines.select(
    col('invoice_number'), col('line_number'), col('description'),
    col('quantity').cast('decimal(18,2)').alias('quantity'),
    col('unit_price').cast('decimal(18,2)').alias('unit_price'),
    col('line_total').cast('decimal(18,2)').alias('line_total'),
    when(
        (col('quantity').isNotNull()) & (col('unit_price').isNotNull()) & (col('line_total').isNotNull()) &
        (abs(col('quantity') * col('unit_price') - col('line_total')) > 0.01), True
    ).otherwise(False).alias('has_calculation_mismatch'),
    col('extracted_at').alias('processed_at')
)
df_sli.write.mode('overwrite').format('delta').saveAsTable('silver_line_items')
print(f'STEP 6: Silver line items={df_sli.count()}')

# CELL ********************

# Gold date dimension
df_si2 = spark.read.table('silver_invoices')
date_range = df_si2.agg(min('invoice_date').alias('mn'), max('invoice_date').alias('mx')).collect()[0]
print(f'STEP 7a: Date range {date_range.mn} to {date_range.mx}')
df_dates = spark.sql(f"SELECT explode(sequence(to_date('{date_range.mn}'), to_date('{date_range.mx}'), interval 1 day)) as date_key")
df_gd = df_dates.select(
    col('date_key'), year('date_key').alias('year'), quarter('date_key').alias('quarter'),
    month('date_key').alias('month_number'), date_format('date_key', 'MMMM').alias('month_name'),
    weekofyear('date_key').alias('week_of_year'), dayofmonth('date_key').alias('day_of_month'),
    date_format('date_key', 'EEEE').alias('day_name'),
    concat(lit('Q'), quarter('date_key'), lit(' '), year('date_key')).alias('quarter_label'),
    concat(date_format('date_key', 'MMM'), lit(' '), year('date_key')).alias('month_label')
)
df_gd.write.mode('overwrite').format('delta').saveAsTable('gold_dim_date')
print(f'STEP 7: Gold dim date={df_gd.count()}')

# CELL ********************

# Gold vendor & customer dimensions
spark.read.table('silver_vendors').write.mode('overwrite').format('delta').saveAsTable('gold_dim_vendor')
spark.read.table('silver_customers').write.mode('overwrite').format('delta').saveAsTable('gold_dim_customer')
print(f'STEP 8: Gold dim vendor={spark.read.table("gold_dim_vendor").count()}, customer={spark.read.table("gold_dim_customer").count()}')

# CELL ********************

# Gold invoice fact
df_gi = spark.read.table('silver_invoices').select(
    col('invoice_number'), col('invoice_date').alias('date_key'), col('vendor_id'), col('customer_id'),
    col('subtotal'), col('tax_amount'), col('total_amount'), col('currency'),
    col('extraction_confidence'), col('extraction_status'), col('is_total_missing'), col('is_extraction_failed'),
    when((col('is_total_missing') == True) | (col('is_extraction_failed') == True), 'Has Issues').otherwise('Clean').alias('data_quality_status')
)
df_gi.write.mode('overwrite').format('delta').saveAsTable('gold_fact_invoices')
print(f'STEP 9: Gold fact invoices={df_gi.count()}')
df_gi.groupBy('data_quality_status').count().show()

# CELL ********************

# Gold line item fact
df_gli = spark.read.table('silver_line_items').select(
    col('invoice_number'), col('line_number'), col('description'),
    col('quantity'), col('unit_price'), col('line_total'), col('has_calculation_mismatch')
)
df_gli.write.mode('overwrite').format('delta').saveAsTable('gold_fact_line_items')
print(f'STEP 10: Gold fact line items={df_gli.count()}')

print('\n=== ALL STEPS COMPLETE ===')
for t in ['silver_vendors', 'silver_customers', 'silver_invoices', 'silver_line_items', 'gold_dim_date', 'gold_dim_vendor', 'gold_dim_customer', 'gold_fact_invoices', 'gold_fact_line_items']:
    print(f'  {t:35s} {spark.read.table(t).count():>8,} rows')
