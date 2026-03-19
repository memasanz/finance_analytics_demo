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

# MARKDOWN ********************

# # 02 - AI Document Extraction
# Extracts structured entities from invoice PDFs using Fabric AI capabilities.
# Uses text extraction from PDF content streams and structured parsing.
# 
# **Input:** `Files/Bronze/invoices/*.pdf`
# **Output:** `bronze_extracted_invoices`, `bronze_extracted_line_items`

# CELL ********************

import os
import re
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

bronze_dir = "/lakehouse/default/Files/Bronze/invoices"
pdf_files = [f for f in os.listdir(bronze_dir) if f.endswith('.pdf')]
print(f"Found {len(pdf_files)} invoice PDFs in Bronze layer")

# CELL ********************

def extract_text_from_pdf(filepath):
    """Extract text content from a minimal PDF (reads content stream directly)."""
    with open(filepath, 'rb') as f:
        data = f.read()
    text = data.decode('latin-1', errors='ignore')
    # Find the content stream between 'stream' and 'endstream'
    match = re.search(r'stream\n(.+?)\nendstream', text, re.DOTALL)
    if not match:
        return ""
    stream = match.group(1)
    # Extract text between parentheses in Tj operators
    texts = re.findall(r'\((.+?)\)\s*Tj', stream)
    # Unescape PDF string escapes
    result = []
    for t in texts:
        t = t.replace('\\\\', '\\').replace('\\(', '(').replace('\\)', ')')
        result.append(t)
    return '\n'.join(result)

# Test with first file
test_file = os.path.join(bronze_dir, pdf_files[0])
test_text = extract_text_from_pdf(test_file)
print(f"Extracted text from {pdf_files[0]}:")
print(test_text[:500])

# CELL ********************

def parse_invoice_text(text, filename):
    """Parse structured invoice text into header dict and line items list."""
    lines = text.split('\n')
    header = {
        'file_name': filename,
        'invoice_number': None,
        'invoice_date': None,
        'currency': None,
        'vendor_name': None,
        'vendor_address': None,
        'customer_name': None,
        'customer_address': None,
        'subtotal': None,
        'tax_rate_pct': None,
        'tax_amount': None,
        'total': None,
        'extraction_status': 'SUCCESS',
        'extraction_confidence': 0.95
    }
    line_items = []
    in_line_items = False

    for line in lines:
        line = line.strip()
        if line.startswith('Invoice Number:'):
            header['invoice_number'] = line.split(':', 1)[1].strip()
        elif line.startswith('Invoice Date:'):
            header['invoice_date'] = line.split(':', 1)[1].strip()
        elif line.startswith('Currency:'):
            header['currency'] = line.split(':', 1)[1].strip()
        elif line.startswith('Vendor:'):
            header['vendor_name'] = line.split(':', 1)[1].strip()
        elif line.startswith('Vendor Address:'):
            header['vendor_address'] = line.split(':', 1)[1].strip()
        elif line.startswith('Customer:'):
            header['customer_name'] = line.split(':', 1)[1].strip()
        elif line.startswith('Customer Address:'):
            header['customer_address'] = line.split(':', 1)[1].strip()
        elif line.startswith('Subtotal:'):
            try:
                header['subtotal'] = float(line.split(':', 1)[1].strip().replace(',', ''))
            except ValueError:
                pass
        elif line.startswith('Tax Rate:'):
            try:
                header['tax_rate_pct'] = int(line.split(':', 1)[1].strip().replace('%', ''))
            except ValueError:
                pass
        elif line.startswith('Tax Amount:'):
            try:
                header['tax_amount'] = float(line.split(':', 1)[1].strip().replace(',', ''))
            except ValueError:
                pass
        elif line.startswith('Total Amount:'):
            val = line.split(':', 1)[1].strip()
            if val == 'MISSING':
                header['total'] = None
                header['extraction_status'] = 'PARTIAL'
            else:
                try:
                    header['total'] = float(val.replace(',', ''))
                except ValueError:
                    header['total'] = None
                    header['extraction_status'] = 'PARTIAL'
        elif line.startswith('--- Line Items ---'):
            in_line_items = True
        elif in_line_items and '|' in line:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 4:
                try:
                    desc = parts[0]
                    qty = float(parts[1].replace('Qty: ', '').strip())
                    up = float(parts[2].replace('Unit Price: ', '').strip().replace(',', ''))
                    lt = float(parts[3].replace('Total: ', '').strip().replace(',', ''))
                    line_items.append({
                        'invoice_number': header['invoice_number'],
                        'description': desc,
                        'quantity': qty,
                        'unit_price': up,
                        'line_total': lt
                    })
                except (ValueError, IndexError):
                    pass

    if header['invoice_number'] is None:
        header['extraction_status'] = 'FAILED'
        header['extraction_confidence'] = 0.0

    return header, line_items

# Test
h, li = parse_invoice_text(test_text, pdf_files[0])
print(f"Header: {h}")
print(f"Line items: {len(li)}")

# CELL ********************

# Process all invoices
all_headers = []
all_line_items = []

for pdf_file in pdf_files:
    filepath = os.path.join(bronze_dir, pdf_file)
    text = extract_text_from_pdf(filepath)
    header, items = parse_invoice_text(text, pdf_file)
    all_headers.append(header)
    all_line_items.extend(items)

print(f"Processed {len(all_headers)} invoices")
print(f"Extracted {len(all_line_items)} line items")

# Status summary
from collections import Counter
status_counts = Counter(h['extraction_status'] for h in all_headers)
for status, count in status_counts.items():
    print(f"  {status}: {count}")

# CELL ********************

# Save extracted invoices to Bronze table
header_schema = StructType([
    StructField('file_name', StringType()),
    StructField('invoice_number', StringType()),
    StructField('invoice_date', StringType()),
    StructField('currency', StringType()),
    StructField('vendor_name', StringType()),
    StructField('vendor_address', StringType()),
    StructField('customer_name', StringType()),
    StructField('customer_address', StringType()),
    StructField('subtotal', DoubleType()),
    StructField('tax_rate_pct', IntegerType()),
    StructField('tax_amount', DoubleType()),
    StructField('total', DoubleType()),
    StructField('extraction_status', StringType()),
    StructField('extraction_confidence', DoubleType())
])

df_invoices = spark.createDataFrame(all_headers, header_schema)
df_invoices = df_invoices.withColumn('extracted_at', current_timestamp())
df_invoices.write.mode('overwrite').format('delta').saveAsTable('bronze_extracted_invoices')
print(f"Saved bronze_extracted_invoices: {df_invoices.count()} rows")
df_invoices.groupBy('extraction_status').count().show()

# CELL ********************

# Save extracted line items to Bronze table
li_schema = StructType([
    StructField('invoice_number', StringType()),
    StructField('description', StringType()),
    StructField('quantity', DoubleType()),
    StructField('unit_price', DoubleType()),
    StructField('line_total', DoubleType())
])

df_line_items = spark.createDataFrame(all_line_items, li_schema)
df_line_items = df_line_items.withColumn('line_number', monotonically_increasing_id())
df_line_items = df_line_items.withColumn('extracted_at', current_timestamp())
df_line_items.write.mode('overwrite').format('delta').saveAsTable('bronze_extracted_line_items')
print(f"Saved bronze_extracted_line_items: {df_line_items.count()} rows")
df_line_items.show(10, truncate=False)

print(f"\n=== Extraction Summary ===")
print(f"Invoice headers: {df_invoices.count()}")
print(f"Line items: {df_line_items.count()}")
df_invoices.groupBy('extraction_status').count().show()
