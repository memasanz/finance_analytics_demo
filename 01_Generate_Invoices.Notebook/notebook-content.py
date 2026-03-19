# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "",
# META       "default_lakehouse_name": "InvoiceLakehouse",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": ""
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 01 - Generate Sample Invoice PDFs
# Creates 200 synthetic invoice PDF documents in the Bronze layer.
# Uses only Python stdlib for PDF generation (no external packages).

# CELL ********************

import random
import datetime
import os

VENDORS = [
    {"name": "Contoso Electronics", "address": "123 Tech Blvd, Seattle, WA 98101"},
    {"name": "Northwind Traders", "address": "456 Commerce St, Portland, OR 97201"},
    {"name": "Adventure Works", "address": "789 Innovation Dr, San Francisco, CA 94102"},
    {"name": "Fabrikam Inc", "address": "321 Enterprise Ave, Austin, TX 78701"},
    {"name": "Tailspin Toys", "address": "654 Market Ln, Denver, CO 80202"},
    {"name": "Wide World Importers", "address": "987 Global Pkwy, Chicago, IL 60601"},
    {"name": "Alpine Ski House", "address": "111 Mountain Rd, Salt Lake City, UT 84101"},
    {"name": "Proseware Inc", "address": "222 Software Dr, Redmond, WA 98052"},
    {"name": "Litware Corp", "address": "333 Data St, Raleigh, NC 27601"},
    {"name": "Datum Industries", "address": "444 Analytics Blvd, Atlanta, GA 30301"}
]

CUSTOMERS = [
    {"name": "Woodgrove Bank", "address": "100 Finance Plaza, New York, NY 10001"},
    {"name": "Margies Travel", "address": "200 Journey Way, Miami, FL 33101"},
    {"name": "Trey Research", "address": "300 Lab Circle, Boston, MA 02101"},
    {"name": "Wingtip Solutions", "address": "400 Cloud Ave, Phoenix, AZ 85001"},
    {"name": "Bellows College", "address": "500 Campus Dr, Minneapolis, MN 55401"}
]

LINE_ITEMS_POOL = [
    "Cloud Hosting Services", "Software License Annual", "IT Consulting Hours",
    "Data Storage TB per month", "Network Equipment", "Security Audit Services",
    "Technical Support Plan", "API Gateway Usage", "Database Management",
    "DevOps Pipeline Setup", "Load Balancer Config", "SSL Certificate Annual",
    "Monitoring Service Monthly", "Backup Recovery Plan", "Training Workshop",
    "Hardware Maintenance", "Firewall Appliance", "VPN Gateway License",
    "Email Hosting per user", "Document Management System"
]

CURRENCIES = ["USD", "USD", "USD", "USD", "EUR", "GBP"]
TAX_RATES = [0.0, 0.05, 0.07, 0.08, 0.10]

random.seed(42)
NUM_INVOICES = 200
print(f"Will generate {NUM_INVOICES} invoices")

# CELL ********************

def generate_invoice_data(invoice_num):
    vendor = random.choice(VENDORS)
    customer = random.choice(CUSTOMERS)
    currency = random.choice(CURRENCIES)
    tax_rate = random.choice(TAX_RATES)
    start = datetime.date(2025, 1, 1)
    inv_date = start + datetime.timedelta(days=random.randint(0, 364))
    lines = []
    for _ in range(random.randint(1, 6)):
        desc = random.choice(LINE_ITEMS_POOL)
        qty = random.randint(1, 50)
        up = round(random.uniform(25.0, 5000.0), 2)
        lines.append({"description": desc, "quantity": qty, "unit_price": up, "line_total": round(qty * up, 2)})
    subtotal = round(sum(l["line_total"] for l in lines), 2)
    tax = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax, 2)
    has_issue = random.random() < 0.05
    return {
        "invoice_number": f"INV-{invoice_num:05d}",
        "invoice_date": inv_date.isoformat(),
        "vendor": vendor, "customer": customer,
        "line_items": lines, "subtotal": subtotal,
        "tax_rate": tax_rate, "tax_amount": tax,
        "total": total if not has_issue else None,
        "currency": currency, "has_issue": has_issue
    }

# CELL ********************

def make_pdf(text_lines):
    content_parts = ["BT", "/F1 10 Tf"]
    y = 750
    for line in text_lines:
        safe = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        content_parts.append(f"1 0 0 1 50 {y} Tm")
        content_parts.append(f"({safe}) Tj")
        y -= 14
        if y < 50:
            break
    content_parts.append("ET")
    stream = "\n".join(content_parts)
    sb = stream.encode("latin-1")
    offsets = []
    pdf = b"%PDF-1.4\n"
    offsets.append(len(pdf))
    pdf += b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
    offsets.append(len(pdf))
    pdf += b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"
    offsets.append(len(pdf))
    pdf += b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n"
    offsets.append(len(pdf))
    pdf += f"4 0 obj\n<< /Length {len(sb)} >>\nstream\n".encode("latin-1") + sb + b"\nendstream\nendobj\n"
    offsets.append(len(pdf))
    pdf += b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n"
    xref_offset = len(pdf)
    pdf += b"xref\n"
    pdf += f"0 {len(offsets)+1}\n".encode()
    pdf += b"0000000000 65535 f \n"
    for off in offsets:
        pdf += f"{off:010d} 00000 n \n".encode()
    pdf += b"trailer\n"
    pdf += f"<< /Size {len(offsets)+1} /Root 1 0 R >>\n".encode()
    pdf += b"startxref\n"
    pdf += f"{xref_offset}\n".encode()
    pdf += b"%%EOF\n"
    return pdf

test = make_pdf(["INVOICE", "Test"])
print(f"Test PDF: {len(test)} bytes")

# CELL ********************

def invoice_to_text(inv):
    lines = ["INVOICE", ""]
    lines.append(f"Invoice Number: {inv['invoice_number']}")
    lines.append(f"Invoice Date: {inv['invoice_date']}")
    lines.append(f"Currency: {inv['currency']}")
    lines.append("")
    lines.append(f"Vendor: {inv['vendor']['name']}")
    lines.append(f"Vendor Address: {inv['vendor']['address']}")
    lines.append("")
    lines.append(f"Customer: {inv['customer']['name']}")
    lines.append(f"Customer Address: {inv['customer']['address']}")
    lines.append("")
    lines.append("--- Line Items ---")
    for item in inv['line_items']:
        lines.append(f"{item['description']} | Qty: {item['quantity']} | Unit Price: {item['unit_price']:.2f} | Total: {item['line_total']:.2f}")
    lines.append("")
    lines.append(f"Subtotal: {inv['subtotal']:.2f}")
    lines.append(f"Tax Rate: {int(inv['tax_rate']*100)}%")
    lines.append(f"Tax Amount: {inv['tax_amount']:.2f}")
    total_str = f"{inv['total']:.2f}" if inv['total'] is not None else "MISSING"
    lines.append(f"Total Amount: {total_str}")
    return lines

# CELL ********************

bronze_dir = "/lakehouse/default/Files/Bronze/invoices"
os.makedirs(bronze_dir, exist_ok=True)

metadata_records = []

for i in range(1, NUM_INVOICES + 1):
    inv = generate_invoice_data(i)
    pdf_bytes = make_pdf(invoice_to_text(inv))
    filepath = os.path.join(bronze_dir, f"{inv['invoice_number']}.pdf")
    with open(filepath, 'wb') as f:
        f.write(pdf_bytes)
    metadata_records.append({
        "invoice_number": inv["invoice_number"],
        "invoice_date": inv["invoice_date"],
        "vendor_name": inv["vendor"]["name"],
        "customer_name": inv["customer"]["name"],
        "num_line_items": len(inv["line_items"]),
        "subtotal": inv["subtotal"],
        "tax_amount": inv["tax_amount"],
        "total": inv["total"],
        "currency": inv["currency"],
        "has_issue": inv["has_issue"]
    })
    if i % 50 == 0:
        print(f"  Generated {i}/{NUM_INVOICES} invoices...")

print(f"Done! {NUM_INVOICES} invoice PDFs written to {bronze_dir}")
print(f"Invoices with issues: {sum(1 for m in metadata_records if m['has_issue'])}")

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("invoice_number", StringType()),
    StructField("invoice_date", StringType()),
    StructField("vendor_name", StringType()),
    StructField("customer_name", StringType()),
    StructField("num_line_items", IntegerType()),
    StructField("subtotal", DoubleType()),
    StructField("tax_amount", DoubleType()),
    StructField("total", DoubleType()),
    StructField("currency", StringType()),
    StructField("has_issue", BooleanType())
])

df = spark.createDataFrame(metadata_records, schema)
df.write.mode("overwrite").format("delta").saveAsTable("bronze_invoice_metadata")
print(f"Ground truth metadata saved: {df.count()} records")
df.show(5)
