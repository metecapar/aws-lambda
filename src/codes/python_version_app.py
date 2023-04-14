import pandas as pd
import datetime
import json

# Get the current time and format the date
file_date = datetime.datetime.now().strftime("%d%m%Y")

# Define file names
customer_file = f"customers_{file_date}.csv"
orders_file = f"orders_{file_date}.csv"
items_file = f"items_{file_date}.csv"
try:
# Read CSV files
    customers_df = pd.read_csv(customer_file)
    orders_df = pd.read_csv(orders_file)
    items_df = pd.read_csv(items_file)
except:
    print("Error while reading csv's")
    
# Validation
missing_customer_references = orders_df[~orders_df["customer_reference"].isin(customers_df["customer_reference"])]
missing_customer_reference_errors = missing_customer_references.copy()
missing_customer_reference_errors["type"] = "error_message"
missing_customer_reference_errors["message"] = "Customer reference not found in customers."
missing_customer_reference_errors = missing_customer_reference_errors[["type", "customer_reference", "order_reference", "message"]]

# Merge DataFrames
orders_items_df = orders_df.merge(items_df, on="order_reference", how="inner")
orders_customers_df = orders_items_df.merge(customers_df, on="customer_reference", how="inner")

# Calculate total amount spent and number of orders per customer
total_amount_spent = (
    orders_customers_df.groupby("customer_reference")["total_price"].sum().reset_index()
)
number_of_orders = (
    orders_customers_df.groupby("customer_reference")["order_reference"]
    .nunique()
    .reset_index()
)

# Create customer summary
customer_summary = number_of_orders.merge(total_amount_spent, on="customer_reference")
customer_summary["type"] = "customer_message"

# Check for not found order references in items
missing_order_references = items_df[~items_df["order_reference"].isin(orders_df["order_reference"])]
missing_order_reference_errors = missing_order_references.copy()
missing_order_reference_errors["type"] = "error_message"
missing_order_reference_errors["customer_reference"] = None
missing_order_reference_errors["message"] = "Order reference not found in orders."
missing_order_reference_errors = missing_order_reference_errors[["type", "customer_reference", "order_reference", "message"]]

# Combine error messages
all_error_messages = pd.concat([missing_customer_reference_errors, missing_order_reference_errors])

# Print results
print(customer_summary.to_json(orient="records"))
print(all_error_messages.to_json(orient="records"))