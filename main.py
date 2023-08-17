import pyodbc
import pandas as pd
import time
import warnings
import create_pages
import subprocess
import datetime
import os
import logging
import numpy as np

warnings.simplefilter(action='ignore', category=UserWarning)

start_time = time.time()

CANADA_DB = os.environ.get('CANADA_DB')
CANADA_DB_NAME = os.environ.get('CANADA_DB_NAME')
US_DB = os.environ.get('US_DB')
US_DB_NAME = os.environ.get('US_DB_NAME')
ORDER_STATUS_APP_DIRECTORY = os.environ.get('ORDER_STATUS_APP_DIRECTORY')
ORDER_STATUS_APP_SERVER_DIRECTORY = os.environ.get('ORDER_STATUS_APP_SERVER_DIRECTORY')

# Logging Configuration
logging.basicConfig(filename='order_status_search_tool.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("Order Status App started.")
print("Order Status App")

def close_command_prompts():
    os.system('taskkill /F /IM cmd.exe')


# ---------------------- MAKE CONNECTION TO DATABASE ----------------------- #

def make_connection(database):
    conn_str = database
    connection = pyodbc.connect(conn_str)
    return connection


# --------------------------------- ORDHFILE -------------------------------- #
def export_ordhfile(connection, database_name):
    ordhfile = pd.read_sql(f"select BL, SHIPDT AS SHIP_DATE, DSC2 AS STATUS, RECEIVED_DATE_cst, DESC1, shipvia from ORDHFILE WHERE CLOSED=FALSE",
                           connection, parse_dates=["SHIP_DATE", "RECEIVED_DATE_cst", "DESC1"])
    ship_date = ordhfile["SHIP_DATE"].to_list()
    ship_date = ship_date[0]

    ship_via = ordhfile["shipvia"].to_list()
    ship_via = ship_via[0]
    if ship_via.lower() == "Customer Pickup".lower():
        ship_via = "Pickup"
        ship_via_past = "picked up"
    else:
        ship_via = "Ship"
        ship_via_past = "shipped"

    # Format the date columns
    ordhfile['SHIP_DATE'] = ordhfile['SHIP_DATE'].dt.strftime('%Y-%m-%d')
    ordhfile['RECEIVED_DATE_cst'] = ordhfile['RECEIVED_DATE_cst'].dt.strftime('%Y-%m-%d')
    ordhfile['DESC1'] = ordhfile['DESC1'].dt.strftime('%Y-%m-%d')

    confirmed_statuses = ["BL Sent", "In Stock", "Invoicing", "BL Received", "Staged", "Revision Required",
                          "Training - BL Sent", "Training - In Stock", "Training - Invoicing", "Training - BL Received",
                          "Shipped", "Margin", "Delayed"]

    ordhfile["SHIP_DATE"] = np.where(ordhfile["STATUS"].isin(confirmed_statuses), ordhfile["SHIP_DATE"], "To be "
                                                                                                         "confirmed")

    statuses = {
        "In Stock": f"Your order is in stock. {ship_via} date is confirmed.",
        "BL Sent": f"Your order is in stock. {ship_via} date is confirmed.",
        "BL Received": f"Your order has been received by our distribution centre. {ship_via} date is confirmed.",
        "Staged": f"Your order has been staged by our distribution centre. {ship_via} date is confirmed.",
        "Delayed": f"Our distribution centre has advised that your order has not yet been picked up.", # Need to adjust for carriers
        "ETA": f"Your order is not yet in stock. Customer service will advise once product has been received. {ship_via} date is not confirmed.",
        "Partial ETA - RR": "One or more products on your order are not yet in stock. Please reply to Customer Service to"
                            f"confirm if you would like to split the order or wait to {ship_via.lower()} complete.",
        "Partial ETA - SC": "One or more products on your order are not yet in stock. As per your instructions, we are " \
                            "waiting to ship the order complete.",
        "Partial ETA": "One or more products on your order are not yet in stock. Customer Service will advise if we "
                       f"are unable to meet your requested {ship_via.lower()} date.",
        "Pending": "Your order has been received by our distribution centre. Product will be rush received as soon as "
                   f"it arrives. {ship_via.lower()} date remains tentative.",
        "Direct": "Your order has been placed with our supplier. Delivery / pickup date to be confirmed.",
        "TBA": "As per your instructions, we have placed this order on hold. Please contact "
               "customerservice@dempseycorporation.com if you wish to release the order.",
        "Awaiting Payment": "Your order requires prepayment. Please refer to instructions in your order confirmation"
                            "email and proforma invoice. Ship / pickup date is not confirmed.",
        "ETA/Awaiting Payment": "Your order requires pre-payment. Please refer to instructions in your order confirmation"
                            "email and proforma invoice. Your product is not yet in stock. Ship / pickup date is not confirmed.",
        "Awaiting Information": "Your order is being processed. We are awaiting information internally before we can "
                                "confirm your order. Customer Service will provide an update shortly.",
        "Invoicing": f"Your order was {ship_via_past} up on {ship_date}. You will receive an invoice shortly.",
        "Revision Required": "Your order has been received by our distribution centre. Ship / pickup date is confirmed.",
        "Shipped": f"Your order was shipped / picked up on {ship_date}. We are awaiting freight charges and "
                   f"you will receive an invoice shortly.",
        "Cancelled": "Your order has been cancelled.",
        "Discrepancy": f"Your order was shipped / picked up on {ship_date}. You will receive an invoice shortly.",
        "Margin": f"Your order was shipped / picked up on {ship_date}. You will receive an invoice shortly.",
        "Shelf Life": f"We require your approval prior to confirming the {ship_via.lower()} date for your order. Please reply"
                      "to the email Customer Service sent you or contact us at customerservice@dempseycorporation.com "
                      "if you have not received an email regarding this order.",
        "Lot Approval": f"We require your approval prior to confirming the {ship_via.lower()} date for your order. Please reply"
                      "to the email Customer Service sent you or contact us at customerservice@dempseycorporation.com "
                      "if you have not received an email regarding this order.",
        "Price Discrepancy": "The price on your purchase order does not match our records. Please reply"
                      "to the email Customer Service sent you or contact us at customerservice@dempseycorporation.com "
                      "if you have not received an email regarding this order.",
        "Training - In Stock": f"Your order is in stock. {ship_via} date is confirmed.",
        "Training - BL Sent": f"Your order is in stock. {ship_via} date is confirmed.",
        "Training - BL Received": f"Your order has been received by our distribution centre. {ship_via} date is confirmed.",
        "Training - ETA": f"Your order is not yet in stock. Customer service will advise once product has been received. {ship_via} date is not confirmed.",
        "Training - Partial ETA - RR": "One or more products on your order are not yet in stock. Please reply to Customer Service to"
                            f"confirm if you would like to split the order or wait to {ship_via.lower()} complete.",
        "Training - Partial ETA - SC": "One or more products on your order are not yet in stock. As per your instructions, we are " \
                            f"waiting to {ship_via.lower()} the order complete.",
        "Training - Partial ETA": "One or more products on your order are not yet in stock. Customer Service will advise if we "
                       f"are unable to meet your requested {ship_via.lower()} date.",
        "Training - Pending": "Your order has been received by our distribution centre. Product will be rush received as soon as "
                   f"it arrives. {ship_via} date remains tentative.",
        "Training - Direct": f"Your order has been placed with our supplier. {ship_via.lower()} date to be confirmed.",
        "Training - TBA": "As per your instructions, we have placed this order on hold. Please contact "
               "customerservice@dempseycorporation.com if you wish to release the order.",
        "Training - Awaiting Payment": "Your order requires pre-payment. Please refer to instructions in your order confirmation"
                            f"email and proforma invoice. {ship_via} date is not confirmed.",
        "Training - ETA/Awaiting Payment": "Your order requires pre-payment. Please refer to instructions in your order confirmation"
                                "email and proforma invoice. Your product is not yet in stock. Ship / pickup date is not confirmed.",
        "Training - Awaiting Information": "We are awaiting information internally before we can confirm your order. "
                                "Customer Service will provide an update shortly.",
        "Training - Invoicing": f"Your order was {ship_via_past} on {ship_date}. You will receive an invoice shortly.",
        "Training - Revision Required": "Your order has been received by our distribution centre. Ship / pickup date is confirmed.",
        "Training - Shipped": f"Your order was {ship_via_past} up on {ship_date}. We are awaiting freight charges and "
                   f"you will receive an invoice shortly.",
        "Training - Cancelled": "Your order has been cancelled.",
        "Training - Shelf Life": f"We require your approval prior to confirming the {ship_via.lower()} date for your order. Please reply"
                      "to the email Customer Service sent you or contact us at customerservice@dempseycorporation.com "
                      "if you have not received an email regarding this order.",
        "Training - Lot Approval": f"We require your approval prior to confirming the {ship_via.lower()} for your order. Please reply"
                        "to the email Customer Service sent you or contact us at customerservice@dempseycorporation.com "
                        "if you have not received an email regarding this order.",
        "Training - Price Discrepancy": "The price on your purchase order does not match our records. Please reply"
                             "to the email Customer Service sent you or contact us at customerservice@dempseycorporation.com "
                             "if you have not received an email regarding this order."

    }
    # Replace the statuses in the dataframe using the dictionary
    ordhfile['STATUS'] = ordhfile['STATUS'].replace(statuses)

    ordhfile.to_csv(ORDER_STATUS_APP_DIRECTORY + f"\ordhfile - {database_name}.csv", index=False, encoding="utf8")
    ordhfile.to_csv(ORDER_STATUS_APP_DIRECTORY + f"\shipments - {database_name}.csv", index=False,
                    encoding="utf8")
    return ordhfile


# --------------------------------- PUSH TO GITHUB -------------------------------- #
def commit_and_push(repo_path, commit_message, branch="main"):
    # Navigate to the repository path
    os.chdir(repo_path)

    # Stage all changes
    subprocess.run(["git", "add", "."], check=True)

    # Commit the changes
    subprocess.run(["git", "commit", "-m", commit_message], check=True)

    # Push the changes to the specified branch
    subprocess.run(["git", "push", "origin", branch], check=True)


# --------------------------------- PROGRAM EXECUTION -------------------------------- #
try:
    database = CANADA_DB
    database_name = CANADA_DB_NAME

    connection = make_connection(database)
    ordhfile = export_ordhfile(connection, database_name)
    create_pages.generate_static_pages(database_name)

    database = US_DB
    database_name = US_DB_NAME

    connection = make_connection(database)
    ordhfile = export_ordhfile(connection, database_name)
    create_pages.generate_static_pages(database_name)

    repo_path = ORDER_STATUS_APP_DIRECTORY
    commit_message = f"Commit {datetime.datetime.now()}"
    branch = "main"  # Change this if you want to push to a different branch
    commit_and_push(repo_path, commit_message, branch)

    time.sleep(2)
    close_command_prompts()

except subprocess.CalledProcessError:
    print("No changes to push")
    logging.warning("No changes to push")

except Exception as e:
    print("An unexpected error occurred")
    logging.exception("An unexpected error occurred")

finally:
    end_time = time.time()
    print(f"Execution time: {end_time - start_time}")
    logging.info(f"Execution time: {end_time - start_time}")

