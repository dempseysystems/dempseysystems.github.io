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


def read_sql_with_retry(sql_query, connection, max_retries=5, retry_interval=5):
    retries = 0
    last_exception = None  # Initialize a variable to store the last exception

    while retries < max_retries:
        try:
            result = pd.read_sql(sql_query, connection, parse_dates=["SHIP_DATE", "RECEIVED_DATE_cst", "DESC1"])
            return result
        except Exception as e:  # Catch all exceptions here
            last_exception = e  # Store the exception
            print(f"Encountered an error: {e}")
            print(f"Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
            retries += 1

    # If all retries failed, raise the last error encountered
    raise last_exception



# ---------------------- MAKE CONNECTION TO DATABASE ----------------------- #

def make_connection(database):
    conn_str = database
    connection = pyodbc.connect(conn_str)
    return connection


# --------------------------------- ORDHFILE -------------------------------- #
def export_ordhfile(connection, database_name):
    ordhfile = read_sql_with_retry(f"select BL, SHIPDT AS SHIP_DATE, DSC2 AS STATUS, RECEIVED_DATE_cst, DESC1, shipvia, PRT"
                           f" from ORDHFILE WHERE CLOSED=FALSE AND (DIV='01' OR DIV='06')",
                           connection)

    # Replacements to apply to both english and french dataframes
    # Current Date
    today = pd.Timestamp.now().normalize()
    # Condition 1 - Past due orders on credit hold
    mask1 = (ordhfile['SHIP_DATE'] <= today) & \
            (ordhfile['PRT'] == 'X') & \
            (ordhfile['STATUS'].isin(['BL Sent', 'In Stock', 'Direct']))
    ordhfile.loc[mask1, 'STATUS'] = 'Credit Hold'

    # Condition 2 - Separate delayed pickups and shipments
    mask2 = (ordhfile['shipvia'].str.lower() == "customer pickup") & \
            (ordhfile['STATUS'] == 'Delayed')
    ordhfile.loc[mask2, 'STATUS'] = 'Delayed Pickup'

    mask3 = ~mask2 & (ordhfile['STATUS'] == 'Delayed')
    ordhfile.loc[mask3, 'STATUS'] = 'Delayed Shipment'

    ordhfile_french = ordhfile.copy(deep=True)

    ship_via = ordhfile["shipvia"].to_list()
    ship_via = ship_via[0]
    if ship_via.lower() == "Customer Pickup".lower():
        ship_via = "Pickup"
        ship_via_past = "picked up"
    else:
        ship_via = "Ship"
        ship_via_past = "shipped"

    # Format the date columns
    ordhfile['SHIP_DATE'] = ordhfile['SHIP_DATE'].dt.strftime('%Y-%b-%d')
    ordhfile['RECEIVED_DATE_cst'] = ordhfile['RECEIVED_DATE_cst'].dt.strftime('%Y-%b-%d')
    ordhfile['DESC1'] = ordhfile['DESC1'].dt.strftime('%Y-%b-%d')

    ship_date = ordhfile["SHIP_DATE"].to_list()
    ship_date = ship_date[0]

    confirmed_statuses = ["BL Sent", "In Stock", "Invoicing", "BL Received", "Staged", "Revision Required",
                          "Training - BL Sent", "Training - In Stock", "Training - Invoicing", "Training - BL Received",
                          "Shipped", "Margin", "Delayed"]

    ordhfile["SHIP_DATE"] = np.where(ordhfile["STATUS"].isin(confirmed_statuses), ordhfile["SHIP_DATE"], "To be "
                                                                                                         "confirmed")

    statuses = {
        "In Stock": f"Your order is in stock. {ship_via} date is confirmed.",
        "BL Sent": f"Your order is in stock. {ship_via} date is confirmed.",
        "Credit Hold": "Your order has not yet been released. Please contact "
                       "dave@dempseycorporation.com",
        "BL Received": f"Your order has been received by our distribution centre. {ship_via} date is confirmed.",
        "Staged": f"Your order has been staged by our distribution centre. {ship_via} date is confirmed.",
        "Delayed Pickup": f"Our distribution centre has advised that your order has not yet been picked up. Please "
                          f"arrange pickup as per the instructions sent to you by Customer Service. If you have not "
                          f"received your confirmation email, please contact customerservice@dempseycorporation.com",
        "Delayed Shipment": "Our distribution centre has advised that your order was not picked up by your carrier"
                            "on the date we requested. We have followed up with the carrier and they will pick up as "
                            "soon as possible.",
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
        "Invoicing": f"Your order was {ship_via_past} on {ship_date}. You will receive an invoice shortly.",
        "Revision Required": "Your order has been received by our distribution centre. Ship / pickup date is confirmed.",
        "Shipped": f"Your order was {ship_via_past} on {ship_date}. We are awaiting freight charges and "
                   f"you will receive an invoice shortly.",
        "Cancelled": "Your order has been cancelled.",
        "Discrepancy": f"Your order was {ship_via_past} on {ship_date}. You will receive an invoice shortly.",
        "Margin": f"Your order was {ship_via_past} on {ship_date}. You will receive an invoice shortly.",
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
        "Training - Shipped": f"Your order was {ship_via_past} on {ship_date}. We are awaiting freight charges and "
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

    # --------------------- FRENCH -------------------- #

    ship_via = ordhfile_french["shipvia"].to_list()
    ship_via = ship_via[0]
    if ship_via.lower() == "Customer Pickup".lower():
        ship_via = "de ramassage"
        ship_via_past = "a été ramassée",
        ship_via_infinitive = "faire ramasser"
    else:
        ship_via = "d'expédition"
        ship_via_past = "a été expédiée",
        ship_via_infinitive = "expédier"

    # Dictionary for English to French month abbreviation mapping
    month_map = {
        'Jan': 'Janv',
        'Feb': 'Févr',
        'Mar': 'Mars',
        'Apr': 'Avr',
        'May': 'Mai',
        'Jun': 'Juin',
        'Jul': 'Juil',
        'Aug': 'Août',
        'Sep': 'Sept',
        'Oct': 'Oct',
        'Nov': 'Nov',
        'Dec': 'Déc'
    }

    # Function to format and replace English month abbreviations with French ones
    def format_date_to_french(date):
        if pd.isnull(date):
            return ""
        formatted_date = date.strftime('%Y-%b-%d')
        for eng, fr in month_map.items():
            formatted_date = formatted_date.replace(eng, fr)
        return formatted_date

    # Applying the function to the DataFrame columns
    ordhfile_french['SHIP_DATE'] = ordhfile_french['SHIP_DATE'].apply(format_date_to_french)
    ordhfile_french['RECEIVED_DATE_cst'] = ordhfile_french['RECEIVED_DATE_cst'].apply(format_date_to_french)
    ordhfile_french['DESC1'] = ordhfile_french['DESC1'].apply(format_date_to_french)

    ship_date = ordhfile_french["SHIP_DATE"].to_list()
    ship_date = ship_date[0]

    ordhfile_french["SHIP_DATE"] = np.where(ordhfile_french["STATUS"].isin(confirmed_statuses),
                                            ordhfile_french["SHIP_DATE"], "À confirmer")


    french_statuses = {
        "In Stock": f"Votre commande est en stock. La date {ship_via} est confirmée.",
        "BL Sent": f"Votre commande est en stock. La date {ship_via} est confirmée.",
        "Credit Hold": "Votre commande n'a pas encore été relâchée. Veuillez contacter "
                       "dave@dempseycorporation.com.",
        "BL Received": f"Votre commande a été reçu par notre centre de distribution. La date {ship_via} est confirmée.",
        "Staged": f"Votre commande a été préparée par notre centre de distribution. La date {ship_via} est confirmée.",
        "Delayed Pickup": f"Notre centre de distribution a informé que votre commande n'a pas encore été récupérée. "
                          f"Veuillez organiser le ramassage selon les instructions envoyées par Service à la clientèle."
                          f"Si vous n'avez pas reçu votre courriel de confirmation, veuillez contacter "
                          f"serviceclientele@dempseycorporation.com.",
        "Delayed Shipment": "Notre centre de distribution nous a informé que votre commande n'a pas été ramassée par "
                            "votre transporteur à la date que nous avions demandée. Nous avons relancé le transporteur "
                            "et ils viendront récupérer dès que possible.",
        "ETA": f"Votre commande n'est pas encore en stock. Service à la clientèle vous informera dès que le produit "
               f"sera reçu. La date {ship_via} n'est pas confirmée.",
        "Partial ETA - RR": f"Un ou plusieurs produits de votre commande ne sont pas encore en stock. Veuillez répondre"
                            f" au Service à la clientèle pour confirmer si vous souhaitez séparer la commande ou attendre "
                            f"pour {ship_via_infinitive} au complet.",
        "Partial ETA - SC": "Un ou plusieurs produits de votre commande ne sont pas encore en stock. Selon vos "
                            "instructions, nous attendons pour expédier la commande au complet.",
        "Partial ETA": f"Un ou plusieurs produits de votre commande ne sont pas encore en stock. Le Service Clients "
                       f"vous informera si nous ne pouvons pas respecter votre date demandée de {ship_via.lower()}.",
        "Pending": f"Votre commande a été reçue par notre centre de distribution. Le produit sera réceptionné en "
                   f"urgence dès son arrivée. La date {ship_via.lower()} reste provisoire.",
        "Direct": "Votre commande a été passée auprès de notre fournisseur. Date de livraison / de retrait à "
                  "confirmer.",
        "TBA": "Selon vos instructions, nous avons mis cette commande en attente. Veuillez contacter "
               "serviceclientele@dempseycorporation.com si vous souhaitez libérer la commande.",
        "Awaiting Payment": "Votre commande nécessite un prépaiement. Veuillez vous référer aux instructions dans "
                            "votre e-mail de confirmation de commande et dans la facture proforma. La date "
                            f"{ship_via} n'est pas confirmée.",
        "ETA/Awaiting Payment": "Votre commande nécessite un prépaiement. Veuillez vous référer aux instructions dans "
                                "votre e-mail de confirmation de commande et dans la facture proforma. Votre produit "
                                f"n'est pas encore en stock. La date {ship_via} n'est pas confirmée.",
        "Awaiting Information": "Votre commande est en cours de traitement. Nous attendons des informations en "
                                "interne avant de pouvoir confirmer votre commande. Le Service Clients fournira une "
                                "mise à jour sous peu.",
        "Invoicing": f"Votre commande a été {ship_via_past} le {ship_date}. Vous recevrez une facture sous peu.",
        "Revision Required": f"Votre commande a été reçue par notre centre de distribution. La date {ship_via} "
                             " est confirmée.",
        "Shipped": f"Votre commande a été {ship_via_past} le {ship_date}. Nous attendons les frais de transport et "
                   f"vous recevrez une facture sous peu.",
        "Cancelled": "Votre commande a été annulée.",
        "Discrepancy": f"Votre commande a été {ship_via_past} le {ship_date}. Vous recevrez une facture sous peu.",
        "Margin": f"Votre commande a été {ship_via_past} le {ship_date}. Vous recevrez une facture sous peu.",
        "Shelf Life": f"Nous avons besoin de votre approbation avant de confirmer la date {ship_via.lower()} pour "
                      f"votre commande. Veuillez répondre au courriel que Service à la clientèle vous a envoyé ou nous "
                      f"contacter à serviceclientele@dempseycorporation.com si vous n'avez pas reçu d'e-mail "
                      f"concernant cette commande.",
        "Lot Approval": f"Nous avons besoin de votre approbation avant de confirmer la date {ship_via.lower()} pour "
                        f"votre commande. Veuillez répondre à l'e-mail que le Service Clients vous a envoyé ou nous "
                        f"contacter à serviceclientele@dempseycorporation.com si vous n'avez pas reçu d'e-mail "
                        f"concernant cette commande.",
        "Price Discrepancy": "Le prix sur votre bon de commande ne correspond pas à celui dans nos dossiers. Veuillez "
                             "répondre au courriel que Service à la clientèle vous a envoyé ou nous contacter à "
                             "serviceclientele@dempseycorporation.com si vous n'avez pas reçu un courriel concernant "
                             "cette commande.",
        "Training - In Stock": f"Votre commande est en stock. La date {ship_via} est confirmée.",
        "Training - BL Sent": f"Votre commande est en stock. La date {ship_via} est confirmée.",
        "Training - BL Received": f"Votre commande a été reçu par notre centre de distribution. La date {ship_via} est confirmée.",
        "Training - Staged": f"Votre commande a été préparée par notre centre de distribution. La date {ship_via} est confirmée.",
        "Training - Delayed": f"Notre centre de distribution nous a informé que votre commande n'a pas encore été ramassée.",
        # Need to adjust for carriers
        "Training - ETA": f"Votre commande n'est pas encore en stock. Service à la clientèle vous informera dès que le produit "
               f"sera reçu. La date {ship_via} n'est pas confirmée.",
        "Training - Partial ETA - RR": f"Un ou plusieurs produits de votre commande ne sont pas encore en stock. Veuillez répondre"
                            f" au Service à la clientèle pour confirmer si vous souhaitez séparer la commande ou attendre "
                            f"pour {ship_via_infinitive} au complet.",
        "Training - Partial ETA - SC": "Un ou plusieurs produits de votre commande ne sont pas encore en stock. Selon vos "
                            "instructions, nous attendons pour expédier la commande au complet.",
        "Training - Partial ETA": f"Un ou plusieurs produits de votre commande ne sont pas encore en stock. Le Service Clients "
                       f"vous informera si nous ne pouvons pas respecter votre date demandée de {ship_via.lower()}.",
        "Training - Pending": f"Votre commande a été reçue par notre centre de distribution. Le produit sera réceptionné en "
                   f"urgence dès son arrivée. La date {ship_via.lower()} reste provisoire.",
        "Training - Direct": "Votre commande a été passée auprès de notre fournisseur. Date de livraison / de retrait à "
                  "confirmer.",
        "Training - TBA": "Selon vos instructions, nous avons mis cette commande en attente. Veuillez contacter "
               "serviceclientele@dempseycorporation.com si vous souhaitez libérer la commande.",
        "Training - Awaiting Payment": "Votre commande nécessite un prépaiement. Veuillez vous référer aux instructions dans "
                            "votre e-mail de confirmation de commande et dans la facture proforma. La date "
                            "d'expédition / de retrait n'est pas confirmée.",
        "Training - ETA/Awaiting Payment": "Votre commande nécessite un prépaiement. Veuillez vous référer aux instructions dans "
                                "votre e-mail de confirmation de commande et dans la facture proforma. Votre produit "
                                f"n'est pas encore en stock. La date {ship_via} n'est pas confirmée.",
        "Training - Awaiting Information": "Votre commande est en cours de traitement. Nous attendons des informations en "
                                "interne avant de pouvoir confirmer votre commande. Le Service Clients fournira une "
                                "mise à jour sous peu.",
        "Training - Shipped": f"Votre commande a été {ship_via_past} le {ship_date}. Nous attendons les frais de transport et "
                   f"vous recevrez une facture sous peu.",
        "Training - Cancelled": "Votre commande a été annulée.",
        "Training - Shelf Life": f"Nous avons besoin de votre approbation avant de confirmer la date {ship_via.lower()} pour "
                      f"votre commande. Veuillez répondre au courriel que Service à la clientèle vous a envoyé ou nous "
                      f"contacter à serviceclientele@dempseycorporation.com si vous n'avez pas reçu d'e-mail "
                      f"concernant cette commande.",
        "Training - Lot Approval": f"Nous avons besoin de votre approbation avant de confirmer la date {ship_via.lower()} pour "
                        f"votre commande. Veuillez répondre à l'e-mail que le Service Clients vous a envoyé ou nous "
                        f"contacter à serviceclientele@dempseycorporation.com si vous n'avez pas reçu d'e-mail "
                        f"concernant cette commande.",
        "Training - Price Discrepancy": "Le prix sur votre bon de commande ne correspond pas à celui dans nos dossiers. Veuillez "
                             "répondre au courriel que Service à la clientèle vous a envoyé ou nous contacter à "
                             "serviceclientele@dempseycorporation.com si vous n'avez pas reçu un courriel concernant "
                             "cette commande.",
    }

    # Replace the statuses in the dataframe using the dictionary
    ordhfile_french['STATUS'] = ordhfile_french['STATUS'].replace(french_statuses)

    ordhfile_french.to_csv(ORDER_STATUS_APP_DIRECTORY + f"\ordhfile-french - {database_name}.csv", index=False, encoding="utf8")
    ordhfile_french.to_csv(ORDER_STATUS_APP_DIRECTORY + f"\shipments-french - {database_name}.csv", index=False,
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
    close_command_prompts()

except Exception as e:
    print("An unexpected error occurred")
    logging.exception("An unexpected error occurred")

finally:
    end_time = time.time()
    print(f"Execution time: {end_time - start_time}")
    logging.info(f"Execution time: {end_time - start_time}")

