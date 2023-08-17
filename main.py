import pyodbc
import pandas as pd
import time
import warnings
import create_pages
import subprocess
import datetime
import os
import logging

warnings.simplefilter(action='ignore', category=UserWarning)

start_time = time.time()

CANADA_DB = os.environ.get('CANADA_DB')
CANADA_DB_NAME = os.environ.get('CANADA_DB_NAME')
US_DB = os.environ.get('US_DB')
US_DB_NAME = os.environ.get('US_DB_NAME')
ORDER_STATUS_APP_DIRECTORY = os.environ.get('ORDER_STATUS_APP_DIRECTORY')


# Logging Configuration
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

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
    ordhfile = pd.read_sql(f"select BL, SHIPDT AS SHIP_DATE, DSC2 AS STATUS from ORDHFILE WHERE CLOSED=FALSE",
                           connection)
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
try:
    commit_and_push(repo_path, commit_message, branch)
except subprocess.CalledProcessError:
    print("No changes to push")
time.sleep(2)
close_command_prompts()

end_time = time.time()
print(f"Execution time: {end_time - start_time}")
