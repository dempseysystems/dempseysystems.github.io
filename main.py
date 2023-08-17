import pyodbc
import pandas as pd
import time
import numpy as np
import datetime
import warnings
import os
import create_pages
import subprocess
import datetime
import os

warnings.simplefilter(action='ignore', category=UserWarning)

start_time = time.time()

CANADA_DB = os.environ.get('CANADA_DB')

US_DB = os.environ.get('US_DB')

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
    ordhfile = pd.read_sql(f"select BL, SHIPDT AS SHIP_DATE, DSC2 AS STATUS from ORDHFILE WHERE CLOSED=FALSE", connection)
    ordhfile.to_csv(r"C:\Users\Mitchell\PycharmProjects\dempseysystems.github.io" + f"\ordhfile - {database_name}.csv", index=False, encoding="utf8")
    ordhfile.to_csv(r"C:\Users\Mitchell\PycharmProjects\dempseysystems.github.io\shipments.csv", index=False, encoding="utf8")
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
database_name = "Dempsey Canada"

connection = make_connection(database)
ordhfile = export_ordhfile(connection, database_name)
create_pages.generate_static_pages()

repo_path = r"C:\Users\Mitchell\PycharmProjects\dempseysystems.github.io"
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
