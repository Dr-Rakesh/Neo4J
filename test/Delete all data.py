import logging
from neo4j import GraphDatabase, bearer_auth

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Neo4j Credentials
NEO4J_URI = "neo4j+ssc://37be20b1.databases.neo4j.io"  # Replace with your Neo4j URI
CLIENT_ID = "9c247b71-ce83-4c05-8366-26231320c348"
CLIENT_SECRET = "c4l8Q~ZAdY10CQ-HNKYCA3UowZWaPb4VIvaAwa0u"
TENANT_ID = "38ae3bcd-9579-4fd4-adda-b42e1495d55a"
SCOPE = f"api://{CLIENT_ID}/.default"

# Function to get an access token from Azure
def get_access_token(client_id, client_secret, tenant_id, scope):
    import requests
    import datetime
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': scope
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.post(url, data=payload, headers=headers)
    response.raise_for_status()
    token_data = response.json()
    expires_on = datetime.datetime.now() + datetime.timedelta(seconds=token_data['expires_in'])
    return token_data['access_token'], expires_on

# Fetch the access token
logging.info("Fetching access token...")
token, token_expiry = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID, SCOPE)
logging.info("Access token retrieved successfully.")

# Connect to Neo4j
driver = GraphDatabase.driver(NEO4J_URI, auth=bearer_auth(token))
logging.info("Connected to Neo4j.")

# Function to delete all data from the database
def delete_all_data(driver):
    logging.info("Deleting all data from the database...")
    query = "MATCH (n) DETACH DELETE n"  # Cypher command to delete all nodes and relationships
    try:
        with driver.session() as session:
            session.run(query)
        logging.info("All data has been successfully deleted from the database.")
    except Exception as e:
        logging.exception("An error occurred while deleting data.")
        raise

# Main execution
if __name__ == "__main__":
    try:
        delete_all_data(driver)
    except Exception as e:
        logging.exception("Script failed.")
    finally:
        driver.close()
        logging.info("Neo4j driver closed.")