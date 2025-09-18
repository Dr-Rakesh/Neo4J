import csv
import logging
import numpy as np
import requests
import datetime
from neo4j import GraphDatabase, bearer_auth

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG to capture detailed logs
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Azure credentials
CLIENT_ID = "9c247b71-ce83-4c05-8366-26231320c348"
CLIENT_SECRET = "c4l8Q~ZAdY10CQ-HNKYCA3UowZWaPb4VIvaAwa0u"
TENANT_ID = "38ae3bcd-9579-4fd4-adda-b42e1495d55a"
SCOPE = f"api://{CLIENT_ID}/.default"

# Neo4j URI
NEO4J_URI = "neo4j+ssc://37be20b1.databases.neo4j.io"

# CSV file paths
NODES_CSV = r"C:\Users\z004zn2u\OneDrive - Siemens AG\Desktop\CODED\Neo4J\data\nodes.csv"
RELATIONSHIPS_CSV = r"C:\Users\z004zn2u\OneDrive - Siemens AG\Desktop\CODED\Neo4J\data\relationships.csv"

# Fetch the initial bearer token
def get_access_token(client_id, client_secret, tenant_id, scope):
    logging.info("Fetching access token from Azure...")
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': scope
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        token_data = response.json()
        expires_on = datetime.datetime.now() + datetime.timedelta(seconds=token_data['expires_in'])
        logging.info("Access token retrieved successfully.")
        return token_data['access_token'], expires_on
    except Exception as e:
        logging.exception("Failed to fetch access token.")
        raise

# Initialize token and driver
token, token_expiry = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID, SCOPE)
driver = GraphDatabase.driver(NEO4J_URI, auth=bearer_auth(token))
logging.info("Neo4j driver initialized successfully.")

# Refresh token if needed
def refresh_token_if_needed(driver):
    global token, token_expiry
    if datetime.datetime.now() >= token_expiry - datetime.timedelta(minutes=5):
        logging.info("Refreshing access token...")
        token, token_expiry = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID, SCOPE)
        driver = GraphDatabase.driver(NEO4J_URI, auth=bearer_auth(token))
        logging.info("Access token refreshed, and Neo4j driver reinitialized.")
    return driver

# Mapping for node labels
def get_label_for_type(node_type):
    mapping = {
        "Supplier": "Supplier",
        "Manufacturer": "Manufacturer",
        "Distributor": "Distributor",
        "Retailer": "Retailer",
        "Product": "Product"
    }
    return mapping.get(node_type, "Entity")

# Node ingestion
def ingest_nodes(driver):
    logging.info("Starting node ingestion...")
    with driver.session() as session:
        try:
            with open(NODES_CSV, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    label = get_label_for_type(row['type'])
                    query = f"""
                    MERGE (n:{label} {{id:$id}})
                    SET n.name = $name, n.location = $location,
                        n.description = $description, n.supply_capacity = $supply_capacity
                    """
                    params = {
                        "id": row['id:ID'],
                        "name": row['name'],
                        "location": row['location'],
                        "description": row['description'],
                        "supply_capacity": np.random.randint(1000, 50001)
                    }
                    session.run(query, params)
            logging.info("Node ingestion completed successfully.")
        except Exception as e:
            logging.exception("Error occurred during node ingestion.")
            raise

# Relationship ingestion
def ingest_relationships(driver):
    logging.info("Starting relationship ingestion...")
    with driver.session() as session:
        try:
            with open(RELATIONSHIPS_CSV, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    query = f"""
                    MATCH (start {{id:$start_id}})
                    MATCH (end {{id:$end_id}})
                    MERGE (start)-[r:{row[':TYPE']} {{product:$product}}]->(end)
                    """
                    params = {
                        "start_id": row[':START_ID'],
                        "end_id": row[':END_ID'],
                        "product": row['product']
                    }
                    session.run(query, params)
            logging.info("Relationship ingestion completed successfully.")
        except Exception as e:
            logging.exception("Error occurred during relationship ingestion.")
            raise

# Create indexes
def create_indexes(driver):
    logging.info("Creating indexes...")
    with driver.session() as session:
        try:
            for label in ["Supplier", "Manufacturer", "Distributor", "Retailer", "Product"]:
                session.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE")
            logging.info("Indexes created successfully.")
        except Exception as e:
            logging.exception("Error occurred while creating indexes.")
            raise

# Main execution
if __name__ == "__main__":
    logging.info("Script started.")
    try:
        driver = refresh_token_if_needed(driver)  # Check and refresh token if needed
        create_indexes(driver)
        ingest_nodes(driver)
        ingest_relationships(driver)
        logging.info("Data ingestion complete.")
    except Exception as e:
        logging.exception("An error occurred during the script execution.")
    finally:
        driver.close()
        logging.info("Neo4j driver closed and script finished.")