import requests
from neo4j import GraphDatabase, bearer_auth
import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG to capture detailed logs
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Hardcoded Azure credentials and Neo4j configurations
client_id = "9c247b71-ce83-4c05-8366-26231320c348"
client_secret = "c4l8Q~ZAdY10CQ-HNKYCA3UowZWaPb4VIvaAwa0u"
tenant_id = "38ae3bcd-9579-4fd4-adda-b42e1495d55a"
database_id = "37be20b1"
scope = f"api://{client_id}/.default"
uri = f"neo4j+ssc://{database_id}.databases.neo4j.io"
#uri = f"neo4j://{database_id}.databases.neo4j.io"

# Function to get an access token from Azure
def get_access_token(client_id, client_secret, tenant_id, scope):
    logging.debug("Getting access token from Azure...")
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': scope
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        result = response.json()
        if 'access_token' not in result:
            logging.error("Failed to retrieve access token: %s", result)
            raise Exception(f"Failed to retrieve token: {result}")
        logging.info("Access token retrieved successfully.")
        # Return token and its expiry timestamp
        expires_on = datetime.datetime.now() + datetime.timedelta(seconds=result['expires_in'])
        return result['access_token'], expires_on
    except Exception as e:
        logging.exception("Error while fetching access token.")
        raise

# Class definition for Neo4j operations with token refresh
class ApiTest:
    def __init__(self, uri, client_id, client_secret, tenant_id, scope):
        logging.info("Initializing ApiTest instance...")
        self.uri = uri
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.scope = scope
        try:
            logging.debug("Fetching initial access token...")
            self.token, self.token_expiry = get_access_token(client_id, client_secret, tenant_id, scope)
            logging.debug("Initializing Neo4j driver...")
            self.driver = GraphDatabase.driver(self.uri, auth=bearer_auth(self.token))
            logging.info("Neo4j driver initialized successfully.")
        except Exception as e:
            logging.exception("Failed to initialize ApiTest.")
            raise

    def refresh_token_if_needed(self):
        logging.debug("Checking if token refresh is required...")
        if datetime.datetime.now() >= self.token_expiry - datetime.timedelta(minutes=5):
            logging.info("Refreshing access token...")
            try:
                self.token, self.token_expiry = get_access_token(
                    self.client_id, self.client_secret, self.tenant_id, self.scope
                )
                logging.debug("Reinitializing Neo4j driver with refreshed token...")
                self.driver = GraphDatabase.driver(self.uri, auth=bearer_auth(self.token))
                logging.info("Access token refreshed and driver updated.")
            except Exception as e:
                logging.exception("Failed to refresh access token.")
                raise

    def close(self):
        logging.info("Closing Neo4j driver...")
        self.driver.close()
        logging.info("Neo4j driver closed.")

    def print_greeting(self, message):
        logging.info("Attempting to print greeting...")
        self.refresh_token_if_needed()  # Refresh token if needed
        try:
            with self.driver.session() as session:
                logging.debug("Executing write transaction for greeting...")
                greeting = session.execute_write(self._create_and_return_greeting, message)
                logging.info("Greeting printed: %s", greeting)
        except Exception as e:
            logging.exception("Error occurred while printing greeting.")
            raise

    @staticmethod
    def _create_and_return_greeting(tx, message):
        logging.debug("Creating greeting node in the database...")
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

    def delete_greeting(self, message):
        logging.info("Attempting to delete greeting...")
        self.refresh_token_if_needed()  # Refresh token if needed
        try:
            with self.driver.session() as session:
                logging.debug("Executing delete transaction for greeting...")
                session.execute_write(self._delete_greeting, message)
                logging.info("Greeting deleted successfully.")
        except Exception as e:
            logging.exception("Error occurred while deleting greeting.")
            raise

    @staticmethod
    def _delete_greeting(tx, message):
        logging.debug("Deleting greeting node from the database...")
        tx.run("MATCH (a:Greeting {message: $message}) DELETE a", message=message)


if __name__ == "__main__":
    logging.info("Starting the script...")
    try:
        # Initialize ApiTest with Azure credentials
        logging.debug("Creating ApiTest instance...")
        greeter = ApiTest(uri, client_id, client_secret, tenant_id, scope)

        # Perform operations
        logging.info("Performing operations: print_greeting and delete_greeting...")
        greeter.print_greeting("Welcome")
        greeter.delete_greeting("Welcome")

        # Close the driver
        greeter.close()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.exception("An error occurred during script execution.")