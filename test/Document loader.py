import requests
from neo4j import GraphDatabase, bearer_auth
import datetime
import logging
import base64
import os

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

    def upload_pdf(self, file_path):
        logging.info(f"Uploading PDF document: {file_path}")
        self.refresh_token_if_needed()  # Refresh token if needed
        try:
            # Encode the PDF file as Base64
            with open(file_path, "rb") as pdf_file:
                encoded_pdf = base64.b64encode(pdf_file.read()).decode("utf-8")
            
            # Get the file name using os.path.basename
            file_name = os.path.basename(file_path)

            # Save the encoded PDF to Neo4j
            with self.driver.session() as session:
                logging.debug("Executing write transaction to upload PDF...")
                session.execute_write(self._store_pdf, file_name, encoded_pdf)
                logging.info(f"PDF document '{file_name}' uploaded successfully.")
        except Exception as e:
            logging.exception("Error occurred while uploading PDF.")
            raise

    @staticmethod
    def _store_pdf(tx, file_name, encoded_pdf):
        logging.debug(f"Storing PDF document '{file_name}' in the database...")
        result = tx.run(
            "CREATE (d:Document {name: $file_name, content: $encoded_pdf})",
            file_name=file_name,
            encoded_pdf=encoded_pdf
        )
        # Consume the result to get the transaction summary
        summary = result.consume()
        logging.debug(f"Transaction result summary: {summary.counters}")

    def retrieve_pdf(self, file_name, output_path):
        logging.info(f"Retrieving PDF document: {file_name}")
        self.refresh_token_if_needed()  # Refresh token if needed
        try:
            with self.driver.session() as session:
                logging.debug("Executing read transaction to retrieve PDF...")
                encoded_pdf = session.execute_read(self._get_pdf, file_name)
                if encoded_pdf:
                    # Decode and save the PDF file
                    with open(output_path, "wb") as pdf_file:
                        pdf_file.write(base64.b64decode(encoded_pdf))
                    logging.info(f"PDF document '{file_name}' retrieved and saved to '{output_path}'.")
                else:
                    logging.warning(f"No PDF document found with name '{file_name}'.")
        except Exception as e:
            logging.exception("Error occurred while retrieving PDF.")
            raise

    @staticmethod
    def _get_pdf(tx, file_name):
        logging.debug(f"Retrieving PDF document '{file_name}' from the database...")
        result = tx.run(
            "MATCH (d:Document {name: $file_name}) RETURN d.content AS content",
            file_name=file_name
        )
        record = result.single()
        logging.debug(f"Retrieved record: {record}")
        return record["content"] if record else None


if __name__ == "__main__":
    logging.info("Starting the script...")
    try:
        # Initialize ApiTest with Azure credentials
        logging.debug("Creating ApiTest instance...")
        greeter = ApiTest(uri, client_id, client_secret, tenant_id, scope)

        # Specify the PDF file to upload and the output path for retrieval
        pdf_file_path = r"C:\Users\z004zn2u\OneDrive - Siemens AG\Desktop\CODED\Neo4J\data\Cricket.pdf"
        output_pdf_path = r"C:\Users\z004zn2u\OneDrive - Siemens AG\Desktop\CODED\Neo4J\data\retrieved_Cricket.pdf"

        # Upload the PDF to Neo4j
        logging.info("Uploading PDF to Neo4j...")
        greeter.upload_pdf(pdf_file_path)

        # Retrieve the PDF from Neo4j
        logging.info("Retrieving PDF from Neo4j...")
        greeter.retrieve_pdf("Cricket.pdf", output_pdf_path)

        # Close the driver
        greeter.close()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.exception("An error occurred during script execution.")