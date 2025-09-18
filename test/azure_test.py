import requests
from neo4j import GraphDatabase, bearer_auth
import datetime

# Hardcoded Azure credentials and Neo4j configurations
client_id = "9c247b71-ce83-4c05-8366-26231320c348"
client_secret = "c4l8Q~ZAdY10CQ-HNKYCA3UowZWaPb4VIvaAwa0u"
tenant_id = "38ae3bcd-9579-4fd4-adda-b42e1495d55a"
database_id = "37be20b1"
scope = f"api://{client_id}/.default"
uri = f"neo4j+ssc://{database_id}.databases.neo4j.io"

# Function to get an access token from Azure
def get_access_token(client_id, client_secret, tenant_id, scope):
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
    response = requests.post(url, data=payload, headers=headers)
    result = response.json()
    if 'access_token' not in result:
        raise Exception(f"Failed to retrieve token: {result}")
    
    # Return token and its expiry timestamp
    expires_on = datetime.datetime.now() + datetime.timedelta(seconds=result['expires_in'])
    return result['access_token'], expires_on

# Class definition for Neo4j operations with token refresh
class ApiTest:
    def __init__(self, uri, client_id, client_secret, tenant_id, scope):
        self.uri = uri
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.scope = scope
        self.token, self.token_expiry = get_access_token(client_id, client_secret, tenant_id, scope)
        self.driver = GraphDatabase.driver(self.uri, auth=bearer_auth(self.token))

    def refresh_token_if_needed(self):
        # Check if token is close to expiration
        if datetime.datetime.now() >= self.token_expiry - datetime.timedelta(minutes=5):
            print("Refreshing token...")
            self.token, self.token_expiry = get_access_token(
                self.client_id, self.client_secret, self.tenant_id, self.scope
            )
            self.driver = GraphDatabase.driver(self.uri, auth=bearer_auth(self.token))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        self.refresh_token_if_needed()  # Refresh token if needed
        with self.driver.session() as session:
            greeting = session.execute_write(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

    def delete_greeting(self, message):
        self.refresh_token_if_needed()  # Refresh token if needed
        with self.driver.session() as session:
            session.execute_write(self._delete_greeting, message)

    @staticmethod
    def _delete_greeting(tx, message):
        tx.run("MATCH (a:Greeting {message: $message}) "
               "DELETE a", message=message)


if __name__ == "__main__":
    # Initialize ApiTest with Azure credentials
    greeter = ApiTest(uri, client_id, client_secret, tenant_id, scope)

    # Perform operations
    greeter.print_greeting("Welcome")
    greeter.delete_greeting("Welcome")
    greeter.close()