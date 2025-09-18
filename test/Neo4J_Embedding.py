import os
import requests
from langchain_openai import AzureOpenAIEmbeddings
from langchain_community.vectorstores import Neo4jVector

# Azure OpenAI credentials
endpoint = "https://openai-aiattack-msa-001758-swedencentral-adi.openai.azure.com/"
api_key = "d7bfff12bdee4e1499f78ca39e76fba5"
deployment_id = "ada003"

# Neo4j Connection URI
NEO4J_URI = "neo4j+ssc://37be20b1.databases.neo4j.io"

# Azure AD Authentication credentials
CLIENT_ID = "9c247b71-ce83-4c05-8366-26231320c348"
CLIENT_SECRET = "c4l8Q~ZAdY10CQ-HNKYCA3UowZWaPb4VIvaAwa0u"
TENANT_ID = "38ae3bcd-9579-4fd4-adda-b42e1495d55a"
SCOPE = f"api://{CLIENT_ID}/.default"

# Function to get access token from Azure AD
def get_access_token(client_id, client_secret, tenant_id, scope):
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
    return token_data['access_token']

# Get token for Neo4j authentication
token = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID, SCOPE)

# Clear any conflicting environment variables
if "OPENAI_API_BASE" in os.environ:
    del os.environ["OPENAI_API_BASE"]
if "OPENAI_API_TYPE" in os.environ:
    del os.environ["OPENAI_API_TYPE"]

# Initialize the Azure OpenAI embeddings using the correct parameters
embedding = AzureOpenAIEmbeddings(
    azure_deployment=deployment_id,  # Specify the deployment name
    azure_endpoint=endpoint,         # Use azure_endpoint instead of openai_api_base
    api_key=api_key,                 # Use api_key instead of openai_api_key
    api_version="2023-05-15"         # Use the appropriate API version
)

# Initialize Neo4j vector store
neo4j_vector = Neo4jVector.from_existing_graph(
    embedding=embedding,
    url=NEO4J_URI,
    username="neo4j",
    password=token,
    index_name="supply_chain",
    node_label="Supplier",
    text_node_properties=["description"],
    embedding_node_property="embedding",
)

# Example usage
# results = neo4j_vector.similarity_search("example query", k=5)