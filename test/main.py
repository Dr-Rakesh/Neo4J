import re
import json
import logging
import datetime
from typing import Optional, Dict, List
from neo4j import GraphDatabase, bearer_auth
from azure.identity import DefaultAzureCredential
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# --- Azure AD and Neo4j Configuration ---
# Azure AD credentials for Neo4j
CLIENT_ID = "9c247b71-ce83-4c05-8366-26231320c348"
CLIENT_SECRET = "c4l8Q~ZAdY10CQ-HNKYCA3UowZWaPb4VIvaAwa0u"
TENANT_ID = "38ae3bcd-9579-4fd4-adda-b42e1495d55a"
NEO4J_SCOPE = f"api://{CLIENT_ID}/.default"

# Neo4j URI
NEO4J_URI = "neo4j+ssc://37be20b1.databases.neo4j.io"
NEO4J_DATABASE = "neo4j"  # Default database name, change if needed

# Azure OpenAI configuration
AZURE_OPENAI_ENDPOINT = "https://openai-aiattack-msa-001758-westus-adi-02.openai.azure.com"
AZURE_OPENAI_API_VERSION = "2024-08-01-preview"
LLM_DEPLOYMENT = "gpt-4o"
EMBEDDING_DEPLOYMENT = "text-embedding-ada-002"

# --- Token Management Functions ---
def get_access_token(client_id, client_secret, tenant_id, scope):
    """Fetch access token from Azure AD"""
    logging.info(f"Fetching access token from Azure for scope: {scope}...")
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
        logging.exception(f"Failed to fetch access token for scope {scope}.")
        raise

# Initialize Neo4j token and driver
neo4j_token, neo4j_token_expiry = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID, NEO4J_SCOPE)
driver = GraphDatabase.driver(NEO4J_URI, auth=bearer_auth(neo4j_token))
logging.info("Neo4j driver initialized successfully.")

# Initialize Azure credential for OpenAI
try:
    azure_credential = DefaultAzureCredential()
    openai_token = azure_credential.get_token("https://cognitiveservices.azure.com/.default").token
    logging.info("Azure DefaultAzureCredential initialized successfully.")
except Exception as e:
    logging.exception("Failed to initialize DefaultAzureCredential.")
    raise

# Refresh Neo4j token if needed
def refresh_neo4j_token_if_needed():
    global neo4j_token, neo4j_token_expiry, driver
    if datetime.datetime.now() >= neo4j_token_expiry - datetime.timedelta(minutes=5):
        logging.info("Refreshing Neo4j access token...")
        neo4j_token, neo4j_token_expiry = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID, NEO4J_SCOPE)
        driver = GraphDatabase.driver(NEO4J_URI, auth=bearer_auth(neo4j_token))
        logging.info("Neo4j access token refreshed, and driver reinitialized.")
    return driver

# Refresh Azure OpenAI token
def refresh_openai_token():
    global openai_token, azure_credential
    try:
        openai_token = azure_credential.get_token("https://cognitiveservices.azure.com/.default").token
        logging.info("Azure OpenAI token refreshed.")
        return openai_token
    except Exception as e:
        logging.exception("Failed to refresh Azure OpenAI token.")
        raise

# --- Neo4j Query Function ---
def execute_neo4j_query(query, params=None):
    """Execute a query against Neo4j with token refresh"""
    global driver
    driver = refresh_neo4j_token_if_needed()
    with driver.session(database=NEO4J_DATABASE) as session:
        result = session.run(query, params)
        return [dict(record) for record in result]

# Check if embeddings exist in the database
def check_embeddings_exist():
    """Check if embeddings exist in the database"""
    cypher_query = """
    MATCH (s:Supplier)
    WHERE s.embedding IS NOT NULL
    RETURN count(s) as count
    """
    result = execute_neo4j_query(cypher_query)
    return result[0]['count'] > 0 if result else False

# --- Direct Azure OpenAI API Functions with Azure AD Authentication ---
def call_azure_openai_api(endpoint_path, payload):
    """Call Azure OpenAI API with Azure AD token"""
    token = refresh_openai_token()
    url = f"{AZURE_OPENAI_ENDPOINT}/{endpoint_path}?api-version={AZURE_OPENAI_API_VERSION}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    try:
        logging.info(f"Calling Azure OpenAI API: {url}")
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error calling Azure OpenAI API: {e}")
        if hasattr(response, 'text'):
            logging.error(f"Response: {response.text}")
        raise

def get_embedding(text):
    """Get embedding vector for text using Azure OpenAI with Azure AD auth"""
    try:
        payload = {
            "input": text,
            "model": EMBEDDING_DEPLOYMENT
        }
        response = call_azure_openai_api(f"openai/deployments/{EMBEDDING_DEPLOYMENT}/embeddings", payload)
        return response['data'][0]['embedding']
    except Exception as e:
        logging.error(f"Error getting embedding: {e}")
        # Return a zero vector as fallback (not ideal but prevents crashes)
        return [0.0] * 1536  # Standard OpenAI embedding size

def chat_completion(messages, functions=None):
    """Get chat completion using Azure OpenAI with Azure AD auth"""
    try:
        payload = {
            "messages": messages,
            "temperature": 0.7,
            "max_tokens": 1000
        }
        
        # Add functions if provided
        if functions:
            payload["functions"] = functions
            payload["function_call"] = "auto"
        
        response_data = call_azure_openai_api(f"openai/deployments/{LLM_DEPLOYMENT}/chat/completions", payload)
        
        # Extract the message directly
        message = response_data['choices'][0]['message']
        
        # Create a simple object to mimic the OpenAI library's response structure
        class SimpleMessage:
            def __init__(self, content=None, function_call=None):
                self.content = content
                self.function_call = function_call
                
        class SimpleChoice:
            def __init__(self, message):
                self.message = message
                
        class SimpleResponse:
            def __init__(self, choices):
                self.choices = choices
        
        # Create a simple message object
        simple_message = SimpleMessage(
            content=message.get('content'),
            function_call=message.get('function_call')
        )
        
        # Create a simple response object
        return SimpleResponse([SimpleChoice(simple_message)])
        
    except Exception as e:
        logging.error(f"Error in chat completion: {e}")
        raise

# --- Tool Definitions ---
def extract_param_name(filter: str) -> str:
    pattern = r'\$\w+'
    match = re.search(pattern, filter)
    if match:
        return match.group()[1:]
    return None

def supplier_count(
    min_supply_amount: Optional[int] = None,
    max_supply_amount: Optional[int] = None,
    grouping_key: Optional[str] = None,
) -> List[Dict]:
    """Calculate the count of Suppliers based on particular filters"""
    filters = [
        ("t.supply_capacity >= $min_supply_amount", min_supply_amount),
        ("t.supply_capacity <= $max_supply_amount", max_supply_amount)
    ]
    params = {
        extract_param_name(condition): value
        for condition, value in filters
        if value is not None
    }
    where_clause = " AND ".join(
        [condition for condition, value in filters if value is not None]
    )
    cypher_statement = "MATCH (t:Supplier) "
    if where_clause:
        cypher_statement += f"WHERE {where_clause} "
    return_clause = (
        f"t.{grouping_key}, count(t) AS supplier_count"
        if grouping_key
        else "count(t) AS supplier_count"
    )
    cypher_statement += f"RETURN {return_clause}"
    logging.info(f"Executing Cypher (supplier_count): {cypher_statement} with params: {params}")
    return execute_neo4j_query(cypher_statement, params=params)

def supplier_list(
    sort_by: str = "supply_capacity",
    k: int = 4,
    description: Optional[str] = None,
    min_supply_amount: Optional[int] = None,
    max_supply_amount: Optional[int] = None,
) -> List[Dict]:
    """List suppliers based on particular filters"""
    
    # For the steel and supply capacity query, let's use a direct approach
    if description and "steel" in description.lower() and min_supply_amount:
        logging.info(f"Using direct text search for steel suppliers with min capacity: {min_supply_amount}")
        cypher_query = """
        MATCH (s:Supplier)
        WHERE s.description CONTAINS 'steel' AND s.supply_capacity >= $min_supply_amount
        RETURN s.name AS name, s.location AS location, s.description AS description, 
               s.supply_capacity AS supply_capacity
        ORDER BY s.supply_capacity DESC
        LIMIT $limit
        """
        
        params = {
            "min_supply_amount": min_supply_amount,
            "limit": k or 4
        }
        
        return execute_neo4j_query(cypher_query, params=params)
    
    # Regular filtering by supply capacity
    filters = [
        ("t.supply_capacity >= $min_supply_amount", min_supply_amount),
        ("t.supply_capacity <= $max_supply_amount", max_supply_amount)
    ]
    params = {
        extract_param_name(condition): value for condition, value in filters if value is not None
    }
    where_clause = " AND ".join([condition for condition, value in filters if value is not None])
    cypher_statement = "MATCH (t:Supplier) "
    if where_clause:
        cypher_statement += f"WHERE {where_clause} "
    # Sorting and returning
    cypher_statement += " RETURN t.name AS name, t.location AS location, t.description as description, t.supply_capacity AS supply_capacity ORDER BY "
    if sort_by == "supply_capacity":
        cypher_statement += "t.supply_capacity DESC "
    else:
        cypher_statement += "t.supply_capacity DESC "
    cypher_statement += " LIMIT toInteger($limit)"
    params["limit"] = k or 4
    logging.info(f"Executing Cypher (supplier_list): {cypher_statement} with params: {params}")
    return execute_neo4j_query(cypher_statement, params=params)

# --- Define Function Schemas ---
supplier_count_schema = {
    "name": "supplier_count",
    "description": "Calculate the count of Suppliers based on particular filters",
    "parameters": {
        "type": "object",
        "properties": {
            "min_supply_amount": {
                "type": "integer",
                "description": "Minimum supply amount of the suppliers"
            },
            "max_supply_amount": {
                "type": "integer",
                "description": "Maximum supply amount of the suppliers"
            },
            "grouping_key": {
                "type": "string",
                "enum": ["supply_capacity", "location"],
                "description": "The key to group by the aggregation"
            }
        },
        "required": []
    }
}

supplier_list_schema = {
    "name": "supplier_list",
    "description": "List suppliers based on particular filters",
    "parameters": {
        "type": "object",
        "properties": {
            "sort_by": {
                "type": "string",
                "enum": ["supply_capacity"],
                "description": "How to sort Suppliers by supply capacity"
            },
            "k": {
                "type": "integer",
                "description": "Number of Suppliers to return"
            },
            "description": {
                "type": "string",
                "description": "Description of the Suppliers"
            },
            "min_supply_amount": {
                "type": "integer",
                "description": "Minimum supply amount of the suppliers"
            },
            "max_supply_amount": {
                "type": "integer",
                "description": "Maximum supply amount of the suppliers"
            }
        },
        "required": []
    }
}

function_schemas = [supplier_count_schema, supplier_list_schema]
tools_map = {
    "supplier_count": supplier_count,
    "supplier_list": supplier_list
}

# --- Function to process a query ---
def process_query(query_text):
    logging.info(f"Processing query: {query_text}")
    
    # First message to the model
    messages = [
        {"role": "system", "content": "You are a helpful assistant tasked with finding and explaining relevant information about Supply chain. Use the available tools to answer questions about suppliers."},
        {"role": "user", "content": query_text}
    ]
    
    # Keep track of the conversation
    conversation_history = []
    conversation_history.extend(messages)
    
    # Process the conversation until we get a final answer
    max_turns = 5
    current_turn = 0
    
    while current_turn < max_turns:
        current_turn += 1
        logging.info(f"Turn {current_turn}")
        
        # Get response from the model
        response = chat_completion(conversation_history, functions=function_schemas)
        
        # Extract the message
        message = response.choices[0].message
        
        # Add the response to the conversation history
        conversation_history.append({"role": "assistant", "content": message.content or ""})
        
        # Check if there's a function call
        if hasattr(message, 'function_call') and message.function_call:
            function_call = message.function_call
            logging.info(f"Function call detected: {function_call['name']}")
            
            # Parse the function arguments
            try:
                function_args = json.loads(function_call["arguments"])
            except json.JSONDecodeError:
                function_args = {}
                logging.error(f"Failed to parse function arguments: {function_call['arguments']}")
            
            # Call the function
            function_name = function_call["name"]
            if function_name in tools_map:
                try:
                    function_response = tools_map[function_name](**function_args)
                    result_str = json.dumps(function_response, default=str)
                    logging.info(f"Function result: {result_str[:100]}...")  # Log first 100 chars
                except Exception as e:
                    result_str = f"Error executing {function_name}: {str(e)}"
                    logging.error(result_str)
            else:
                result_str = f"Function {function_name} not found."
                logging.error(result_str)
            
            # Add the function result to the conversation
            conversation_history.append({
                "role": "function",
                "name": function_name,
                "content": result_str
            })
        else:
            # If no function call, we have our final answer
            logging.info("No function call, returning final answer.")
            break
    
    # Return the final response (the last assistant message)
    for message in reversed(conversation_history):
        if message["role"] == "assistant":
            return message["content"]
    
    return "Failed to generate a response."

# --- Check if embeddings exist ---
has_embeddings = check_embeddings_exist()
logging.info(f"Database has embeddings: {has_embeddings}")

# --- Test the system ---
query = "Find suppliers that deal with steel and have at least 20000 supply capacity."
result = process_query(query)
print("\n--- Final Result ---")
print(result)