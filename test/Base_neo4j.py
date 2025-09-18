import os
import csv
import numpy as np
from neo4j import GraphDatabase
from langchain_community.graphs import Neo4jGraph

NEO4J_URI = "neo4j+s://<your-instance-id>.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "<your-password>"
OPENAI_API_KEY = "<your-openai-key>"

os.environ["NEO4J_URI"] = NEO4J_URI
os.environ["NEO4J_USERNAME"] = NEO4J_USER
os.environ["NEO4J_PASSWORD"] = NEO4J_PASSWORD
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY


graph = Neo4jGraph(refresh_schema=False)

NODES_CSV = r"C:\Users\z004zn2u\OneDrive - Siemens AG\Desktop\CODED\Neo4J\data\nodes.csv"
RELATIONSHIPS_CSV = r"C:\Users\z004zn2u\OneDrive - Siemens AG\Desktop\CODED\Neo4J\data\relationships.csv"
def get_label_for_type(node_type):
    mapping = {
        "Supplier": "Supplier",
        "Manufacturer": "Manufacturer",
        "Distributor": "Distributor",
        "Retailer": "Retailer",
        "Product": "Product"
    }
    return mapping.get(node_type, "Entity")


def ingest_nodes(driver):
    with driver.session() as session:
        with open(NODES_CSV, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                node_id = row['id:ID']
                name = row['name']
                node_type = row['type']
                location = row['location']
                supply_capacity = np.random.randint(1000, 50001)
                description = row['description']
                label = get_label_for_type(node_type)
                if location.strip():
                    query = f"""
                    MERGE (n:{label} {{id:$id}})
                    SET n.name = $name, n.location = $location, 
                        n.description = $description, n.supply_capacity = $supply_capacity
                    """
                    params = {
                        "id": node_id,
                        "name": name,
                        "location": location,
                        "description": description,
                        "supply_capacity": supply_capacity
                    }
                else:
                    query = f"""
                    MERGE (n:{label} {{id:$id}})
                    SET n.name = $name
                    """
                    params = {"id": node_id, "name": name}
                session.run(query, params)


def ingest_relationships(driver):
    with driver.session() as session:
        with open(RELATIONSHIPS_CSV, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                start_id = row[':START_ID']
                end_id = row[':END_ID']
                rel_type = row[':TYPE']
                product = row['product']
                if product.strip():
                    query = f"""
                    MATCH (start {{id:$start_id}})
                    MATCH (end {{id:$end_id}})
                    MERGE (start)-[r:{rel_type} {{product:$product}}]->(end)
                    """
                    params = {
                        "start_id": start_id,
                        "end_id": end_id,
                        "product": product
                    }
                else:
                    query = f"""
                    MATCH (start {{id:$start_id}})
                    MATCH (end {{id:$end_id}})
                    MERGE (start)-[r:{rel_type}]->(end)
                    """
                    params = {
                        "start_id": start_id,
                        "end_id": end_id
                    }
                session.run(query, params)
                

def create_indexes(driver):
    with driver.session() as session:
        for label in ["Supplier", "Manufacturer", "Distributor", "Retailer", "Product"]:
            session.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE")
            
            
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
create_indexes(driver)
ingest_nodes(driver)
ingest_relationships(driver)
print("Data ingestion complete.")
driver.close()


