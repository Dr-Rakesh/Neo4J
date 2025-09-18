import re
from langchain_core.tools import tool
from pydantic import BaseModel, Field
from typing import Optional, Dict, List
from langchain_openai import AzureChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import StateGraph, START, MessagesState
from langgraph.prebuilt import tools_condition, ToolNode
from IPython.display import Image, display
from langchain_core.messages import HumanMessage



# Azure OpenAI configuration
endpoint = "https://openai-aiattack-msa-001758-swedencentral-adi.openai.azure.com/"
api_key = "d7bfff12bdee4e1499f78ca39e76fba5"
api_version = "2024-12-01-preview"
deployment = "gpt-4o"  # Using the same deployment as in the example


class SupplierCountInput(BaseModel):
    min_supply_amount: Optional[int] = Field(
        description="Minimum supply amount of the suppliers"
    )
    max_supply_amount: Optional[int] = Field(
        description="Maximum supply amount of the suppliers"
    )
    grouping_key: Optional[str] = Field(
        description="The key to group by the aggregation", 
        enum=["supply_capacity", "location"]
    )
    

def extract_param_name(filter: str) -> str:
    pattern = r'\$\w+'
    match = re.search(pattern, filter)
    if match:
        return match.group()[1:]
    return None

@tool("supplier-count", args_schema=SupplierCountInput)
def supplier_count(
    min_supply_amount: Optional[int],
    max_supply_amount: Optional[int],
    grouping_key: Optional[str],
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
    print(cypher_statement)  # Debugging output
    return graph.query(cypher_statement, params=params)

class SupplierListInput(BaseModel):
    sort_by: str = Field(description="How to sort Suppliers by supply capacity", enum=['supply_capacity'])
    k: Optional[int] = Field(description="Number of Suppliers to return")
    description: Optional[str] = Field(description="Description of the Suppliers")
    min_supply_amount: Optional[int] = Field(description="Minimum supply amount of the suppliers")
    max_supply_amount: Optional[int] = Field(description="Maximum supply amount of the suppliers")
    
    
@tool("supplier-list", args_schema=SupplierListInput)
def supplier_list(
    sort_by: str = "supply_capacity",
    k : int = 4,
    description: Optional[str] = None,
    min_supply_amount: Optional[int] = None,
    max_supply_amount: Optional[int] = None,
) -> List[Dict]:
    """List suppliers based on particular filters"""

    # Handle vector-only search when no prefiltering is applied
    if description and not min_supply_amount and not max_supply_amount:
        return neo4j_vector.similarity_search(description, k=k)
    filters = [
        ("t.supply_capacity >= $min_supply_amount", min_supply_amount),
        ("t.supply_capacity <= $max_supply_amount", max_supply_amount)
    ]
    params = {
        key.split("$")[1]: value for key, value in filters if value is not None
    }
    where_clause = " AND ".join([condition for condition, value in filters if value is not None])
    cypher_statement = "MATCH (t:Supplier) "
    if where_clause:
        cypher_statement += f"WHERE {where_clause} "
    # Sorting and returning
    cypher_statement += " RETURN t.name AS name, t.location AS location, t.description as description, t.supply_capacity AS supply_capacity ORDER BY "
    if description:
        cypher_statement += (
            "vector.similarity.cosine(t.embedding, $embedding) DESC "
        )
        params["embedding"] = embedding.embed_query(description)
    elif sort_by == "supply_capacity":
        cypher_statement += "t.supply_capacity DESC "
    else:
        # Fallback or other possible sorting
        cypher_statement += "t.year DESC "
    cypher_statement += " LIMIT toInteger($limit)"
    params["limit"] = k or 4
    print(cypher_statement)  # Debugging output
    data = graph.query(cypher_statement, params=params)
    return data


# Initialize Azure OpenAI with LangChain
llm = AzureChatOpenAI(
    azure_endpoint=endpoint,
    api_key=api_key,
    api_version=api_version,
    azure_deployment=deployment,
    temperature=1.0
)

# Keep your tools and system message
tools = [supplier_count, supplier_list]
llm_with_tools = llm.bind_tools(tools)
sys_msg = SystemMessage(content="You are a helpful assistant tasked with finding and explaining relevant information about Supply chain")


def assistant(state: MessagesState):
   return {"messages": [llm_with_tools.invoke([sys_msg] + state["messages"])]}


builder = StateGraph(MessagesState)
builder.add_node("assistant", assistant)
builder.add_node("tools", ToolNode(tools))
# Define edges: 
builder.add_edge(START, "assistant")
# If there's a tool call, go to 'tools'; else finish
builder.add_conditional_edges("assistant", tools_condition)
builder.add_edge("tools", "assistant")
react_graph = builder.compile()
display(Image(react_graph.get_graph(xray=True).draw_mermaid_png()))


messages = [
    HumanMessage(
        content="Find suppliers that deal with steel and have at least 20000 supply capacity."
    )
]
result = react_graph.invoke({"messages": messages})
for m in result["messages"]:
    m.pretty_print()