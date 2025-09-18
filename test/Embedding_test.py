import os
from openai import AzureOpenAI

# Azure OpenAI endpoint and key
endpoint = "https://openai-aiattack-msa-001758-swedencentral-adi.openai.azure.com/"
api_key = "d7bfff12bdee4e1499f78ca39e76fba5"

# Deployment and API version
deployment_id = "ada003"

# Initialize the Azure OpenAI client
client = AzureOpenAI(
    azure_endpoint=endpoint, 
    api_key=api_key,
    api_version="2024-02-01"
)

# Request embeddings for multiple phrases
response = client.embeddings.create(
    input=["first phrase", "second phrase", "third phrase"],
    model=deployment_id
)

# Process and print the embeddings
for item in response.data:
    length = len(item.embedding)
    print(
        f"data[{item.index}]: length={length}, "
        f"[{item.embedding[0]}, {item.embedding[1]}, "
        f"..., {item.embedding[length-2]}, {item.embedding[length-1]}]"
    )