# Step 1: Install required libraries
!pip install elasticsearch pandas

# Step 2: Import libraries
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
from google.colab import files

# Step 3: Upload the CSV file
uploaded = files.upload()  # This will allow you to upload your dataset

# Step 4: Initialize the Elasticsearch client
es = Elasticsearch(['https:<elastic-cloud-id>'],
                   api_key=('xxxx', 'xxxx'))

# Step 5: Define functions
def createCollection(p_collection_name):
    es.indices.create(index=p_collection_name, ignore=400)  # Ignore 400 if index exists

def indexData(p_collection_name, p_exclude_column):
    # Load data from the uploaded CSV file
    try:
        df = pd.read_csv(next(iter(uploaded)), encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(next(iter(uploaded)), encoding='ISO-8859-1')  # Fallback to ISO-8859-1

    # Drop the excluded column
    df = df.drop(columns=[p_exclude_column])

    # Replace NaN values with None
    df = df.where(pd.notnull(df), None)

    # Prepare the data for bulk indexing
    actions = []
    for _, row in df.iterrows():
        action = {
            "_index": p_collection_name,
            "_id": str(row["Employee ID"]),
            "_source": row.to_dict()
        }
        actions.append(action)

    # Perform bulk indexing
    try:
        success, failed = bulk(es, actions)
        print(f"Successfully indexed {success} documents.")
        print(f"Failed to index {failed} documents.")
    except Exception as e:
        print("An error occurred during bulk indexing:", e)

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    response = es.search(index=p_collection_name, body=query)
    return response['hits']['hits']

def getEmpCount(p_collection_name):
    return es.count(index=p_collection_name)['count']

def delEmpById(p_collection_name, p_employee_id):
    es.delete(index=p_collection_name, id=p_employee_id)

def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword"  # Adjust based on your data
                }
            }
        }
    }
    response = es.search(index=p_collection_name, body=query)
    return response['aggregations']['departments']['buckets']

# Step 6: Execute the functions
v_nameCollection = 'hash_kaviya'  # Replace with your name in lowercase
v_phoneCollection = 'hash_1234'  # Replace with your phone's last four digits in lowercase

# Create collections (indexes)
createCollection(v_nameCollection)
createCollection(v_phoneCollection)

# Index data (make sure to exclude the correct column)
indexData(v_nameCollection, 'Department')  # Excluding 'Department' from Name Collection
indexData(v_phoneCollection, 'Gender')      # Excluding 'Gender' from Phone Collection

# Get employee count
print("Employee Count in Name Collection:", getEmpCount(v_nameCollection))

# Example deletion (replace 'E02003' with a valid employee ID)
delEmpById(v_nameCollection, 'E02003')
print("Employee Count after deletion:", getEmpCount(v_nameCollection))

# Perform searches
print("Search IT Department:", searchByColumn(v_nameCollection, 'Department', 'IT'))
print("Search Male Gender:", searchByColumn(v_nameCollection, 'Gender', 'Male'))
print("Search IT Department in Phone Collection:", searchByColumn(v_phoneCollection, 'Department', 'IT'))

# Get department facets
print("Department Facet in Name Collection:", getDepFacet(v_nameCollection))
print("Department Facet in Phone Collection:", getDepFacet(v_phoneCollection))
