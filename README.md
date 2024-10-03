# elasticsearch

https://drive.google.com/file/d/1hu9vCVy6SPLz4LQz8UiraCKiutvRSJ28/view?usp=drivesdk

from elasticsearch import Elasticsearch, helpers
import pandas as pd
import os
from datetime import datetime

es = Elasticsearch(
    "http://localhost:9200",
    basic_auth=("elastic", "elastic")  
)

index_settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1
    },
    "mappings": {
        "properties": {
            "Employee ID": {"type": "text"},
            "Full Name": {"type": "text"},
            "Job Title": {"type": "text"},
            "Department": {"type": "text"},
            "Business Unit": {"type": "text"},
            "Gender": {"type": "text"},
            "Ethnicity": {"type": "text"},
            "Age": {"type": "integer"},
            "Hire Date": {"type": "date"},  
            "Annual Salary": {"type": "integer"},
            "Bonus %": {"type": "text"},
            "Country": {"type": "text"},
            "City": {"type": "text"},
            "Exit Date": {"type": "date"}  
        }
    }
}

def createCollection(p_collection_name):
    p_collection_name = p_collection_name.lower()  
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name, body=index_settings)
        print(f"Index '{p_collection_name}' created")
    else:
        print(f"Index '{p_collection_name}' already exists")

def indexData(p_collection_name, p_exclude_column):
    p_collection_name = p_collection_name.lower()

    csv_file_path = 'C:\\Users\\nilof\\Downloads\\archive\\employee.csv' 
    if os.path.exists(csv_file_path):
        df = pd.read_csv(csv_file_path, encoding='latin1')

        if 'Hire Date' in df.columns:
            df['Hire Date'] = df['Hire Date'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').isoformat() if pd.notnull(x) else None)

        if 'Exit Date' in df.columns:
            df['Exit Date'] = df['Exit Date'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').isoformat() if pd.notnull(x) else None)

        if 'Annual Salary' in df.columns:
            df['Annual Salary'] = df['Annual Salary'].apply(lambda x: int(x.replace('$', '').replace(',', '').strip()) if pd.notnull(x) else None)

        if p_exclude_column in df.columns:
            df = df.drop(columns=[p_exclude_column])

        def generate_data(df):
            for _, row in df.iterrows():
                yield {
                    "_index": p_collection_name,
                    "_source": row.to_dict()
                }

        helpers.bulk(es, generate_data(df), raise_on_error=False)
        print(f"Data indexed into '{p_collection_name}' excluding column '{p_exclude_column}'")
    else:
        print(f"CSV file not found at '{csv_file_path}'")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    p_collection_name = p_collection_name.lower()
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    result = es.search(index=p_collection_name, body=query)
    return result['hits']['hits']

def getEmpCount(p_collection_name):
    p_collection_name = p_collection_name.lower()
    query = {"query": {"match_all": {}}}
    result = es.count(index=p_collection_name, body=query)
    return result['count']

def delEmpById(p_collection_name, p_employee_id):
    p_collection_name = p_collection_name.lower()
    query = {
        "query": {
            "match": {
                "Employee ID": p_employee_id
            }
        }
    }
    result = es.delete_by_query(index=p_collection_name, body=query)
    return result['deleted']

def getDepFacet(p_collection_name):
    p_collection_name = p_collection_name.lower()
    query = {
        "size": 0,
        "aggs": {
            "department_count": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    result = es.search(index=p_collection_name, body=query)
    return result['aggregations']['department_count']['buckets']


v_nameCollection = 'Hash_Nilofar'.lower()  
v_phoneCollection = 'Hash_1234'.lower()  


createCollection(v_nameCollection)
createCollection(v_phoneCollection)

print(f"Employee count in '{v_nameCollection}': {getEmpCount(v_nameCollection)}")

indexData(v_nameCollection, 'Department')

indexData(v_phoneCollection, 'Gender')

delEmpById(v_nameCollection, 'E02003')

print(f"Employee count in '{v_nameCollection}' after deletion: {getEmpCount(v_nameCollection)}")

print(f"Search by 'Department' = 'IT' in '{v_nameCollection}': {searchByColumn(v_nameCollection, 'Department', 'IT')}")
print(f"Search by 'Gender' = 'Male' in '{v_nameCollection}': {searchByColumn(v_nameCollection, 'Gender', 'Male')}")
print(f"Search by 'Department' = 'IT' in '{v_phoneCollection}': {searchByColumn(v_phoneCollection, 'Department', 'IT')}")

print(f"Department facet for '{v_nameCollection}': {getDepFacet(v_nameCollection)}")
print(f"Department facet for '{v_phoneCollection}': {getDepFacet(v_phoneCollection)}")

