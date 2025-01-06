from flask import Flask, request, jsonify, render_template
import utils as ut  # Import the functions
import random
import sqlalchemy 
from pymongo import MongoClient

app = Flask(__name__)

column_details = {}
all_columns = []
all_attributes = []
all_measures = []
all_unique_elements = {}
input_dataset_paths_global=[]
dataset_samples={}
db_url="mysql+pymysql://root:1234@localhost/dsci551trial"
mongo_uri='mongodb://localhost:27017'
database_type=''
mongo_db_name='dsci551'

def execute_sql_query(sql_query):
    """
    Executes a given SQL query and fetches the result.
    :param sql_query: The SQL query string to execute.
    :return: Result rows as a list of dictionaries.
    """
    global db_url
    try:
        engine = sqlalchemy.create_engine(db_url)
        with engine.connect() as connection:
            result = connection.execute(sqlalchemy.text(sql_query))
            rows = [dict(zip(result.keys(), row)) for row in result]
            
            # Handle empty result set
            if not rows:
                return {"message": "No results found"}
            
            return rows
    except Exception as e:
        print(f"Error executing query: {e}")
        return {"error": str(e)}


            # return [dict(row) for row in result]  # Convert result rows to a list of dictionaries
    except Exception as e:
        print(f"Error executing query: {e}")
        return {"error": str(e)}

def execute_mongo_query(translated_query, collection_name, final_pipeline):
    """
    Executes a MongoDB query based on the translated query and returns the result as JSON.
    :param translated_query: Dictionary containing MongoDB query details, including the collection and pipeline.
    :return: JSON result of the query.
    """
    global mongo_uri,mongo_db_name
    try:
       
        pipeline = final_pipeline

        if not collection_name or not pipeline:
            raise ValueError("Translated query must include 'collection' and 'pipeline'.")

       
        client = MongoClient(mongo_uri)
        db = client[mongo_db_name]
        collection = db[collection_name]

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Return the result as a JSON-friendly list
        return result
    except Exception as e:
        print(f"Error executing MongoDB query: {e}")
        return {"error": str(e)}
    

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/load_datasets', methods=['POST'])
def load_datasets():
    global column_details, all_columns, all_attributes, all_measures, all_unique_elements,input_dataset_paths_global,dataset_samples,db_url,mongo_uri,database_type
    database_type = request.json['database_type']
    input_dataset_paths = request.json['dataset_paths']
    input_dataset_paths_global=input_dataset_paths
    if database_type == "SQL":
        for path in input_dataset_paths_global:
            table_name = path.replace('.csv', '')
            column_details[path] = {}
            column_details[path]['column_names'], column_details[path]['attributes'], column_details[path]['measures'], column_details[path]['unique_elements'],data = ut.load_csv_to_sql(
                path, db_url, table_name
            )
            dataset_samples[table_name] = data.head(5).to_dict(orient='records')
            all_columns.extend(column_details[path]['column_names'])
            all_attributes.extend(column_details[path]['attributes'])
            all_measures.extend(column_details[path]['measures'])
            all_unique_elements.update(column_details[path]['unique_elements'])

    elif database_type == "NoSQL":
        for csv_path in input_dataset_paths_global:
            column_details[csv_path]={}
            collection_name=csv_path.replace('.csv','')
            column_details[csv_path]['column_names'], column_details[csv_path]['attributes'], column_details[csv_path]['measures'], column_details[csv_path]['unique_elements'], sample_data = ut.load_csv_to_mongo(csv_path, mongo_uri, 'dsci551', collection_name)
            dataset_samples[collection_name] = sample_data
            all_columns.extend(column_details[csv_path]['column_names'])
            all_attributes.extend(column_details[csv_path]['attributes'])
            all_measures.extend(column_details[csv_path]['measures'])
            all_unique_elements.update(column_details[csv_path]['unique_elements'])


    all_columns = list(set(all_columns))
    all_attributes = list(set(all_attributes))
    all_measures = list(set(all_measures))
    response={
        "message": f"Datasets loaded successfully for {database_type} database!",
        "samples": dataset_samples  # Include dataset samples in the response
    }
    return jsonify(response)

@app.route('/process_query', methods=['POST'])
def process_query():
    global database_type
    database_type = request.json['database_type']
    input_user_query = request.json['query']
    response = {}
    if 'sample' in input_user_query:
        if database_type == "SQL":
            output_samples=ut.output_sample_queries(input_user_query,input_dataset_paths_global,all_columns,column_details)
        elif database_type == "NoSQL":
            output_samples = ut.output_sample_queries_mongo(input_user_query, input_dataset_paths_global, all_columns, column_details)
    
        no_of_output_queries=5
        if len(output_samples)<no_of_output_queries:
            no_of_output_queries=len(output_samples)
        random_queries=random.sample(output_samples,no_of_output_queries)
        # Use an external function for sample queries
        response = {"samples": random_queries}
    else:
        # Use an external function for SQL translation
        if database_type == "SQL":
            translated_query = ut.translate_to_sql(input_user_query,input_dataset_paths_global,all_attributes,all_measures,all_columns,column_details)
            result = execute_sql_query(translated_query)
        elif database_type == "NoSQL":
            translated_query, collection_name, final_pipeline= ut.translate_to_mongo(input_user_query, input_dataset_paths_global, all_attributes, all_measures, all_columns, column_details)
            result = execute_mongo_query(translated_query,collection_name,final_pipeline)

        response = {"translated_query": translated_query}
        print('query',translated_query)
        if "error" in result:
            response["error"] = result["error"]
        else:
            response["query_result"] = result

    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)
