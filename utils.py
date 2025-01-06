import pandas as pd
import random
from sqlalchemy import create_engine
import copy
import re
from pymongo import MongoClient


def load_csv_to_sql(csv_path, db_url, table_name, if_exists='replace'):
    """
    Loads a CSV file into an SQL database.

    Parameters:
    - csv_path (str): Path to the CSV file.
    - db_url (str): Database URL in SQLAlchemy format (e.g., 'sqlite:///mydatabase.db' for SQLite,
                    'mysql+pymysql://user:password@localhost/dbname' for MySQL).
    - table_name (str): Name of the table where data should be stored.
    - if_exists (str): What to do if the table already exists. Options: 'fail', 'replace', 'append'.
                       Default is 'replace'.

    Returns:
    - None
    """
    # Load CSV into a DataFrame
    try:
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.lower()
        column_names=df.columns
        attributes = []  # Categorical variables
        measures = []    # Continuous variables
        #dates = []       # Date variables
        unique_elements={}

# Identify categorical, continuous, and date variables
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                attributes.append(col)  # Date if datetime type
                unique_elements[col]=df[col].unique().tolist()
            elif pd.api.types.is_numeric_dtype(df[col]) and 'id' != col[-2:]:
                measures.append(col)  # Continuous if numeric with many unique values
            else:
                attributes.append(col)
                unique_elements[col]=df[col].unique().tolist()
        
        
    
        print(f"CSV file loaded successfully with {len(df)} rows and {len(df.columns)} columns.")
        
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    # Connect to the database
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            # Store the DataFrame into the SQL database
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            print(f"Data successfully loaded into '{table_name}' table in the database.")
            return list(column_names),list(attributes),list(measures),unique_elements,df
    except Exception as e:
        print(f"Error connecting to the database or writing to the table: {e}")


def create_sample_query(query_type,column_names,attributes,measures,unique_elements,user_dataset):


    # if query_type=='group by':
    #     agg_functions=['Sum','Avg','Min','Max']
    #     group_by_samples=[]
    #     for attr in attributes:
    #         for measure in measures:
    #             for func in agg_functions:
    #                 group_by_sample_query_1=f'Select {attr},{func}({measure}) as {func}_{measure} from {user_dataset} group by {attr}'
    #                 group_by_samples.append(group_by_sample_query_1)
    #                 group_by_sample_query_2=f'Select {attr} from {user_dataset} group by {attr}'
    #                 group_by_samples.append(group_by_sample_query_2)


    if query_type.lower()=='group by':
        agg_functions=['Sum','Avg','Min','Max']
        group_by_samples=[]
        for i in range (0,20):
            no_of_select_columns=random.randint(1,int(len(attributes)/2)+1)
            select_columns=random.sample(attributes,no_of_select_columns)
            select_columns_text=','.join(select_columns)
            selected_agg_function=random.sample(agg_functions,1)[0]
            selected_agg_column=random.sample(measures,1)[0]
            group_by_sample_1=f'Select {select_columns_text},{selected_agg_function}({selected_agg_column}) as {selected_agg_column}_{selected_agg_function} from {user_dataset} group by {select_columns_text}'
            group_by_samples.append(group_by_sample_1)
            group_by_sample_2=f'Select {select_columns_text} from {user_dataset} group by {select_columns_text}'
            group_by_samples.append(group_by_sample_2)
        return group_by_samples

    # if query_type=='Sum':
    #     sum_samples=[]
    #     for measure in measures:
    #         for attr in attributes:
    #             sum_sample_query_1=f'Select {attr},sum({measure} as sum_{measure} from {user_dataset} group by {attr})'
    #             sum_samples.append(sum_sample_query_1)
    #         sum_sample_query_2=f'Select sum({measure}) as sum_{measure} from {user_dataset}'
    #         sum_samples.append(sum_sample_query_2)

    if query_type.lower()=='sum':
        sum_samples=[]
        for i in range (0,20):
            no_of_select_columns=random.randint(1,int(len(attributes)/2)+1)
            select_columns=random.sample(attributes,no_of_select_columns)
            select_columns_text=','.join(select_columns)
            selected_agg_column=random.sample(measures,1)[0]
            sum_sample_1=f'Select {select_columns_text},Sum({selected_agg_column}) as Sum_{selected_agg_column} from {user_dataset} group by {select_columns_text}'
            sum_samples.append(sum_sample_1)
            sum_sample_2=f'Select Sum({selected_agg_column}) as Sum_{selected_agg_column} from {user_dataset}'
            sum_samples.append(sum_sample_2)
        return sum_samples



    # if query_type=='Avg':
    #     avg_samples=[]
    #     for measure in measures:
    #         for attr in attributes:
    #             avg_sample_query_1=f'Select {attr},avg{measure} as avg_{measure} from {user_dataset} group by {attr})'
    #             avg_samples.append(avg_sample_query_1)
    #         avg_sample_query_2=f'Select avg({measure}) as avg_{measure} from {user_dataset}'
    #         avg_samples.append(avg_sample_query_2)

    if query_type.lower()=='avg':
        Avg_samples=[]
        for i in range (0,20):
            no_of_select_columns=random.randint(1,int(len(attributes)/2)+1)
            select_columns=random.sample(attributes,no_of_select_columns)
            select_columns_text=','.join(select_columns)
            selected_agg_column=random.sample(measures,1)[0]
            Avg_sample_1=f'Select {select_columns_text},Avg({selected_agg_column}) as Avg_{selected_agg_column} from {user_dataset} group by {select_columns_text}'
            Avg_samples.append(Avg_sample_1)
            Avg_sample_2=f'Select Avg({selected_agg_column}) as Avg_{selected_agg_column} from {user_dataset}'
            Avg_samples.append(Avg_sample_2)
        return Avg_samples


    # if query_type=='Min':
    #     min_samples=[]
    #     for measure in measures:
    #         for attr in attributes:
    #             min_sample_query_1=f'Select {attr},min{measure} as min_{measure} from {user_dataset} group by {attr})'
    #             min_samples.append(min_sample_query_1)
    #         min_sample_query_2=f'Select min({measure}) as min_{measure} from {user_dataset}'
    #         min_samples.append(min_sample_query_2)

    if query_type.lower()=='min':
        Min_samples=[]
        for i in range (0,20):
            no_of_select_columns=random.randint(1,int(len(attributes)/2)+1)
            select_columns=random.sample(attributes,no_of_select_columns)
            select_columns_text=','.join(select_columns)
            selected_agg_column=random.sample(measures,1)[0]
            Min_sample_1=f'Select {select_columns_text},Min({selected_agg_column}) as Min_{selected_agg_column} from {user_dataset} group by {select_columns_text}'
            Min_samples.append(Min_sample_1)
            Min_sample_2=f'Select Min({selected_agg_column}) as Min_{selected_agg_column} from {user_dataset}'
            Min_samples.append(Min_sample_2)
        return Min_samples


    # if query_type=='Max':
    #     max_samples=[]
    #     for measure in measures:
    #         for attr in attributes:
    #             max_sample_query_1=f'Select {attr},max{measure} as max_{measure} from {user_dataset} group by {attr})'
    #             max_samples.append(max_sample_query_1)
    #         max_sample_query_2=f'Select max({measure}) as max_{measure} from {user_dataset}'
    #         max_samples.append(max_sample_query_2)

    if query_type.lower()=='max':
        Max_samples=[]
        for i in range (0,20):
            no_of_select_columns=random.randint(1,int(len(attributes)/2)+1)
            select_columns=random.sample(attributes,no_of_select_columns)
            select_columns_text=','.join(select_columns)
            selected_agg_column=random.sample(measures,1)[0]
            Max_sample_1=f'Select {select_columns_text},Max({selected_agg_column}) as Max_{selected_agg_column} from {user_dataset} group by {select_columns_text}'
            Max_samples.append(Max_sample_1)
            Max_sample_2=f'Select Max({selected_agg_column}) as Max_{selected_agg_column} from {user_dataset}'
            Max_samples.append(Max_sample_2)
        return Max_samples

    # if query_type=='Where':
    #     where_samples=[]
    #     for i,attr in enumerate(attributes):
    #         value_list=random.sample(unique_elements[attr],5)
    #         for j,value in enumerate(value_list):
    #             where_sample_query_1=f'Select * from {user_dataset} where {attr}={value}'
    #             where_samples.append(where_sample_query_1)
    #             no_of_select_columns=random.randint(1,int(len(column_names)/2)+1)
    #             select_columns=random.sample(column_names,no_of_select_columns)
    #             select_columns_text=','.join(select_columns)
    #             where_sample_query_2=f'Select {select_columns_text} from {user_dataset} where {attr}={value}'
    #             where_samples.append(where_sample_query_2)

    if query_type.lower()=='where':
        where_samples=[]
        for i in range(0,20):
            where_attr=random.sample(attributes,1)[0]
            where_value=random.sample(unique_elements[where_attr],1)[0]
            where_attr_2=random.sample(attributes,1)[0]
            where_value_2=random.sample(unique_elements[where_attr_2],1)[0]
            where_sample_1=f"Select * from {user_dataset} where {where_attr}='{where_value}'"
            no_of_select_columns=random.randint(1,int(len(column_names)/2)+1)
            select_columns=random.sample(column_names,no_of_select_columns)
            select_columns_text=','.join(select_columns)
            where_sample_2=f"Select {select_columns_text} from {user_dataset} where {where_attr}='{where_value}'"
            where_sample_3=f"Select {select_columns_text} from {user_dataset} where {where_attr}='{where_value}' and {where_attr_2}='{where_value_2}'"
            where_samples.append(where_sample_1)
            where_samples.append(where_sample_2)
            where_samples.append(where_sample_3)
        return where_samples
            


    if query_type.lower()=='order by':
        order_by_samples=[]
        for i in range(0,20):
            no_of_select_columns=random.randint(1,int(len(column_names)/2)+1)
            select_columns=random.sample(column_names,no_of_select_columns)
            select_columns_text=','.join(select_columns)
            aesc_desc_list=['aesc','desc']
            aesc_desc_selection=random.sample(aesc_desc_list,1)[0]
            order_by_column=random.sample(select_columns,1)[0]
            order_by_sample_1=f'Select {select_columns_text} from {user_dataset} order by {order_by_column} {aesc_desc_selection}'
            agg_functions=['Sum','Avg','Min','Max']
            no_of_select_cat_columns=random.randint(1,int(len(attributes)/2)+1)
            select_cat_columns=random.sample(attributes,no_of_select_cat_columns)
            select_cat_columns_text=','.join(select_cat_columns)
            selected_agg_function=random.sample(agg_functions,1)[0]
            selected_agg_column=random.sample(measures,1)[0]
            order_by_sample_2=f'Select {select_cat_columns_text},{selected_agg_function}({selected_agg_column}) as {selected_agg_column}_{selected_agg_function} from {user_dataset} group by {select_cat_columns_text} order by {selected_agg_column}_{selected_agg_function} {aesc_desc_selection}'
            order_by_samples.append(order_by_sample_1)
            order_by_samples.append(order_by_sample_2)
        return order_by_samples


    if query_type.lower()=='limit':
        limit_samples=[]
        for i in range(0,20):
            no_of_select_columns=random.randint(1,int(len(column_names)/2)+1)
            select_columns=random.sample(column_names,no_of_select_columns)
            select_columns_text=','.join(select_columns)
            limit_number=random.randint(1,50)
            limit_sample_1=f'Select  {select_columns_text} from {user_dataset} limit {limit_number}'
            limit_samples.append(limit_sample_1)
            agg_functions=['Sum','Avg','Min','Max']
            no_of_select_cat_columns=random.randint(1,int(len(attributes)/2)+1)
            select_cat_columns=random.sample(attributes,no_of_select_cat_columns)
            select_cat_columns_text=','.join(select_cat_columns)
            selected_agg_function=random.sample(agg_functions,1)[0]
            selected_agg_column=random.sample(measures,1)[0]
            aesc_desc_list=['aesc','desc']
            aesc_desc_selection=random.sample(aesc_desc_list,1)[0]
            limit_sample_2=f'Select {select_cat_columns_text},{selected_agg_function}({selected_agg_column}) as {selected_agg_column}_{selected_agg_function} from {user_dataset} group by {select_cat_columns_text} order by {selected_agg_column}_{selected_agg_function} {aesc_desc_selection} limit {limit_number}'
            limit_samples.append(limit_sample_2)
            offset_number=random.randint(1,50)
            limit_sample_3=f'Select {select_cat_columns_text},{selected_agg_function}({selected_agg_column}) as {selected_agg_column}_{selected_agg_function} from {user_dataset} group by {select_cat_columns_text} order by {selected_agg_column}_{selected_agg_function} {aesc_desc_selection} limit {offset_number},{limit_number}'
            limit_samples.append(limit_sample_3)
        return limit_samples

    if query_type.lower()=='offset':
        offset_samples=[]
        for i in range (0,20):
            no_of_select_columns=random.randint(1,int(len(column_names)/2)+1)
            select_columns=random.sample(column_names,no_of_select_columns)
            select_columns_text=','.join(select_columns)
            limit_number=random.randint(1,50)    
            offset_number=random.randint(1,50)
            offset_sample_1=f'Select {select_columns_text} from {user_dataset} offset {offset_number}'
            offset_sample_2=f'Select {select_columns_text} from {user_dataset} limit {limit_number} offset {offset_number}'
            offset_samples.append(offset_sample_1)
            offset_samples.append(offset_sample_2)   
            agg_functions=['Sum','Avg','Min','Max']
            no_of_select_cat_columns=random.randint(1,int(len(attributes)/2)+1)
            select_cat_columns=random.sample(attributes,no_of_select_cat_columns)
            select_cat_columns_text=','.join(select_cat_columns)
            selected_agg_function=random.sample(agg_functions,1)[0]
            selected_agg_column=random.sample(measures,1)[0]   
            aesc_desc_list=['aesc','desc']
            aesc_desc_selection=random.sample(aesc_desc_list,1)[0]          
            offset_sample_3=f'Select {select_cat_columns_text},{selected_agg_function}({selected_agg_column}) as {selected_agg_column}_{selected_agg_function} from {user_dataset} group by {select_cat_columns_text} order by {selected_agg_column}_{selected_agg_function} {aesc_desc_selection} offset {offset_number} limit {limit_number}'
            offset_samples.append(offset_sample_3)
        return offset_samples

    if query_type.lower()=='having':
        having_samples=[]
        for i in range (0,20):
            agg_functions=['Sum','Avg','Min','Max']
            no_of_select_cat_columns=random.randint(1,int(len(attributes)/2)+1)
            select_cat_columns=random.sample(attributes,no_of_select_cat_columns)
            select_cat_columns_text=','.join(select_cat_columns)
            selected_agg_function=random.sample(agg_functions,1)[0]
            selected_agg_column=random.sample(measures,1)[0] 
            random_op_list=['>','<','>=','<=','=','!=']
            random_op=random.sample(random_op_list,1)[0]
            random_number=random.randint(1,1000)
            having_sample_1=f'Select  {select_cat_columns_text},{selected_agg_function}({selected_agg_column}) as {selected_agg_column}_{selected_agg_function} from {user_dataset} group by {select_cat_columns_text} having {selected_agg_column}_{selected_agg_function} {random_op} {random_number}'
            having_samples.append(having_sample_1)
            having_sample_2=f'Select  {select_cat_columns_text},count(*) as cnt from {user_dataset} group by {select_cat_columns_text} having Cnt {random_op} {random_number}'
            having_samples.append(having_sample_2)
        return having_samples
            
    


# def detect_base_pattern(nl_query):
#     """Detects the pattern of a natural language query."""
#     patterns = {
 
#         "total_group_by": r"(?:total|sum of|sum|sum of all|total of all) (.+?)(?:grouped by|group by|by|for each|for every|of each|per) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)", # e.g., "total sales by category"
#         "average_group_by": r"(?:average of|avg of|mean of|average|avg|mean) (.+?)(?:grouped by|group by|by|for each|for every|of each|per) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",        # e.g., "average price by region"
#         "min_group_by": r"(?:min of|minimum of|min|minimum|lowest|smallest) (.+?)(?:grouped by|group by|by|for each|for every|of each|per) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",  
#         "max_group_by": r"(?:max of|maximum of|max|maximum|largest|biggest) (.+?)(?:grouped by|group by|by|for each|for every|of each|per) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
#         "count_group_by": r"(?:count|number) (.+?)(?:grouped by|group by|by|for each|for every|of each|per) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)" ,
#         "Top_n_by":r"(?:top|first) (.+?)(?:ordered by|based on|by) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
#         "Bottom_n_by":r"(?:bottom|last) (.+?) (?:ordered by|based on|by) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
#         "total": r"(?:total|sum of|sum|sum of all|total of all) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
#         "average": r"(?:average of|avg of|mean of|average|avg|mean) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
#         "min" :r"(?:min of|minimum of|min|minimum|lowest|smallest) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
#         "max" :r"(?:max of|maximum of|max|maximum|largest|biggest) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
#         "count" : r"(?:count|number) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
#         "Select": r"(?:Select|List|Give|Show|Find|Provide) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)"        
#     }

#     recogonized_patterns={}
#     for pattern_name, pattern_regex in patterns.items():
#         match = re.search(pattern_regex, nl_query, re.IGNORECASE)
#         if match:
#             recogonized_patterns['pattern_name']=pattern_name
#             recogonized_patterns['groups']=match.groups()
#             return pattern_name, match.groups()
#     return None, None

def detect_base_pattern(nl_query):
    """Detects the pattern of a natural language query."""
    patterns = {
 
        "total_group_by": r"(?:total|sum of|sum|sum of all|total of all) (.+?)(?:grouped by|group by|by|for each|for every|of each|per) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)", # e.g., "total sales by category"
        "average_group_by": r"(?:average of|avg of|mean of|average|avg|mean) (.+?)(?:grouped by|group by|by|for each|for every|of each|per) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",        # e.g., "average price by region"
        "min_group_by": r"(?:min of|minimum of|min|minimum|lowest|smallest) (.+?)(?:grouped by|group by|by|for each|for every|of each|per) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",  
        "max_group_by": r"(?:max of|maximum of|max|maximum|largest|biggest) (.+?)(?:grouped by|group by|by|for each|for every|of each|per) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
        "count_group_by": r"(?:count|number) (.+?)(?:grouped by|group by|by|for each|for every|of each|per) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)" ,
        "Top_n_by":r"(?:Select|List|Give|Show|Find|Provide).*?(?:top|first) (.+?)(?:ordered by|based on|by) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
        "Bottom_n_by":r"(?:Select|List|Give|Show|Find|Provide).*?(?:bottom|last) (.+?) (?:ordered by|based on|by) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
        "total": r"(?:Select|List|Give|Show|Find|Provide).*?(?:total|sum of|sum|sum of all|total of all) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
        "average": r"(?:Select|List|Give|Show|Find|Provide).*?(?:average of|avg of|mean of|average|avg|mean) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
        "min" :r"(?:Select|List|Give|Show|Find|Provide).*?(?:min of|minimum of|min|minimum|lowest|smallest) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
        "max" :r"(?:Select|List|Give|Show|Find|Provide).*?(?:max of|maximum of|max|maximum|largest|biggest) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
        "count" : r"(?:Select|List|Give|Show|Find|Provide).*?(?:count|number) (.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)",
        "Select": r"(?:Select|List|Give|Show|Find|Provide)(.+?)(?:$|where|when|with|whose|limit|limited|limit to|sort|sorted|when|whose|ordered|order|arranged)"        
    }

    recogonized_patterns={}
    for pattern_name, pattern_regex in patterns.items():
        match = re.search(pattern_regex, nl_query, re.IGNORECASE)
        if match:
            recogonized_patterns['pattern_name']=pattern_name
            recogonized_patterns['groups']=match.groups()
            return pattern_name, match.groups()
    return None, None

def generate_base_sql(nl_query,attributes,measures,column_names):
    """Translates a natural language query to an SQL query based on recognized patterns."""
    pattern, elements = detect_base_pattern(nl_query)

    if pattern=='total_group_by':
        measure_found,attr_found=0,0
        
        selected_measures=[]
        for measure in measures:
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(f'Sum({measure}) as sum_{measure}')
                measure_found=1
        selected_measures_text=','.join(selected_measures)

        group_by_column=[]
        for attr in attributes:
            if attr in elements[1].lower() or attr.replace('_',' ') in elements[1].lower():
                group_by_column.append(attr)
                attr_found=1
        group_by_text=','.join(group_by_column)

        if measure_found==1 and attr_found==1:
            sql_query=f'Select {group_by_text},{selected_measures_text} from user_dataset group by {group_by_text}'

            return pattern,sql_query
        else:
            print('Please check your dimension and measure names')


    if pattern=='average_group_by':

        measure_found,attr_found=0,0
        
        selected_measures=[]
        for measure in measures:
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(f'Avg({measure}) as avg_{measure}')
                measure_found=1
        selected_measures_text=','.join(selected_measures)

        group_by_column=[]
        for attr in attributes:
            if attr in elements[1].lower() or attr.replace('_',' ') in elements[1].lower():
                group_by_column.append(attr)
                attr_found=1
        group_by_text=','.join(group_by_column)

        if measure_found==1 and attr_found==1:
            sql_query=f'Select {group_by_text},{selected_measures_text} from user_dataset group by {group_by_text}'
            return pattern,sql_query
        else:
            print('Please check your dimension and measure names')


    if pattern=='min_group_by':

        measure_found,attr_found=0,0
        
        selected_measures=[]
        for measure in measures:
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(f'Min({measure}) as min_{measure}')
                measure_found=1
        selected_measures_text=','.join(selected_measures)

        group_by_column=[]
        for attr in attributes:
            if attr in elements[1].lower() or attr.replace('_',' ') in elements[1].lower():
                group_by_column.append(attr)
                attr_found=1
        group_by_text=','.join(group_by_column)

        if measure_found==1 and attr_found==1:
            sql_query=f'Select {group_by_text},{selected_measures_text} from user_dataset group by {group_by_text}'
            return pattern,sql_query
        else:
            print('Please check your dimension and measure names')


    if pattern=='max_group_by':

        measure_found,attr_found=0,0
        
        selected_measures=[]
        for measure in measures:
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(f'Max({measure}) as max_{measure}')
                measure_found=1
        selected_measures_text=','.join(selected_measures)

        group_by_column=[]
        for attr in attributes:
            if attr in elements[1].lower() or attr.replace('_',' ') in elements[1].lower():
                group_by_column.append(attr)
                attr_found=1
        group_by_text=','.join(group_by_column)

        if measure_found==1 and attr_found==1:
            sql_query=f'Select {group_by_text},{selected_measures_text} from user_dataset group by {group_by_text}'
            return pattern,sql_query
        else:
            print('Please check your dimension and measure names')

    if pattern=='count_group_by':

        measure_found,attr_found=0,0
        
        selected_measures=[]
        for measure in column_names: # count can be of both categorical and continious variables 
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(f'Count({measure}) as cnt_{measure}')
                measure_found=1
        if measure_found==0:
            selected_measures_text='*'
            measure_found=1
        else:
            selected_measures_text=','.join(selected_measures)

        group_by_column=[]
        for attr in attributes:
            if attr in elements[1].lower() or attr.replace('_',' ') in elements[1].lower():
                group_by_column.append(attr)
                attr_found=1
        group_by_text=','.join(group_by_column)

        if measure_found==1 and attr_found==1:
            sql_query=f'Select {group_by_text},{selected_measures_text} from user_dataset group by {group_by_text}'
            return pattern,sql_query
        else:
            print('Please check your dimension and measure names')


    if pattern=='total':
        measure_found=0

        selected_measures=[]
        for measure in measures:
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(f'Sum({measure}) as sum_{measure}')
                measure_found=1
        selected_measures_text=','.join(selected_measures)

        if measure_found==1:
            sql_query=f'Select {selected_measures_text} from user_dataset'
            return pattern,sql_query
        else:
            print('Please check your dimension and measure names')

    if pattern=='average':
        measure_found=0

        selected_measures=[]
        for measure in measures:
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(f'Avg({measure}) as avg_{measure}')
                measure_found=1
        selected_measures_text=','.join(selected_measures)

        if measure_found==1:
            sql_query=f'Select {selected_measures_text} from user_dataset'
            return pattern,sql_query
        else:
            print('Please check your dimension and measure names')

    
    if pattern=='min':
        measure_found=0

        selected_measures=[]
        for measure in measures:
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(f'Min({measure}) as min_{measure}')
                measure_found=1
        selected_measures_text=','.join(selected_measures)

        if measure_found==1:
            sql_query=f'Select {selected_measures_text} from user_dataset'
            return pattern,sql_query
        else:
            print('Please check your dimension and measure names')

    
    if pattern=='max':
        measure_found=0

        selected_measures=[]
        for measure in measures:
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(f'Max({measure}) as max_{measure}')
                measure_found=1
        selected_measures_text=','.join(selected_measures)

        if measure_found==1:
            sql_query=f'Select {selected_measures_text} from user_dataset'
            return pattern,sql_query
        else:
            print('Please check your dimension and measure names')


    if pattern=='count':

        measure_found=0
        selected_measures=[]
        for measure in column_names: # count can be of both categorical and continious variables 
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(f'Count({measure}) as cnt_{measure}')
                measure_found=1
        if measure_found==0:
            selected_measures_text='*'
            measure_found=1
        else:
            selected_measures_text=','.join(selected_measures)
        sql_query=f'Select {selected_measures_text} from user_dataset'
        return pattern,sql_query
    
    if pattern=='Select':

        columns_found=0
        selected_columns=[]
        for col in column_names:
            if col in elements[0].lower() or col.replace('_',' ') in elements[0].lower():
                selected_columns.append(col)
                columns_found=1
        if columns_found==1:
            selected_columns_text=','.join(selected_columns)
        else:
            selected_columns_text=' * '
        sql_query=f'Select {selected_columns_text} from user_dataset'
        return pattern,sql_query

    if pattern=='Top_n_by':

        columns_found=0
        numbers = re.findall(r'\d+', elements[0])
        selected_columns=[]
        order_by_column=[]
        order_found=0
        for col in column_names:
            if col in elements[0].lower() or col.replace('_',' ') in elements[0].lower():
                selected_columns.append(col)
                columns_found=1
            if col in elements[1].lower() or col.replace('_',' ') in elements[1].lower():
                order_by_column.append(col)
                order_found=1
                order_by_text=','.join(order_by_column)
        if columns_found==0:
            selected_columns_text='*'
        else:
            selected_columns_text=','.join(selected_columns)
        if order_found==1:
            sql_query=f'Select {selected_columns_text},{order_by_text} from user_dataset order by {order_by_text} desc limit {numbers[0]}'
            return pattern,sql_query

    if pattern=='Bottom_n_by':

        columns_found=0
        numbers = re.findall(r'\d+', elements[0])
        selected_columns=[]
        order_by_column=[]
        order_found=0
        for col in column_names:
            if col in elements[0].lower() or col.replace('_',' ') in elements[0].lower():
                selected_columns.append(col)
                columns_found=1
            if col in elements[1].lower() or col.replace('_',' ') in elements[1].lower():
                order_by_column.append(col)
                order_found=1
                order_by_text=','.join(order_by_column)
        if columns_found==0:
            selected_columns_text='*'
        else:
            selected_columns_text=','.join(selected_columns)
        if order_found==1:
            sql_query=f'Select {selected_columns_text},{order_by_text} from user_dataset order by {order_by_text} aesc limit {numbers[0]}'
            return pattern,sql_query

def detect_where_pattern(nl_query):
    """Detects the where condition in the query and formats it accordingly"""
    where_pattern=r"(?:where|when|whose|with|having) (.+?)(?:$|limit|limited|limit to|sort|sorted|arranged|ordered|order)"
    match = re.search(where_pattern, nl_query, re.IGNORECASE)
    if match:
        return match.groups()


def wrap_non_numbers_in_quotes(expression):
    # Regular expression to match comparison operator and the right side value
    # This will match the operator followed by an optional space and then any non-numeric value
    return re.sub(r'([=\!<>]=?)\s*([^\d\s]+)', r'\1 \'\2\'', expression)

def enclose_dates_in_quotes(s):
    # Regular expression to detect dates in the format YYYY-MM-DD
    date_pattern = r'\b(\d{4})-(\d{2})-(\d{2})\b'
    
    # Function to enclose detected dates in single quotes
    return re.sub(date_pattern, r"'\g<0>'", s)


def concat_between(lst):
    i = 0
    while i < len(lst):
        if 'between' in lst[i].lower() and i + 2 < len(lst):
            # Concatenate current element with the next two elements
            lst[i] += ' ' + lst.pop(i + 1) + ' ' + lst.pop(i + 1)
            # Remove everything before 'between' but keep 'between'
            index = lst[i].lower().index('between')
            lst[i] = lst[i][index:]
            i += 1
            # Skip further concatenations for this block
        else:
            i += 1  # Move to the next element
    return lst


def generate_where_part (nl_query,column_names):
    detected_where_part=detect_where_pattern(nl_query)
    detected_where_text=''
    if detected_where_part:
        detected_where_text=detected_where_part[0].lower()
    text=' '+detected_where_text
    words=copy.deepcopy(column_names)
    words=[' '+ word  for word in words]
    words.append(' and')
    words.append(' but')
    pattern = '|'.join(f'({re.escape(word)})' for word in words)
    split_text = re.split(pattern, text)
    split_text=[part.strip() for part in split_text if part]
    concat_between(split_text)

    current_condn_col=None
    where_conditions=[]
    having_conditions=[]
    operator_patterns={ 

        '<=' : r"\b(?:is\s+)?(?:less|lesser|below|lower|smaller)\s+than\s+or\s+equal\s+to\b|\b(?:is\s+)?at\s+most\b",
        '>=' : r"\b(?:is\s+)?(?:greater|more|above|higher|larger|bigger)\s+than\s+or\s+equal\s+to\b|\b(?:is\s+)?at\s+least\b",
        '<' : r"\b(?:is\s+)?(?:less|lesser|below|lower|smaller)\s+than\b",
        '>' : r"\b(?:is\s+)?(?:greater|more|above|higher|larger|bigger)\s+than\b",
        '!=' : r"\b(?:is\s+)?(?:not\s+equal\s+to|not\s+equals|different\s+from|not\s+the\s+same\s+as)\b",
        '=': r"\b(?:is\s+)?(?:equal\s+to|equals|same\s+as|exactly)\b|\bis\s+\b"

    }
    
    aggregate_fc_words=['average','avg','mean','sum','total','min','minimum','max','maximum','lowest','smallest','biggest','largest']
    agg_fn_map={ 'Avg' : ['average','avg','mean'],
                'Sum' : ['sum','total'],
                'Min' : ['min','minimum','lowest','smallest'],
                'Max' : ['max','maximum','biggest','largest']
    }
    condition_on_agg=0
    agg_fn=None
 
    for i,part in enumerate(split_text):
        if part.lower()=='and' or part.lower()=='' or part.lower()=='but':
            continue

        agg_match=next((word for word in aggregate_fc_words if re.search(rf'\b{word}\b', part, re.IGNORECASE)), None)
        if agg_match:
            condition_on_agg=1
            agg_word=agg_match
            for func_name,func_word_list in agg_fn_map.items():
                if  agg_word in func_word_list:
                    agg_fn=func_name
            continue

        if part.lower() in column_names:
            if condition_on_agg==0:
                current_condn_col=part.lower()
            else:
                current_condn_col=f'{agg_fn}({part.lower()})'
                condition_on_agg=0
            continue
        
        if current_condn_col!=None and current_condn_col!=part.lower():
            if 'between' not in part.lower():
                condition_text=part
                for operator, pattern in operator_patterns.items():
                    condition_text = re.sub(pattern, operator, condition_text, flags=re.IGNORECASE)
                condition_text=current_condn_col+' '+condition_text
                condition_text=wrap_non_numbers_in_quotes(condition_text).replace('\\','')
                condition_text=enclose_dates_in_quotes(condition_text)
            else:
                condition_text=part
                condition_text=current_condn_col+' '+condition_text   
                condition_text=enclose_dates_in_quotes(condition_text)
            if current_condn_col in column_names:
                where_conditions.append(condition_text)
            else:
                having_conditions.append(condition_text)

    return where_conditions,having_conditions


def detect_limit_sort_order_pattern(nl_query):
    """Detects the where condition in the query and formats it accordingly"""
    pattern_list={}
    pattern_list['order_by_pattern']=r"(?:order by|ordered by|arranged|arrange|sorted|sort) (.+?)(?:$|limit|limited|limit to|skip|offset)"
    pattern_list['limit_to_pattern']=r"(?:limit to|limited to|limit) (.+?)(?:$|order by|ordered by|arranged|arrange|sorted|sort|skip|offset)"
    pattern_list['offset_by_pattern']=r"(?:skip|offset) (.+?)(?:$|order by|ordered by|arranged|arrange|sorted|sort|skip|offset|limit to|limited to|limit)"
    result_dict={}
    for pattern_name,pattern in pattern_list.items():
        match = re.search(pattern, nl_query, re.IGNORECASE)
        if match:
            result_dict[pattern_name]=match.groups()
    return result_dict


def generate_limit_sort_order_parts(nl_query,column_names):

    detected_parts=detect_limit_sort_order_pattern(nl_query)
    limit_part=''
    order_part=''
    offset_part=''

    for query_part,text in detected_parts.items():
        if query_part=='order_by_pattern':
            order_by_columns=[]
            for col in column_names:
                if col in text[0].lower() or col.replace('_',' ') in text[0].lower():
                    order_by_columns.append(col)
            if 'descending' in text[0].lower():
                order_by_type='DESC'
            else:
                order_by_type=''

            order_part= ' order by '+','.join(order_by_columns) +' '+order_by_type


        if query_part=='limit_to_pattern':
            numbers = re.findall(r'\d+', text[0])
            limit_part=' limit '+ numbers[0]

        if query_part=='offset_by_pattern':
            numbers = re.findall(r'\d+', text[0])
            offset_part=' offset '+ numbers[0]
    
    return order_part+limit_part+offset_part
     

def generate_join_part(input_user_query,input_dataset_paths,all_columns,column_details):
    table_already_exists_in_join={}
    Columns_in_query=[]
    for col in all_columns:
        if col in input_user_query.lower():
            Columns_in_query.append(col)

    cols_selected={}
    cols_gone_through=[]
    for path in input_dataset_paths:
        table_already_exists_in_join[path]=0
        cols_selected[path]=0
        for col in Columns_in_query:
            if col in column_details[path]['column_names'] and col not in cols_gone_through:
                cols_selected[path]=1
                cols_gone_through.append(col)

    tables_in_selection = [key for key, value in cols_selected.items() if value == 1]

    join_conditions=[]
    if len(tables_in_selection)>1:
        from_exists=0
        from_table=''
        for table1 in tables_in_selection:
            for table2 in tables_in_selection:
                if table1 != table2:
                    join_columns = list(set(column_details[table1]['column_names']) & set(column_details[table2]['column_names']))
                    if join_columns: # code works only when one column is common between tables
                        col=join_columns[0]
                        if table_already_exists_in_join[table1]==0 and table_already_exists_in_join[table2]==0 and from_exists==0:
                            join_conditions.append(f' from {table1.replace('.csv','')} join {table2.replace('.csv','')} on {table1.replace('.csv','')}.{col} = {table2.replace('.csv','')}.{col}')
                            table_already_exists_in_join[table1]=1
                            table_already_exists_in_join[table2]=1
                            from_exists=1
                            from_table=table1
                        if from_exists==1 and from_table==table1 and table_already_exists_in_join[table2]==0:
                            join_conditions.append(f' join {table2.replace('.csv','')} on {table1.replace('.csv','')}.{col} = {table2.replace('.csv','')}.{col}')
                            table_already_exists_in_join[table2]=1
                        if from_exists==1 and from_table==table2 and table_already_exists_in_join[table1]==0:
                            join_conditions.append(f' join {table1.replace('.csv','')} on {table1.replace('.csv','')}.{col} = {table2.replace('.csv','')}.{col}')
                            table_already_exists_in_join[table1]=1
    else:
        join_conditions.append(f' from {tables_in_selection[0].replace('.csv','')}')

    return join_conditions

# def generate_join_part(input_user_query,input_dataset_paths,all_columns,column_details):
#     table_already_exists_in_join={}

#     Columns_in_query=[]
#     for col in all_columns:
#         if col in input_user_query.lower():
#             Columns_in_query.append(col)

#     cols_selected={}
#     for path in input_dataset_paths:
#         table_already_exists_in_join[path]=0
#         cols_selected[path]=0
#         for col in Columns_in_query:
#             if col in column_details[path]['column_names']:
#                 cols_selected[path]=1

#     tables_in_selection = [key for key, value in cols_selected.items() if value == 1]

#     join_conditions=[]
#     if len(tables_in_selection)>1:
#         from_exists=0
#         from_table=''
#         for table1 in tables_in_selection:
#             for table2 in tables_in_selection:
#                 if table1 != table2:
#                     join_columns = list(set(column_details[table1]['column_names']) & set(column_details[table2]['column_names']))
#                     if join_columns: # code works only when one column is common between tables
#                         col=join_columns[0]
#                         if table_already_exists_in_join[table1]==0 and table_already_exists_in_join[table2]==0 and from_exists==0:
#                             join_conditions.append(f' from {table1.replace('.csv','')} join {table2.replace('.csv','')} on {table1.replace('.csv','')}.{col} = {table2.replace('.csv','')}.{col}')
#                             table_already_exists_in_join[table1]=1
#                             table_already_exists_in_join[table2]=1
#                             from_exists=1
#                             from_table=table1
#                         if from_exists==1 and from_table==table1 and table_already_exists_in_join[table2]==0:
#                             join_conditions.append(f' join {table2.replace('.csv','')} on {table1.replace('.csv','')}.{col} = {table2.replace('.csv','')}.{col}')
#                             table_already_exists_in_join[table2]=1
#                         if from_exists==1 and from_table==table2 and table_already_exists_in_join[table1]==0:
#                             join_conditions.append(f' join {table1.replace('.csv','')} on {table1.replace('.csv','')}.{col} = {table2.replace('.csv','')}.{col}')
#                             table_already_exists_in_join[table1]=1
#     else:
#         join_conditions.append(f' from {tables_in_selection[0]}')

#     return join_conditions

    

def output_sample_queries(input_user_query,input_dataset_paths,all_columns,column_details):

    sample_query_types=['order by','min','max','avg','sum','where','having','limit','offset','group by']
    specific_sample_found=0

    # samples are generated only for tables with atleast 1 measure and 1 attribute for all cases other than join
    sample_tables=[]
    for table in column_details:
        if column_details[table]['attributes'] and column_details[table]['measures']:
            sample_tables.append(table)


    if 'example' in input_user_query.lower() or 'sample' in input_user_query.lower():
        if 'join' in input_user_query.lower():
            specific_sample_found=1
            i=0
            output_sample=[]
            while i<5:
                random_no_of_columns=random.randint(1,int(len(all_columns)/2)+1)
                random_columns=random.sample(all_columns,random_no_of_columns)
                random_columns_text=','.join(random_columns)
                sql_query_base_join=f' Select {random_columns_text}'
                join_conditions=generate_join_part(sql_query_base_join,input_dataset_paths,all_columns,column_details)
                if join_conditions:
                    i+=1
                    join_text=' '.join(join_conditions)
                    output_sample.append(sql_query_base_join+' '+join_text)
        else:
            for sample_type in sample_query_types:
                if sample_type in input_user_query.lower():
                    samples_list=[]
                    for path in sample_tables:
                        table_name=path.replace('.csv','')
                        samples_list.extend(create_sample_query(sample_type,column_details[path]['column_names'],column_details[path]['attributes'],column_details[path]['measures'],column_details[path]['unique_elements'],table_name))
                        # print description on what sample type
                    output_sample=samples_list #random.sample(samples_list,5) #output_sample
                    specific_sample_found=1
                    break

        if specific_sample_found==0:
            all_samples=[]
            for sample_type in sample_query_types:
                for path in sample_tables:
                    table_name=path.replace('.csv','')
                    generic_samples_list=create_sample_query(sample_type,column_details[path]['column_names'],column_details[path]['attributes'],column_details[path]['measures'],column_details[path]['unique_elements'],table_name)
                    all_samples.extend(generic_samples_list)
            output_sample=all_samples #random.sample(all_samples,5)
    return output_sample

def translate_to_sql(input_user_query,input_dataset_paths,attributes,measures,column_names,column_details):
        final_query=''
        base_query_output=generate_base_sql(input_user_query,attributes,measures,column_names)
        base_query_format=base_query_output[0]
        base_query_part=base_query_output[1]
        where_having_output=generate_where_part(input_user_query,column_names)
        where_part=''
        having_part=''
        join_text=''
        if where_having_output[0]:
            where_part=' where ' + ' and '.join(where_having_output[0])
        if where_having_output[1]:
            having_part=' having ' +' and '.join(where_having_output[1])
        limit_sort_order_part=generate_limit_sort_order_parts(input_user_query,column_names)
        join_conditions=generate_join_part(input_user_query,input_dataset_paths,column_names,column_details)
        join_text=' '.join(join_conditions)
        base_query_part=base_query_part.replace('from user_dataset',join_text)
        if base_query_format not in ['Top_n_by','Bottom_n_by']:
            final_query=base_query_part
            if 'group by' in final_query:
                final_query=final_query.replace('group by',where_part+' '+'group by')
            else:
                final_query=final_query+' '+where_part
            final_query=final_query+' '+having_part+' '+limit_sort_order_part
        else:
            if 'order by' in base_query_format:
                final_query=base_query_part
                final_query=final_query.replace('order by',where_part+' '+'order by')
        return final_query


# mongo parts

def load_csv_to_mongo(csv_path, mongo_uri, db_name, collection_name):
    """
    Loads a CSV file into a MongoDB collection.

    Parameters:
    - csv_path (str): Path to the CSV file.
    - mongo_uri (str): MongoDB connection URI.
    - db_name (str): Name of the database.
    - collection_name (str): Name of the collection where data should be stored.

    Returns:
    - dict: Metadata about the columns and a sample of the data.
    """
    try:
        # Load CSV into a DataFrame
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.lower()
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Insert data into MongoDB
        collection.delete_many({})  # Clear existing data
        data = df.to_dict(orient='records')
        collection.insert_many(data)

        # Extract column metadata
        attributes = df.select_dtypes(include=['object']).columns.tolist()
        attributes.extend([col for col in df.columns if col.lower().endswith('id') and col not in attributes])
        measures = [col for col in df.columns if col not in attributes]
        unique_elements = {col: df[col].unique().tolist() for col in attributes}
        sample_data = df.head(5).to_dict(orient='records')
        

        return list(df.columns), attributes, measures, unique_elements, sample_data
    except Exception as e:
        print(f"Error loading CSV into MongoDB: {e}")
        return [], [], [], {}, []
    

def create_sample_mongo_query(query_type, attributes, measures, unique_elements, collection_name):
    """
    Creates sample MongoDB queries based on the query type.

    Parameters:
    - query_type: Type of query (e.g., group by, sum, where).
    - attributes: List of categorical attributes.
    - measures: List of continuous measures.
    - unique_elements: Unique elements for attributes.
    - collection_name: The target MongoDB collection.

    Returns:
    - list: List of MongoDB queries or pipelines.
    """
    sample_queries = []

    if query_type.lower()=='project':
        for i in range (0,10):
            select_attrs=random.randint(0,len(attributes))
            select_measures=random.randint(0,len(measures))
            select_attr_columns=random.sample(attributes,select_attrs)
            select_measure_columns=random.sample(measures,select_measures)
            pipeline = [{"$project":{}}]
            for attr in select_attr_columns:
                temp={f"{attr}_new":f"${attr}"}
                pipeline[0]['$project'].update(temp)
            for measure in select_measure_columns:
                temp={f"{measure}_new":f"${measure}"}
                pipeline[0]['$project'].update(temp)
            if select_attrs!=0 or select_measures!=0:
                sample_queries.append({"collection": collection_name , "pipeline":pipeline})

    elif query_type.lower()== 'match':
        for i in range (0,10):
            select_attrs=random.randint(0,len(attributes))
            select_attr_columns=random.sample(attributes,select_attrs)
            pipeline = [{"$match":{}}]
            for attr in select_attr_columns:
                values_list=unique_elements[attr]
                random_value=random.sample(values_list,1)[0]
                temp={f"{attr}":random_value}
                pipeline[0]['$match'].update(temp)
            if select_attrs!=0:
                sample_queries.append({"collection":collection_name, "pipeline":pipeline})


    elif query_type.lower() == 'group':
        for attr in attributes:
            for measure in measures:
                pipeline = [
                    {"$group": {
                        "_id": f"${attr}",
                        f"sum_{measure}": {"$sum": f"${measure}"}
                    }},
                    {"$sort": {"_id": 1}}
                ]
                sample_queries.append({"collection": collection_name, "pipeline": pipeline})


    elif query_type.lower() in  ['sum','avg','min','max']:
        agg_fn=query_type.lower()
        for measure in measures:
            for attr in attributes:
                pipeline = [
                {"$group": {
                    "_id": f"${attr}",
                    f"{agg_fn}_{measure}": {f"${agg_fn}": f"${measure}"}
                }}]
                sample_queries.append({"collection": collection_name, "pipeline": pipeline})
            pipeline = [
                {"$group": {
                    "_id": None,
                    f"{agg_fn}_{measure}": {f"${agg_fn}": f"${measure}"}
                }}
            ]
            sample_queries.append({"collection": collection_name, "pipeline": pipeline})


    elif query_type.lower() == 'where':
        for attr in attributes:
            for value in unique_elements[attr][:5]:  # Limit the number of unique values sampled
                query = {attr: value}
                sample_queries.append({"collection": collection_name, "query": query})

    elif query_type.lower() == 'sort':
        cols=attributes+measures
        for col in cols:
            for i in range(0,3):
                sort_types=[-1,1]
                sort_type=random.sample(sort_types,1)[0]
                random_number=random.randint(3,15)
                pipeline = [
                    {"$sort": {col: sort_type}},  # Ascending
                    {"$limit": random_number}
                ]
                sample_queries.append({"collection": collection_name, "pipeline": pipeline})
                pipeline = [
                    {"$sort": {col: sort_type}}]
                sample_queries.append({"collection": collection_name, "pipeline": pipeline})

    elif query_type.lower() == 'limit':
        cols=attributes+measures
        for i in range (0,10):
            sort_types=[-1,1]
            sort_type=random.sample(sort_types,1)[0]
            col=random.sample(cols,1)[0]
            limit_number=random.randint(3,25)
            pipeline = [
                {"$limit": limit_number}
            ]
            sample_queries.append({"collection": collection_name, "pipeline": pipeline})
            pipeline = [
                    {"$sort": {col: sort_type}},  # Ascending
                    {"$limit": limit_number}
                ]
            sample_queries.append({"collection": collection_name, "pipeline": pipeline})

    elif query_type.lower() == 'skip':

        for offset in range(1, 10):  # Generate sample offsets
            limit_number=random.randint(3,20)
            pipeline = [
                {"$skip": offset},
                {"$limit": limit_number}
            ]
            sample_queries.append({"collection": collection_name, "pipeline": pipeline})

    elif query_type.lower() == 'having':
        for attr in attributes:
            for measure in measures:
                pipeline = [
                    {"$group": {
                        "_id": f"${attr}",
                        f"sum_{measure}": {"$sum": f"${measure}"}
                    }},
                    {"$match": {
                        f"sum_{measure}": {"$gt": 100}  # Example condition
                    }}
                ]
                sample_queries.append({"collection": collection_name, "pipeline": pipeline})

    return sample_queries

def generate_base_mongo(nl_query,attributes,measures,column_names):
    pattern, elements = detect_base_pattern(nl_query)
    pipeline= [{"error": "corresponding base query pattern not found"}]
    if 'group_by' in pattern and 'count' not in pattern:


        agg_fn_map={ 'total' : 'sum',
                'average' : 'avg',
                'min' : 'min',
                'max' : 'max'}

        for func_word,func_name in agg_fn_map.items():
            if func_word in pattern:
                agg_fn=func_name
                break


        measure_found,attr_found=0,0
        pipeline=[{"$group":{}}]

        selected_measures=[]
        for measure in measures:
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(measure)
                measure_found=1

            

        group_by_column=[]
        for attr in attributes:
            if attr in elements[1].lower() or attr.replace('_',' ') in elements[1].lower():
                group_by_column.append(attr)
                attr_found=1

        if measure_found==1 and attr_found==1:
            if len(group_by_column)>1:
                temp={"_id":{}}
                for attr in group_by_column:
                    temp2={f"{attr}":f"${attr}"}
                    temp['_id'].update(temp2)
            else:
                temp={"_id":f"${group_by_column[0]}"}
            
            pipeline[0]['$group'].update(temp)

            for measure in selected_measures:
                temp={f"{agg_fn}_{measure}":{f"${agg_fn}":f"${measure}"}}
                pipeline[0]['$group'].update(temp)

    elif 'total' in pattern or 'average' in pattern or 'min' in pattern or 'max' in pattern:

        agg_fn_map={ 'total' : 'sum',
                'average' : 'avg',
                'min' : 'min',
                'max' : 'max'}

        for func_word,func_name in agg_fn_map.items():
            if func_word in pattern:
                agg_fn=func_name
                break

        pipeline=[{"$group":{"_id": "null"}}]
        
        measure_found=0
        selected_measures=[]
        for measure in measures:
            if measure in elements[0].lower() or measure.replace('_',' ') in elements[0].lower():
                selected_measures.append(measure)
                measure_found=1
        
        if measure_found==1:
            for measure in selected_measures:
                temp={f"{agg_fn}_{measure}":f"${measure}"}
                pipeline[0]['$group'].update(temp)

    elif 'count' in pattern:
        if 'group_by' in pattern:
            pipeline=[{"$group":{}}]
            group_by_column=[]
            for attr in attributes:
                if attr in elements[1].lower() or attr.replace('_',' ') in elements[1].lower():
                    group_by_column.append(attr)
                    attr_found=1
            if attr_found==1:
                if len(group_by_column)>1:
                    temp={"_id":{}}
                    for attr in group_by_column:
                        temp2={f"{attr}":f"${attr}"}
                        temp['_id'].update(temp2)
                else:
                    temp={"_id":f"${group_by_column[0]}"}
            
                pipeline[0]['$group'].update(temp)
            temp={"Count":{"$sum":1}}
            pipeline[0]['$group'].update(temp)
        
        else:
            pipeline=[{"$group":{"_id":"null","Count":{"$sum":1}}}]

    elif 'Select' in pattern:
        pipeline=[{"$project":{"_id":0}}]
        col_found=0
        selected_columns=[]
        for col in column_names:
            if col in elements[0].lower() or col.replace('_',' ') in elements[0].lower():
                selected_columns.append(col)
                col_found=1
        if col_found==1:
            for col in selected_columns:
                temp={f"{col}":1}
                pipeline[0]['$project'].update(temp)

    elif 'Top' in pattern or 'Bottom' in pattern:
        if 'Top' in pattern:
            order_number=-1
        else:
            order_number=1
        columns_found=0
        numbers = re.findall(r'\d+', elements[0])
        selected_columns=[]
        order_by_column=[]
        order_found=0
        pipeline=[]
        for col in column_names:
            if col in elements[0].lower() or col.replace('_',' ') in elements[0].lower():
                selected_columns.append(col)
                columns_found=1
            if col in elements[1].lower() or col.replace('_',' ') in elements[1].lower():
                order_by_column.append(col)
                order_found=1

        all_select_columns= selected_columns + order_by_column

        if columns_found==1:
            dummy_dict={"$project":{"_id":0}}
            pipeline.append(dummy_dict)
            for col in selected_columns:
                temp2={f"{col}":1}
                pipeline[0]['$project'].update(temp2)

        if order_found==1:
            sort_temp={"$sort":{}}
            for col in order_by_column:
                temp={f"col": order_number}
                sort_temp['$sort'].update(temp)
            pipeline.append(sort_temp)

        limit_temp={"$limit":f"{numbers[0]}"}
        pipeline.append(limit_temp) 
    return pipeline, pattern


def generate_match_mongo (nl_query,column_names):
    # using generate_where_part function used for SQL and working from that
    where_conditions,having_conditions = generate_where_part (nl_query,column_names)
    where_pipeline=[]
    if where_conditions:
        where_pipeline=[{"$match":{}}]
        for condn in [part.replace("'",'') for part in where_conditions]:
            condn_column='No_Col_Found'

            for col in column_names:
                if col in condn.lower() or col.replace('_',' ') in condn.lower():
                    condn_column=col

            if 'between' in condn:
                words=['and','between']
                pattern = '|'.join(f'({re.escape(word)})' for word in words)
                split_text = re.split(pattern, condn)
                split_text=[part.strip() for part in split_text if part]
                and_pos=split_text.index('and')

                greater_than_value=split_text[and_pos-1]
                if greater_than_value.isdigit():
                    greater_than_value=int(greater_than_value)
                less_than_value=split_text[and_pos+1]
                if less_than_value.isdigit():
                    less_than_value=int(less_than_value)

                temp = {f"{condn_column}":{"$gte":greater_than_value,"$lte":less_than_value}}

            else:
                pattern = r'(\s*(?:>=|<=|!=|=|>|<)\s*)'
                split_text = re.split(pattern, condn)
                split_text = [part.strip() for part in split_text if part]
                operators = {'>=', '<=', '=', '>', '<'}
                operator_positions = [i for i, item in enumerate(split_text) if item in operators]
                condn_operator=split_text[operator_positions[0]]
                condn_value=split_text[operator_positions[0]+1].replace("'",'')

                if condn_operator=='=':
                    temp={f"{condn_column}":condn_value}
                else:
                    operator_map={'>=':'$gte', '<=' : '$lte', '!=' :'$ne', '>' : '$gt' , '<' :'$lt' }
                    mongo_operator=operator_map[condn_operator]
                    temp={f"{condn_column}":{f'{mongo_operator}':condn_value}}
            if condn_column in where_pipeline[0]['$match']:
                where_pipeline[0]['$match'][condn_column].update(temp[condn_column])
            else:
                where_pipeline[0]['$match'].update(temp)

    having_pipeline=[]
    if having_conditions:
        having_pipeline=[{"$match":{}}]
        for condn in [part.replace("'",'') for part in having_conditions]:
            condn_column='No_Col_Found'

            for col in column_names:
                if col in condn.lower() or col.replace('_',' ') in condn.lower():
                    condn_column=col

            agg_functions=['avg','min','max','sum']
            agg_func='No Agg Function'

            for func in agg_functions:
                if func in condn.lower() or func.replace('_',' ') in condn.lower():
                    agg_func=func

            condn_col=agg_func+'_'+condn_column

            if 'between' in condn:
                words=['and','between']
                pattern = '|'.join(f'({re.escape(word)})' for word in words)
                split_text = re.split(pattern, condn)
                split_text=[part.strip() for part in split_text if part]
                and_pos=split_text.index('and')

                greater_than_value=split_text[and_pos-1]
                if greater_than_value.isdigit():
                    greater_than_value=int(greater_than_value)
                less_than_value=split_text[and_pos+1]
                if less_than_value.isdigit():
                    less_than_value=int(less_than_value)

                temp = {f"{condn_col}":{"$gte":greater_than_value,"$lte":less_than_value}}

            else:
                pattern = r'(\s*(?:>=|<=|!=|=|>|<)\s*)'
                split_text = re.split(pattern, condn)
                split_text = [part.strip() for part in split_text if part]
                operators = {'>=', '<=', '=', '>', '<'}
                operator_positions = [i for i, item in enumerate(split_text) if item in operators]
                condn_operator=split_text[operator_positions[0]]
                condn_value=split_text[operator_positions[0]+1].replace("'",'')
                if condn_value.isdigit():
                    condn_value=int(condn_value)

                if condn_operator=='=':
                    temp={f"{condn_col}":condn_value}
                else:
                    operator_map={'>=':'$gte', '<=' : '$lte', '!=' :'$ne', '>' : '$gt' , '<' :'$lt' }
                    mongo_operator=operator_map[condn_operator]
                    temp={f"{condn_col}":{f'{mongo_operator}':condn_value}}
            if condn_col in having_pipeline[0]['$match']:
                having_pipeline[0]['$match'][condn_col].update(temp[condn_col])
            else:
                having_pipeline[0]['$match'].update(temp)

    return where_pipeline,having_pipeline

#create_sample_mongo_query(query_type, attributes, measures, unique_elements, collection_name)
def output_sample_queries_mongo(input_user_query,input_dataset_paths,all_columns,column_details):

    sample_query_types=['sort','min','max','avg','sum','where','having','limit','skip','group','project','match']
    specific_sample_found=0
    pymongo_samples=[]
    mongo_query_samples=[]

    # samples are generated only for tables with atleast 1 measure and 1 attribute to prevent errors
    sample_tables=[]
    for collection in column_details:
        if column_details[collection]['attributes'] and column_details[collection]['measures']:
            sample_tables.append(collection)
    
    if 'example' in input_user_query.lower() or 'sample' in input_user_query.lower():
        if 'find' in input_user_query.lower():
            sample_type='match'
            samples_list=[]
            for path in sample_tables:
                collection_name=path.replace('.csv','')
                samples_list.extend(create_sample_mongo_query(sample_type, column_details[path]['attributes'], column_details[path]['measures'], column_details[path]['unique_elements'], collection_name))
            pymongo_samples=samples_list
            specific_sample_found=1
            for sample in pymongo_samples:
                mongo_query=f'db.{collection_name}.find({sample['pipeline'][0]['$match']})'
                mongo_query_samples.append(mongo_query)
        for sample_type in sample_query_types:
            if sample_type in input_user_query.lower():
                samples_list=[]
                for path in sample_tables:
                    collection_name=path.replace('.csv','')
                    samples_list.extend(create_sample_mongo_query(sample_type, column_details[path]['attributes'], column_details[path]['measures'], column_details[path]['unique_elements'], collection_name))
                    # print description on what sample type
                pymongo_samples=samples_list #random.sample(samples_list,5) #output_sample
                specific_sample_found=1
                for query in pymongo_samples:
                    mongo_query=f"db.{query['collection']}.aggregate({query['pipeline']})"
                    mongo_query_samples.append(mongo_query)
                break
        if specific_sample_found==0:
            all_samples=[]
            for sample_type in sample_query_types:
                for path in sample_tables:
                    collection_name=path.replace('.csv','')
                    generic_samples_list=create_sample_mongo_query(sample_type, column_details[path]['attributes'], column_details[path]['measures'], column_details[path]['unique_elements'], collection_name)
                    all_samples.extend(generic_samples_list)
            pymongo_samples=all_samples
            for query in pymongo_samples:
                    mongo_query=f"db.{query['collection']}.aggregate({query['pipeline']})"
                    mongo_query_samples.append(mongo_query)


    return mongo_query_samples

def generate_limit_sort_skip_mongo(nl_query,column_names):
    detected_parts=detect_limit_sort_order_pattern(nl_query)
    limit_pipeline=[]
    sort_pipeline=[]
    skip_pipeline=[]
    order_type=1
    

    for query_part,text in detected_parts.items():

        if query_part=='order_by_pattern':
            
            order_by_columns=[]
            for col in column_names:
                if col in text[0].lower() or col.replace('_',' ') in text[0].lower():
                    order_by_columns.append(col)
        
            if 'descending' in text[0].lower():
                order_type=-1
            
            sort_pipeline=[{"$sort":{}}]

            for col in order_by_columns:
                temp={f'{col}':order_type}
                sort_pipeline[0]['$sort'].update(temp)

        elif query_part=='limit_to_pattern':
            numbers = re.findall(r'\d+', text[0])
            limit_pipeline=[{'$limit':int(numbers[0])}]
        
        elif query_part=='offset_by_pattern':
            numbers = re.findall(r'\d+', text[0])
            skip_pipeline=[{'$skip':int(numbers[0])}]

    return sort_pipeline,skip_pipeline,limit_pipeline
    
                

def translate_to_mongo(input_user_query,input_dataset_paths,attributes,measures,column_names,column_details):
    final_pipeline=[]
    base_pipeline,base_pattern=generate_base_mongo(input_user_query,attributes,measures,column_names)
    where_pipeline,having_pipeline=generate_match_mongo (input_user_query,column_names)
    sort_pipeline,skip_pipeline,limit_pipeline= generate_limit_sort_skip_mongo (input_user_query,column_names)
    if where_pipeline:
        final_pipeline.append(where_pipeline[0])
    if base_pipeline:
        final_pipeline.extend(base_pipeline)
    if 'Top' not in base_pattern and 'Bottom' not in base_pattern:
        if having_pipeline:
            final_pipeline.append(having_pipeline[0])
        if sort_pipeline:
            final_pipeline.append(sort_pipeline[0])
        if skip_pipeline:
            final_pipeline.append(skip_pipeline[0])
        if limit_pipeline:
            final_pipeline.append(limit_pipeline[0])

    columns_in_query=[]
    for col in column_names:
        if col in input_user_query.lower() or col.replace('_',' ') in input_user_query.lower():
            columns_in_query.append(col)

    collection_name='No collection Found'
    for path in input_dataset_paths:
        columns=column_details[path]['column_names']
        columns_counter=0
        for col in columns:
            
            if col in input_user_query.lower() or col.replace('_',' ') in input_user_query.lower():
                columns_counter+=1
        if columns_counter==len(columns_in_query):
                collection_name=path.replace('.csv','')
                
            
    mongo_query=f"db.{collection_name}.aggregate({final_pipeline})"
    return mongo_query , collection_name, final_pipeline

    