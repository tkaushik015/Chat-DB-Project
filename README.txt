************ README ****************************************

Launching the App:

1. Please place all csv files to be loaded in the root directory of the project

2. To launch the app > Open Command Prompt and change directory to the root of the project and run 

	python app.py

3. The app would start running @ "http://127.0.0.1:5000/" . Please enter the url in your browser to open the app.


App Structure: 

app.py -> Main flask backend application to be run

utils.py -> utility code with all the helper functions

template / index.html -> template for the html front end


App Usage instructions: 

1) Dataset loading & DB type selections 

	a) Select DB type using the radio buttons between SQL / No SQL
        b) Enter input dataset paths separated by comma. Ex: sales.csv,products.csv,customers.csv 

and hit load dataset

Dataset would be pushed to the DB of choice and sample data would be displayed

2) Sample query generation

Under the text box labelled 'Enter Natural Language query' type the following

 a) Give me sample query with group by 
				or
 b) Give me sample queries

3) Natural language to SQL / No SQL translation

Just type the natural language in the text box. The query would be translated and executed and the results displayed




