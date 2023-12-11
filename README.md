# Multinational retail data centralization
##  Description
The main goal of the project, as the name suggests, was to centralize sales data from multiple locations into one database.
It was meant to challenge the ability to use data handling, data cleaning and database management and show an understanding of 
the subject in practice. Since the retail data was being hosted in multiple locations (AWS RDS & S3, API) and in multiple formats (csv, json, pdf), 
this required different approaches and the use of different python dependencies.
Overall it has been an interesting experience, which makes one realize what issues might arise when handling data and what knowledge gaps are still to be filled
in that respect. The most significant benefit of completing the project was a direct exposure to the challenges of data cleaning, which also took the most time to complete.
It is critical to get this part right, and so the amount of thought investment has reflected it. I have re-written the code on many occasions in order to optimise and fix issues that 
have been missed initially. The inclusion of docstrings and comments (perhaps a few too many in a couple of methods) has made it easy to track the parts of code that still needed work.


### Milestone 1
The first milestone involved setting up the git hub repository and has been a straightforward task.

### Milestone 2
This was a major part of the project aiming at the implementation of the core classes and methods.

To outline the main functionality of the program:
1. Download data from source.
2. Consolidate and clean data.
3. Upload to local database.

The code has been segregated into three python scripts:
- database_utils.py
Contains the class DatabaseConnector with all methods related to interacting with databases.
These include: reading credentials, creating an engine (use of sqlalchemy), listing all tables in a database and uploading data.
Note that files containing database credentials have been included in the .gitignore file for security reasons.

- data_cleaning.py
Contains the class DataCleaning witl all methods that clean the extracted data.
Each task in this milestone required to process a different set of data and so each has its own method in the class.
Data tables contained information about: users, credit cards, stores, products, times of purchase.

The products data contained a column with weights, which was more complex to clean and received a dedicated method.
The complication was that the entries contained different units of weight for different products (grams, kilograms and mililitres). 
Some entries contained multiplications of smaller weights, in the format of: weight x quantity.
Additionally, there were incorrect entries and null values as in the case of other data tables.
The solution I have chosen to unify the weight data was to filter the data to all entries that contain "x", which was only present in the multiplications.
The unit of weight (in this case "g") has been removed from all filtered entries and the "x" replaced with and asterisk. See code below:
```
df.loc[df['weight'].str.contains('x'), 'weight'] = df.loc[df['weight'].str.contains('x'), 'weight'].replace(to_replace='g', value='', regex=True)
df.loc[df['weight'].str.contains('x'), 'weight'] = df.loc[df['weight'].str.contains('x'), 'weight'].replace(to_replace='x', value='*', regex=True)
```
This has enabled me to use the pandas.eval() and a lambda function to calculate the weights and convert to kilograms, as required. See code below:
```
df.loc[df['weight'].str.contains('\*'), 'weight'] = df.loc[df['weight'].str.contains('\*'), 'weight'].apply(lambda x : pd.eval(x)/1000)
```
In the above line, pd.eval(x) takes in "x" as a string and calculates the contents. This means that entries containing: weight * quantity get calculated, correcting the entry.
Similar logic has been applied to less complex entries that just required a change of unit.

A couple of observations for every dataset processed:
    - string entries of 'NULL'
    - incorrect entries of an alphanumerical format with 10 characters, for example: XYZ123XU8E
    - dates in mixed formates

- data_extraction.py
This is the main script, containing the class DataExtractor.
Methods included in the class focus on extracting data from various sources.
Classes from the other two files, mentioned above, are imported and used together to achieve the final data outcome.




