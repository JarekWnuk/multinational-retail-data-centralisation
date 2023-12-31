import numpy as np, pandas as pd
from pandas.tseries.offsets import MonthEnd


class DataCleaning:
    def __init__(self) -> None:
        pass

    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes in a pandas dataframe containg user data from the project database and cleans it.
        Operations involve removing incorrect entries, parsing data types, correcting some entries containing extra characters.

        Args:
            df (pd.DataFrame): pandas dataframe with user data
        Returns:
            df (pd.DataFrame): pandas dataframe with clean user data
        """
        # replaces all string entries equal to 'NULL' with nan
        df.replace(to_replace='NULL', value=np.nan, inplace=True) 

        # replaces all incorrect entries containing 10 alphanumeric uppercase characters with nan
        df.replace(to_replace='^[A-Z0-9]{10}',regex=True, value=np.nan, inplace=True) 

        # converts dates of birth to datetime objects
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='mixed', errors='coerce').dt.date

        # converts email addresses to string 
        df['email_address'] = df['email_address'].astype('string', errors='raise')

        # removes the extra '@' character in some email addresses
        df['email_address'] = df['email_address'].str.replace('@@','@')

        # replaces all email address entries that do not contain the '@' character with nan
        df.loc[~df['email_address'].str.contains('@'), 'email_address'] = np.nan

        # converts join dates to datetime objects
        df['join_date'] = pd.to_datetime(df['join_date'], format='mixed', errors='coerce').dt.date

        # replaces 'GGB' with 'GB' in 6 country code entries
        df['country_code'] = df['country_code'].str.replace('GGB','GB')

        # replaces '\n' with a whitespace in many address entries
        df['address'] = df['address'].str.replace('\n',' ')

        # parses multiple columns as string
        df[['first_name', 'last_name', 'company', 'address','country', 'phone_number', 'user_uuid']] = \
            df[['first_name', 'last_name', 'company', 'address', 'country', 'phone_number', 'user_uuid']].astype('string', errors='raise')

        # parses country_code column as category
        df['country_code'] = df['country_code'].astype('category', errors='raise') 

        # removes all null values from the database
        df = df.dropna()

        return df
    
    def clean_card_data(self, df:pd.DataFrame) -> pd.DataFrame:   
        """
        Takes in a pandas dataframe containg card details from the project database and cleans it.
        Operations involve removing incorrect entries, parsing data types, correcting some entries containing invalid characters.
        
        Args:
            df (pd.DataFrame): pandas dataframe with card details data
        Returns:
            df (pd.DataFrame): pandas dataframe with clean card details data
        """
        # replaces all string entries equal to 'NULL' with nan
        df.replace(to_replace='NULL', value=np.nan, inplace=True)

        # replaces all non numeric entries in the "card_number" column with nan
        df['card_number'].replace(to_replace='^[a-zA-Z]',regex=True, value=np.nan, inplace=True)

        # converts the "expiry_date" column to datetime infering last day for each month
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce') + MonthEnd(1)

        # converts the "date_payment_confirmed" column to datetime
        pd.to_datetime(df['date_payment_confirmed'], errors='coerce')

        # converts the "card_provider" column to category
        df['card_provider'].astype('category')

        # removes all null values from the database
        df = df.dropna()
        
        return df
    
    def clean_store_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes in a pandas dataframe containg store data from the project database and cleans it.
        Operations involve removing incorrect entries, parsing data types, correcting some entries containing extra characters.

        Args:
            df (pd.DataFrame): pandas dataframe with store details data to clean
        Returns:
            df (pd.DataFrame): pandas dataframe with clean store details data
        """
        # replaces newline characters from the "address" column with whitespaces
        df['address'] = df['address'].str.replace('\n',' ')
        
        # removes the "lat" column since it contains minimal data
        df = df.drop('lat', axis=1)
        
        # removes multiple rows which contained only incorrect data
        df = df.drop([63, 172, 217, 231, 333, 381, 414, 447, 405, 437], axis=0)
        
        # corrects some entries in the "staff numbers" column, contained a mix of numbers and characters
        df['staff_numbers'].loc[31] = '78'   # previously J78
        df['staff_numbers'].loc[341] = '97'  # previously A97
        df['staff_numbers'].loc[179] = '30'  # previously 30e
        df['staff_numbers'].loc[248] = '80'  # previously 80R
        df['staff_numbers'].loc[375] = '39'  # previously 3n9

        # converts the "staff numbers" column to integer
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='raise')
        
        # converts the "opening date" column to datetime
        df['opening_date'] = pd.to_datetime(df['opening_date'], format='mixed', errors='coerce')
        
        # removes additional 'ee' characters in some "continent" column entries
        df['continent'].replace(to_replace='eeEurope', value='Europe', inplace=True)
        df['continent'].replace(to_replace='eeAmerica', value='America', inplace=True)
        
        # converts the "store type", "country code" and "continent" columns to category
        df[['store_type', 'country_code', 'continent']] = df[['store_type', 'country_code', 'continent']].astype('category', errors='raise')
        
        return df
    
    def convert_product_weights(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes in a dataframe containing product data and cleans all the entries in the "weight" column.
        Units are unified to kilograms. Fields containing multiplications are first calculated and the converted to kg.
        Erroneous entries are removed.

        Args:
            df (pd.DataFrame): pandas dataframe with products data to clean
        Returns:
            pd.DataFrame: pandas dataframe with clean weight column 
        """
        # removes all null values from the dataframe
        df.dropna(inplace=True)
        
        # filters the weight column for any entries containing multiplications or "x" and replaces with "*" which is accepted by pd.eval()
        # removes "g" from all filtered entries

        df.loc[df['weight'].str.contains('x'), 'weight'] = df.loc[df['weight'].str.contains('x'), 'weight'].replace(to_replace='g', value='', regex=True)
        df.loc[df['weight'].str.contains('x'), 'weight'] = df.loc[df['weight'].str.contains('x'), 'weight'].replace(to_replace='x', value='*', regex=True)
        
        # Uses pd.eval() on the cleaned entries to calculate the weights and converts to kilograms.
        df.loc[df['weight'].str.contains('\*'), 'weight'] = df.loc[df['weight'].str.contains('\*'), 'weight'].apply(lambda x : pd.eval(x)/1000)
        
        # removes the "kg" from some rows
        df['weight'].replace(to_replace='kg', value='', regex=True, inplace=True)
        
        # replaces "ml" with "g" in some rows
        df['weight'].replace(to_replace='ml', value='g', regex=True, inplace=True)

        # filters all rows containing "g", removes it and converts to kilograms
        df.loc[df['weight'].str.contains('g', na=False), 'weight'] = df.loc[df['weight'].str.contains('g', na=False), 'weight'].replace(to_replace='g', value='', regex=True).apply(lambda x : pd.eval(x)/1000 if str(x).isdigit() else x)

        # removes rows with incorrect data and corrects ones where data can be implied
        df = df.drop([751, 1133, 1400], axis=0)
        df['weight'].replace(to_replace='77 .', value=0.077, regex=True, inplace=True)
        df['weight'].replace(to_replace='16oz', value=0.454, regex=True, inplace=True)

        # converts entries in the weight column to float
        df['weight'] = df['weight'].astype(float)
        
        return df 
    
    def clean_products_data(self, df: pd.DataFrame) -> pd.DataFrame:
        
        """
        Takes in a pandas dataframe, cleans the data and returns.
        
        Args:
            df (pd.DataFrame): pandas dataframe with products data to clean
        Returns:
            pd.DataFrame: pandas dataframe with clean products data
        """     
        # removes "£" from the product_price column and converts all entries to float
        df['product_price'].replace(to_replace='£', value='', regex=True, inplace=True)
        df['product_price'] = df['product_price'].astype(float)
        
        # converts the category column to category
        df['category'] = df['category'].astype('category')
        
        # converts entries in date_added column to datetime
        df['date_added'] = pd.to_datetime(df['date_added'],format='mixed', errors='raise')
        
        #  parses the "removed" column to category
        df['removed'] = df['removed'].astype('category')
        
        return df
    
    def clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes is a pandas dataframe containing orders data, cleans it and returns.

        Args:
            df (pd.DataFrame): pandas dataframe with orders data to clean
        Returns:
            pd.DataFrame: pandas dataframe with clean orders data 
        """
        #removes redundant columns
        df.drop(columns=['level_0', 'first_name', 'last_name', '1'], inplace=True)
        
        return df
    
    def clean_date_times(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes is a pandas dataframe containing date and time data, cleans it and returns.
        
        Args:
            df (pd.DataFrame): pandas dataframe with date and time data to clean
        Returns:
            df (pd.DataFrame): pandas dataframe with clean date and time data 
        """
        # removes all incorrect values from the 'timestamp' column, specifically all entries that do not contain a colon
        df.drop(df[~df['timestamp'].str.contains(':')].index, inplace=True)

        # adds a column with datetime objects, created from columns: 'day', 'month', 'year' and 'timestap'
        df['date_time'] = df['year'] + '/' + df['month'] + '/' + df['day'] + ' ' + df['timestamp']
        df['date_time'] = pd.to_datetime(df['date_time'], format='ISO8601', errors='raise')

        # removes redundant columns
        df.drop(columns=['timestamp', 'month', 'year', 'day'], inplace=True)

        # converts the 'time_period' column to category
        df['time_period'] = df['time_period'].astype('category')
        
        # rearrange columns
        df = df[['date_time','time_period', 'date_uuid']]
        
        return df