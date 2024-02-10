from pandas.tseries.offsets import MonthEnd
import numpy as np
import pandas as pd


class DataCleaning:
    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes in a pandas dataframe containg user data from the project database and cleans it.
        Operations involve removing incorrect entries, parsing data types, correcting some entries containing extra characters.

        Args:
            df (pd.DataFrame): pandas dataframe with user data
        Returns:
            df (pd.DataFrame): pandas dataframe with clean user data
        """
        df.replace(to_replace='NULL', value=np.nan, inplace=True)

        # replaces all incorrect entries containing 10 alphanumeric uppercase characters with nan
        df.replace(to_replace='^[A-Z0-9]{10}',regex=True, value=np.nan, inplace=True) 

        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='mixed', errors='coerce').dt.date

        df['email_address'] = df['email_address'].astype('string', errors='raise')

        df['email_address'] = df['email_address'].str.replace('@@','@')

        df.loc[~df['email_address'].str.contains('@'), 'email_address'] = np.nan

        df['join_date'] = pd.to_datetime(df['join_date'], format='mixed', errors='coerce').dt.date

        df['country_code'] = df['country_code'].str.replace('GGB','GB')

        df['address'] = df['address'].str.replace('\n',' ')

        column_list = ['first_name', 'last_name', 'company', 'address','country', 'phone_number', 'user_uuid'] 
        df[column_list] = df[column_list].astype('string', errors='raise')

        df['country_code'] = df['country_code'].astype('category', errors='raise') 

        # removes entire rows with missing data by indexing the 'user_uuid' column
        df.dropna(subset= ['user_uuid'], how='all', inplace=True)

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
        df.replace(to_replace='NULL', value=np.nan, inplace=True)

        # replaces all non numeric entries in the "card_number" column with nan
        df['card_number'].replace(to_replace='^[a-zA-Z]',regex=True, value=np.nan, inplace=True)

        df.dropna(inplace=True)

        df['card_number'] = df['card_number'].astype('string', errors='raise')

        df['card_number'] = df['card_number'].str.replace('?','')
        
        # converts the "expiry_date" column to datetime infering last day for each month
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce') + MonthEnd(1)

        pd.to_datetime(df['date_payment_confirmed'], errors='coerce')

        df['card_provider'].astype('category')
        
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
        df['address'] = df['address'].str.replace('\n',' ')
        
        # removes the "lat" column since it contains minimal data
        df = df.drop('lat', axis=1)
        
        # removes all rows with incorrect entries by filtering the 'country_code' column to length of 10, inluding NaNs
        mask = df['country_code'].str.contains('^.{10}', regex=True, na=True)
        df = df[~mask]
        
        # corrects some entries in the "staff numbers" column, contained a mix of numbers and characters
        df['staff_numbers'] = df['staff_numbers'].str.replace(r'\D', '', regex=True)
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='raise')
        
        df['opening_date'] = pd.to_datetime(df['opening_date'], format='mixed', errors='coerce')
        
        # removes additional 'ee' characters in some "continent" column entries
        df['continent'].replace(to_replace='eeEurope', value='Europe', inplace=True)
        df['continent'].replace(to_replace='eeAmerica', value='America', inplace=True)
        
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
        
        df['weight'].replace(to_replace='kg', value='', regex=True, inplace=True)
        
        df['weight'].replace(to_replace='ml', value='g', regex=True, inplace=True)

        # filters all rows containing the character 'g', removes it and converts entries to kilograms
        mask_g = df['weight'].str.contains('g', na=False)
        df.loc[mask_g, 'weight'] = df.loc[mask_g, 'weight'].replace(to_replace='g', value='', regex=True).apply(lambda x : pd.eval(x)/1000 if str(x).isdigit() else x)

        # removes rows with incorrect alphanumerical data of length 10
        error_indexes = df[df['weight'].str.contains('^.{10}', regex=True, na=False)].index
        df = df.drop(error_indexes, axis=0)

        df['weight'].replace(to_replace='77 .', value=0.077, regex=True, inplace=True)

        # converts entries containing ounces into kilograms
        mask_oz = df['weight'].str.contains('oz', na=False)
        df.loc[mask_oz, 'weight'] = df.loc[mask_oz, 'weight'].replace(to_replace='oz', value='', regex=True).apply(lambda x : pd.eval(x)*28.35/1000)

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
        
        df['category'] = df['category'].astype('category')
        
        df['date_added'] = pd.to_datetime(df['date_added'],format='mixed', errors='raise')
        
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

        df['time_period'] = df['time_period'].astype('category')
        
        # rearrange columns
        df = df[['date_time','time_period', 'date_uuid']]
        
        return df