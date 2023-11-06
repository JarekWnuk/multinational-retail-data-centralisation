import pandas as pd
import numpy as np

class DataCleaning:
    def __init__(self) -> None:
        pass

    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes in a pandas dataframe containg user data from the project database and cleans it.
        Operations involve removing incorrect entries, parsing data types, correcting some entries containing extra characters.

        Args:
            df: pandas dataframe
        
        Returns:
            df: pandas dataframe with clean data
        """
        # replaces all string entries equal to 'NULL' with nan
        df.replace(to_replace='NULL', value=np.nan, inplace=True) 

        # replaces all incorrect entries containing 10 alphanumeric uppercase characters with nan
        df.replace(to_replace='^[A-Z0-9]{10}',regex=True, value=np.nan, inplace=True) 

        # converts dates of birth to datetime objects
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='mixed', errors='coerce').dt.date

        # converts email addresses to string type raising any errors
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
        df.dropna(inplace=True)

        return df

    