import pandas as pd
import alpha_vantage.timeseries
import alpha_vantage.fundamentaldata

import sqlalchemy

import keys
from keys import api_key

import pprint

class StockData:
    """
    A class for fetching, storing and persisting stock data for a particular ticker. 
    
    Parameters:
    -----------
    ticker : str
        The ticker symbol for the stock.
    
    Attributes:
    -----------
    ticker : str
        The ticker symbol for the stock.
    ts : alpha_vantage.timeseries.TimeSeries object
        The Alpha Vantage API time series object.
    fd : alpha_vantage.fundamentaldata.FundamentalData object
        The Alpha Vantage API fundamental data object.
    
    Methods:
    --------
    get_historical_data():
        Fetches historical daily adjusted stock data for the ticker and returns it as a pandas DataFrame.
        
    get_fundamental_data():
        Fetches company overview, balance sheet, income statement and cash flow data for the ticker and returns each 
        dataset as a separate pandas DataFrame.
    
    save_to_database():
        Connects to a PostgreSQL database and saves historical, overview, balance sheet, income statement and 
        cash flow data for the ticker in their respective tables.
    """
    
    def __init__(self, ticker: str):
        """
        Constructs a new StockData object for the specified ticker.
        
        Parameters:
        -----------
        ticker : str
            The ticker symbol for the stock.
        """
        self.ticker = ticker
        self.ts = alpha_vantage.timeseries.TimeSeries(key=api_key,
                                                      output_format='pandas')
        self.fd = alpha_vantage.fundamentaldata.FundamentalData(key=api_key)
    
    def get_historical_data(self):
        """
        Fetches historical daily adjusted stock data for the ticker and returns it as a pandas DataFrame.
        
        Returns:
        --------
        historical_data : pandas.DataFrame
            A DataFrame containing historical daily adjusted stock data for the ticker.
        """
        data, _ = self.ts.get_daily_adjusted(symbol=self.ticker, outputsize='full')
        historical_data = pd.DataFrame(data=data)
        historical_data = historical_data.rename(columns={historical_data.columns[0]: 'open',
                                                          historical_data.columns[1]: 'high',
                                                          historical_data.columns[2]: 'low',
                                                          historical_data.columns[3]: 'close',
                                                          historical_data.columns[4]: 'adjusted_close',
                                                          historical_data.columns[5]: 'volume', 
                                                          historical_data.columns[6]: 'dividend_amount',
                                                          historical_data.columns[7]: 'split_coefficient'})
        historical_data = historical_data.reset_index()
        return historical_data
    
    def get_fundamental_data(self):
        """
        Fetches company overview, balance sheet, income statement and cash flow data for the ticker and returns each 
        dataset as a separate pandas DataFrame.
        
        Returns:
        --------
        overview_df : pandas.DataFrame
            A DataFrame containing company overview data for the ticker.
        bs_df : pandas.DataFrame
            A DataFrame containing balance sheet data for the ticker.
        is_df : pandas.DataFrame
            A DataFrame containing income statement data for the ticker.
        cf_df : pandas.DataFrame
            A DataFrame containing cash flow data for the ticker.
        """
        overview = self.fd.get_company_overview(symbol=self.ticker)
        overview_df = pd.DataFrame(overview[0], index=[0])
        
        q_bs = self.fd.get_balance_sheet_quarterly(symbol=self.ticker)
        bs_df = pd.DataFrame.from_records(q_bs[0])

        q_is = self.fd.get_income_statement_quarterly(symbol=self.ticker)
        is_df = pd.DataFrame.from_records(q_is[0])

        q_cf = self.fd.get_cash_flow_quarterly(symbol=self.ticker)
        cf_df = pd.DataFrame.from_records(q_cf[0])

        return overview_df, bs_df, is_df, cf_df

    def save_to_database(self, conn_str: str):
        """
        Connects to a PostgreSQL database and saves historical, overview, balance sheet, income statement and 
        cash flow data for the ticker in their respective tables.
    
        Parameters:
        -----------
        conn_str : str
            A string containing the connection information for the PostgreSQL database.
        
        """
        engine = sqlalchemy.create_engine(conn_str)
        historical_data = self.get_historical_data()
        overview_df, bs_df, is_df, cf_df = self.get_fundamental_data()

        historical_data.to_sql('historical_data', engine, if_exists='replace', index=False)
        overview_df.to_sql('microsoft', engine, if_exists='replace', index=False)
        bs_df.to_sql('balance_sheet_data', engine, if_exists='replace', index=False)
        is_df.to_sql('income_statement_data', engine, if_exists='replace', index=False)
        cf_df.to_sql('cash_flow_data', engine, if_exists='replace', index=False)




if __name__ == '__main__':
    stock = StockData('MSFT')
    # stock.save_to_database(conn_str=keys.cloud_connection_string)