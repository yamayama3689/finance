# Execute the following command in advance at the terminal to authenticate the gcloud CLI.
# gcloud auth application-default login

# import the necessary libraries
import google.cloud.bigquery as bigquery
import yfinance as yf
import pandas as pd
import pandas_gbq
import numpy as np
import concurrent.futures
import time

# Initializing the Big Query client
client = bigquery.Client()

# Retrieve ticker list from BigQuery
# Focus only on stocks listed on the Prime Market
query = "SELECT Code FROM `my-project-1567934249798.finance.external_table_ticker_list` WHERE MarketCode = '0111'"
query_job = client.query(query)
ticker_list = [row.Code for row in query_job.result()]


# Define the function which retrieves fundamentals
def get_financials(ticker):
    try:
        # By introducing a delay of a few seconds for each request to yfinance, the load on the API is distributed
        time.sleep(2)
        ticker = ticker[:-1]
        stock = yf.Ticker(ticker + '.T')  # Toobtain data on firms listed on the Tokyo Stock Exchange, you must add .T at the end
        info = stock.info
        # get metrics
        # Debt/Euity Ratio
        debt_equity = info.get('debtToEquity', np.nan)
        # Current Ratio
        current_ratio = info.get('currentRatio', np.nan)
        # PBR
        price_book = info.get('priceToBook', np.nan)
        # ROE(%)
        roe = info.get('returnOnEquity', np.nan) * 100
        # ROA(%)
        roa = info.get('returnOnAssets', np.nan) * 100
        # Profit growth (quarter)
        quarter_growth = info.get('earningsQuarterlyGrowth', np.nan)
        return {
            'Ticker': ticker,
            'Debt_Equity': debt_equity,
            'Current_Ratio': current_ratio,
            'Price_Book': price_book,
            'ROE': roe,
            'ROA': roa,
            'Quarter Growth': quarter_growth,
        }
    except Exception as e:
        print(f'Error fetching data for {ticker}: {e}')
        return None


# Retrieve data using yfinance
data_frames = []

# By reducing the number of requests sent simultaneously, we can lower the likelihood of hitting the API rate limit
MAX_THREADS = 5

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    # USE 'map' to apply the get_financials function to multiple tickers simultaneously
    results = executor.map(get_financials, ticker_list)
    # Exclude None from the results
    data_frames = [data for data in results if data is not None]

# transform into dataframe
df = pd.DataFrame(data_frames)

# Points are added when each screening condition is met
df['Point'] = 0
df.loc[df['Debt_Equity'] < 0.5, 'Point'] += 10
df.loc[df['Current_Ratio'] > 1.5, 'Point'] += 10
df.loc[df['Price_Book'] < 1.5, 'Point'] += 10
df.loc[df['ROE'] > 8, 'Point'] += 10
df.loc[df['ROA'] > 6, 'Point'] += 10
df.loc[df['Quarter Growth'] > 0.10, 'Point'] += 10

# abstract stocks more than 50 points
excellent_firms = df[df['Point'] >= 50]

# Write data to BigQueery
# If there is no table, save it as a new table. If there is a table, replace the data.
table_id = 'my-project-1567934249798.finance.excellent_firms'
pandas_gbq.to_gbq(excellent_firms, table_id, project_id=client.project, if_exists='replace')
