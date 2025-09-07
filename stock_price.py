import google.cloud.bigquery as bigquery
import yfinance as yf
import pandas as pd
import pandas_gbq
import functions_framework
from datetime import datetime, timedelta


@functions_framework.http
def fetch_stock_price(request):
    # Initializing the Big Query client
    client = bigquery.Client()

    # Retrieve ticker list from BigQuery
    # Focus only on stocks listed on the Prime Market
    query = "SELECT Ticker FROM `my-project-1567934249798.finance.excellent_firms`"
    query_job = client.query(query)
    tickers = [row.Ticker for row in query_job.result()]

    # Retrieve the ticker list from the parameters
    # if not tickers or tickers == ['']:
    #     return 'The ticker does not exist', 400

    # Create an empty list to store the DataFrame
    dfs_to_concat = []

    for ticker in tickers:
        ticker = (ticker + '.T')
        # Identify yesterday's date
        yesterday = datetime.now() - timedelta(days=1)
        # Format according to yfinance's download format
        start_date = yesterday.strftime('%Y-%m-%d')
        
        data = yf.download(
            tickers=ticker,
            start=start_date,
            end=datetime.now().strftime('%Y-%m-%d'), # Include today's date in the range to be retrieved
            interval="1d",
            multi_level_index=False
        )

        # Add a 'ticker' column to the DataFrame
        data['Ticker'] = ticker

        # Reset the index to make 'Date' a column
        data.reset_index(inplace=True)

        # Reorder columns to place 'Date' and 'ticker' at the beginning
        cols = ['Ticker', 'Date'] + [col for col in data.columns if col not in ['Ticker', 'Date']]
        data = data[cols]

        # Add the new DataFrame to the list
        dfs_to_concat.append(data)

    # Join all DataFrames
    final_df = pd.concat(dfs_to_concat, ignore_index=True)

    table_id = 'my-project-1567934249798.finance.stock_price'
    pandas_gbq.to_gbq(final_df, table_id, project_id=client.project, if_exists='append')

    return 'Data loaded to BigQuery successfully!', 200
