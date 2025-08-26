import google.cloud.bigquery as bigquery
import yfinance as yf
import pandas as pd
import pandas_gbq
import functions_framework


@functions_framework.http
def fetch_stock_financials(request):
    """
    Triggered by an HTTP request, this function retrieves and processes financial data for specified stocks and returns the results.
    """
    try:
        # Initializing the Big Query client
        client = bigquery.Client()

        # Retrieve ticker list from BigQuery
        # Focus only on stocks listed on the Prime Market
        query = "SELECT Ticker FROM `my-project-1567934249798.finance.excellent_firms`"
        query_job = client.query(query)
        tickers = [row.Ticker for row in query_job.result()]

        # Get ticker list from parameters
        if not tickers or tickers == ['']:
            return 'The ticker does not exist', 400

        # Create an empty list to store the DataFrame.
        dfs_to_concat = []

        for ticker in tickers:
            stock = yf.Ticker(ticker + '.T')

            # Obtain sales and profit information from the income statement
            income = stock.incomestmt
            income_transpose = income.T
            sales_profit = income_transpose[['Total Revenue', 'Gross Profit', 'Operating Income', 'Net Income']].copy()
            sales_profit.columns = ['total_revenue', 'gross_profit', 'operating_profit', 'net_income']

            # calculate profit margin
            sales_profit.loc[:, 'gross_margin'] = sales_profit['gross_profit'] / sales_profit['total_revenue'] * 100
            sales_profit.loc[:, 'operating_margin'] = sales_profit['operating_profit'] / sales_profit['total_revenue'] * 100
            sales_profit.loc[:, 'net_margin'] = sales_profit['net_income'] / sales_profit['total_revenue'] * 100

            # Obtain cash flow information
            cashflow = stock.cashflow
            cashflow_transpose = cashflow.T
            cf = cashflow_transpose[['Operating Cash Flow', 'Investing Cash Flow', 'Financing Cash Flow', 'Free Cash Flow']].copy()
            cf.columns = ['operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow', 'free_cash_flow']

            # Join two DataFrames internally
            firm_performance = pd.merge(sales_profit, cf, left_index=True, right_index=True)

            # Remove rows containing NuLL values from the joined DataFrame
            firm_performance = firm_performance.dropna()
            
            # Add an index (date) to the column.
            firm_performance = firm_performance.reset_index()
            firm_performance.rename(columns={'index': 'date'}, inplace=True)
            
            # Delete time information using .dt.date
            firm_performance['date'] = firm_performance['date'].dt.date
            
            # Add ticker and company name
            firm_performance.insert(0, 'ticker', ticker)
            firm_performance.insert(1, 'firm_name', stock.info.get('longName', 'N/A'))
            
            # Add a new DataFrame to the list
            dfs_to_concat.append(firm_performance)
            
        # Combine all DataFrames
        final_df = pd.concat(dfs_to_concat, ignore_index=True)
        
        # Write on the BQ table
        table_id = 'my-project-1567934249798.finance.firm_performance'
        pandas_gbq.to_gbq(final_df, table_id, project_id=client.project, if_exists='replace')
    except Exception as e:
        print(f"An error occurred: {e}")
        return f"An error occurred: {e}", 500
