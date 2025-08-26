import google.cloud.bigquery as bigquery
import yfinance as yf
import pandas as pd
import pandas_gbq
import functions_framework


@functions_framework.http
def fetch_stock_financials(request):
    # Initializing the Big Query client
    client = bigquery.Client()

    # Retrieve ticker list from BigQuery
    # Focus only on stocks listed on the Prime Market
    query = "SELECT Ticker FROM `my-project-1567934249798.finance.excellent_firms`"
    query_job = client.query(query)
    tickers = [row.Ticker for row in query_job.result()]

    # パラメータからティッカーリストを取得
    # if not tickers or tickers == ['']:
    #     return 'The ticker does not exist', 400

    # DataFrameを格納する空のリストを作成
    dfs_to_concat = []

    for ticker in tickers:
        stock = yf.Ticker(ticker + '.T')

        # 損益計算書から売上と利益情報を取得
        income = stock.incomestmt
        income_transpose = income.T
        sales_profit = income_transpose[['Total Revenue', 'Gross Profit', 'Operating Income', 'Net Income']].copy()
        sales_profit.columns = ['total_revenue', 'gross_profit', 'operating_profit', 'net_income']

        # 利益率を計算
        sales_profit.loc[:, 'gross_margin'] = sales_profit['gross_profit'] / sales_profit['total_revenue'] * 100
        sales_profit.loc[:, 'operating_margin'] = sales_profit['operating_profit'] / sales_profit['total_revenue'] * 100
        sales_profit.loc[:, 'net_margin'] = sales_profit['net_income'] / sales_profit['total_revenue'] * 100

        # キャッシュフロー情報を取得
        cashflow = stock.cashflow
        cashflow_transpose = cashflow.T
        cf = cashflow_transpose[['Operating Cash Flow', 'Investing Cash Flow', 'Financing Cash Flow', 'Free Cash Flow']].copy()
        cf.columns = ['operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow', 'free_cash_flow']

        # 2つのDataFrameを内部結合
        firm_performance = pd.merge(sales_profit, cf, left_index=True, right_index=True)
        
        # 結合後のDataFrameからNuLL値を含む行を削除
        firm_performance = firm_performance.dropna()
        
        # インデックス（日付）をカラムで持たせる。
        firm_performance = firm_performance.reset_index()
        firm_performance.rename(columns={'index': 'date'}, inplace=True)
        # .dt.date を使って時刻情報を削除
        firm_performance['date'] = firm_performance['date'].dt.date
        
        # ティッカーと企業名を追加
        firm_performance.insert(0, 'ticker', ticker)
        firm_performance.insert(1, 'firm_name', stock.info.get('longName', 'N/A'))

        # 新しいDataFrameをリストに追加
        dfs_to_concat.append(firm_performance)

    # 全てのDataFrameを結合
    final_df = pd.concat(dfs_to_concat, ignore_index=True)

    table_id = 'my-project-1567934249798.finance.firm_performance'
    pandas_gbq.to_gbq(final_df, table_id, project_id=client.project, if_exists='replace')
