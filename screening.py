# Execute the following command in advance at the terminal to authenticate the gcloud CLI.
# gcloud auth application-default login

# import the necessary libraries
from google.cloud import bigquery
import yfinance as yf
import pandas as pd

# Initializing the Big Query client
client = bigquery.Client()

# Retrieve ticker list from BigQuery
# Focus only on stocks listed on the Prime Market
query = "SELECT Code FROM `my-project-1567934249798.finance.external_table_ticker_list` WHERE MarketCode = '0111'"
query_job = client.query(query)
code_list = [row.Code for row in query_job.result()]

# Retrieve data using yfinance
data_frames = []
for code in code_list:
    try:
        ticker = yf.Ticker(code + '.T')  # Toobtain data on firms listed on the Tokyo Stock Exchange, you must add .T at the end
        hist = ticker.history(period="1mo")
        hist['Code'] = code
        data_frames.append(hist)
        print(f"取得完了: {code}")
    except Exception as e:
        print(f"取得失敗: {code}, エラー: {e}")
        continue

# Combine the acquired data into a single DataFrame
if data_frames:
    all_data = pd.concat(data_frames)

    # Write data to BigQueery
    # If there is no table, save it as a new table. If there is a table, replace the data.
    table_id = 'my-project-1567934249798.finance.excellent_firms'
    all_data.to_gbq(table_id, project_id=client.project, if_exists='replace')

    print("データが正常にBigQueryに保存されました。")
else:
    print("取得するデータがありませんでした。")
