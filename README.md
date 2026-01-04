# finance
main.py
screening.pyでスクリーニングした銘柄の財務情報（利益、cashflow）をdailyで取得しBigQuery上のテーブルに格納する。

cloudbuild.yaml
githubでpushするたびに、main.pyに書かれた関数をGCP上のCloud Runにデプロイする。

screening.py
BigQueryのexternal_table_ticker_listという各銘柄のTickerコードが格納されたテーブルから
Tickerコードのリストを取得し、いくつかの基準（DEratio, ROE, など）の数値基準をもとに有望な銘柄を絞り込む。

stock_price.py
screening.pyでスクリーニングした銘柄の株価をdailyで取得しBigQuery上のテーブルに格納する。

## データ処理フロー
```mermaid
graph TD
    %% --- サブグラフ: インフラ/トリガー ---
    subgraph Trigger ["Daily Automation"]
        Scheduler["Cloud Scheduler"]
        CB["cloudbuild.yaml<br/>(Execution Environment)"]
    end

    %% --- サブグラフ: スクリプト処理 ---
    subgraph Scripts ["Python Scripts"]
        Screening["screening.py<br/>(Create Master List)"]
        StockPrice["stockprice.py<br/>(Fetch Stock Prices)"]
        Main["main.py<br/>(Fetch Financial Info)"]
    end

    %% --- サブグラフ: 外部/ストレージ ---
    subgraph Data ["Data Sources & Storage"]
        Yahoo["Yahoo Finance API"]
        BQ_Excellent[("BigQuery<br/>Table: excellent_firms")]
        BQ_Final[("BigQuery<br/>Table: final_df")]
    end

    %% --- 処理フロー ---
    
    %% 1. マスタ生成 (独立したプロセスとして表現)
    Screening -->|"Analyze Fundamentals"| BQ_Excellent
    
    %% 2. 日次バッチ処理
    Scheduler -->|"Trigger Daily"| CB
    CB -->|"Execute"| StockPrice
    CB -->|"Execute"| Main

    %% 3. データ取得と参照
    BQ_Excellent -.-|"Read Tickers"| StockPrice
    BQ_Excellent -.-|"Read Tickers"| Main
    
    StockPrice -->|"Fetch Price"| Yahoo
    Main -->|"Fetch Financials"| Yahoo
    
    %% 4. データ格納
    Yahoo -->|"Write Data"| BQ_Final
