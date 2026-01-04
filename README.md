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
    %% --- ノード定義 ---
    
    %% インフラ・トリガー
    subgraph Trigger ["Automation"]
        Scheduler["Cloud Scheduler<br/>(Daily Trigger)"]
        CB["cloudbuild.yaml<br/>(Runner)"]
    end

    %% データ処理スクリプト
    subgraph Processor ["Data Processing (Python)"]
        S_Stock["stockprice.py<br/>(Fetch Prices)"]
        S_Main["main.py<br/>(Fetch Financials)"]
        S_Screening["screening.py<br/>(Update Master)"]
    end

    %% データソースと保存先
    subgraph Storage ["Data Lake / Warehouse"]
        Yahoo["Yahoo Finance API<br/>(External Source)"]
        BQ_Master[("BigQuery: excellent_firms<br/>(Ticker Master)")]
        BQ_Final[("BigQuery: final_df<br/>(Data Mart)")]
    end

    %% --- データの流れ (Data Flow) ---

    %% 1. マスタ更新フロー
    S_Screening -->|"1. 銘柄選定 & 更新"| BQ_Master

    %% 2. 実行トリガー
    Scheduler -->|"2. 起動"| CB
    CB -->|"実行"| S_Stock
    CB -->|"実行"| S_Main

    %% 3. データ取得プロセス (ここを修正)
    %% マスタから対象Tickerを読み込む
    BQ_Master -->|"3. 対象Tickerリストの取得"| S_Stock
    BQ_Master -->|"3. 対象Tickerリストの取得"| S_Main

    %% Yahoo APIからデータを吸い上げる (矢印をYahoo起点に変更)
    Yahoo -->|"4. 株価データの提供"| S_Stock
    Yahoo -->|"4. 財務データの提供"| S_Main

    %% 4. データの格納
    S_Stock -->|"5. データ書き込み"| BQ_Final
    S_Main -->|"5. データ書き込み"| BQ_Final

    %% --- スタイル調整 ---
    style Yahoo fill:#E37400,color:#fff
    style BQ_Master fill:#FBBC04,color:#000
    style BQ_Final fill:#34A853,color:#fff
    style S_Stock fill:#4285F4,color:#fff
    style S_Main fill:#4285F4,color:#fff
