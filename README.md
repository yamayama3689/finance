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
graph LR
    subgraph Local_or_GitHub
        A[Source Code]
    end

    subgraph GCP_CI_CD
        B[cloudbuild.yaml]
    end

    subgraph Runtime_Execution
        C{main.py}
        D[screening.py]
        E[requirements.txt]
    end

    A -->|Push| B
    B -->|Deploy/Build| C
    C -->|Import/Module| D
    E -->|Install Dependencies| C

    %% データの流れ（推測）
    Input[(Data Source)] --> C
    C --> Output[(Processed Data)]

mindmap
  root((srcフォルダ))
    Execution(実行プログラム)
        main.py: メインの処理ロジック
        screening.py: フィルタリングや検証用のサブモジュール
    Infrastructure(インフラ/構成)
        cloudbuild.yaml: Google Cloudへのデプロイ・ビルド定義
        requirements.txt: 必要なライブラリの一覧
    Documentation(ドキュメント)
        README.md: プロジェクトの説明書
