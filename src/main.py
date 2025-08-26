import google.cloud.bigquery as bigquery
import yfinance as yf
import pandas as pd
import pandas_gbq
import functions_framework


@functions_framework.http
def fetch_stock_financials(request):
    print('Hello-World')
