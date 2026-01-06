import sys
import pandas as pd
import numpy as np
import boto3
import io
import time
import math
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'process_date', 'load_type'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args['source_bucket']
process_date = args['process_date']
load_type = args['load_type']

print("Processing date: " + process_date)
print("Load type: " + load_type)

s3 = boto3.client('s3')

year_val, month_val, day_val = process_date.split('-')

DATA_START_DATE = '2018-01-01'

# ticker -> column name mappings
TICKER_MAPPING = {
    '^GSPC': 'sp500',
    '^IXIC': 'nasdaq',
    'BTC-USD': 'bitcoin_price',
    'ETH-USD': 'ethereum_price',
    'CL=F': 'oil',
    'GC=F': 'gold',
    'HG=F': 'copper',
    'USDCHF=X': 'usdchf',
    'EURUSD=X': 'eurusd',
    'GBPUSD=X': 'gbpusd',
    'USDJPY=X': 'usdjpy',
}

CRYPTOCOMPARE_MAPPING = {
    'BTC-USD': 'bitcoin_price_cc',
    'ETH-USD': 'ethereum_price_cc',
}

# FRED series -> column name
FRED_MAPPING = {
    'DFF': 'policy_rate_us',
    'DGS10': 'treasury_10y',
    'DGS2': 'treasury_2y',
    'T10Y2Y': 'treasury_spread',
    'DPRIME': 'prime_rate',
    'CPIAUCSL': 'cpi_us',
    'GDP': 'gdp_us',
    'M2SL': 'm2_us',
    'ICSA': 'jobless_claims',
    'WALCL': 'fed_balance_sheet',
    'CHECPIALLMINMEI': 'cpi_ch',
    'GBRCPIALLMINMEI': 'cpi_uk',
    'JPNCPIALLMINMEI': 'cpi_jp',
    'CP0000EZ19M086NEST': 'cpi_eu',
    'CLVMNACSCAB1GQEA19': 'gdp_eu',
    'CSCICP02EZM460S': 'cc_eu',
    'CSCICP02GBM460S': 'cc_uk',
    'CSCICP02JPM460S': 'cc_jp',
    'CSCICP02DEM460S': 'cc_de',
    'CSCICP02CHQ460S': 'cc_ch',
    'CSINFT02USM460S': 'cc_us',
}

# dbnomics series -> column name
DBNOMICS_MAPPING = {
    'BIS/WS_CBPOL/D.XM': 'ecb_main_rate',
    'BIS/WS_CBPOL/D.GB': 'policy_rate_uk',
    'BIS/WS_CBPOL/D.CH': 'policy_rate_ch',
    'BIS/WS_CBPOL/D.JP': 'policy_rate_jp',
    'OECD/MEI_CLI/LOLITOAA.JPN.M': 'cc_jp',
    'OECD/MEI_CLI/LOLITOAA.GBR.M': 'cc_uk',
    'OECD/MEI_CLI/LOLITOAA.DEU.M': 'cc_eu',
}

ECON_MAPPING = {**FRED_MAPPING, **DBNOMICS_MAPPING}

BASE_COLUMNS = [
    'date',
    'sp500', 'nasdaq',
    'bitcoin_price', 'ethereum_price',
    'bitcoin_price_cc', 'ethereum_price_cc',
    'oil', 'gold', 'copper',
    'usdchf', 'eurusd', 'gbpusd', 'usdjpy',
    'policy_rate_us', 'cpi_us', 'gdp_us', 'm2_us',
    'treasury_10y', 'treasury_2y', 'treasury_spread', 'prime_rate',
    'jobless_claims', 'fed_balance_sheet',
    'cpi_ch', 'cpi_uk', 'cpi_jp', 'cpi_eu',
    'gdp_eu',
    'ecb_main_rate', 'policy_rate_uk', 'policy_rate_ch', 'policy_rate_jp',
    'cc_us', 'cc_jp', 'cc_uk', 'cc_eu', 'cc_de', 'cc_ch',
]

# macro columns to forward-fill (not market data)
FORWARD_FILL_COLUMNS = [
    'policy_rate_us', 'cpi_us', 'gdp_us', 'm2_us',
    'treasury_10y', 'treasury_2y', 'treasury_spread', 'prime_rate',
    'jobless_claims', 'fed_balance_sheet',
    'cpi_ch', 'cpi_uk', 'cpi_jp', 'cpi_eu',
    'gdp_eu',
    'ecb_main_rate', 'policy_rate_uk', 'policy_rate_ch', 'policy_rate_jp',
    'cc_us', 'cc_jp', 'cc_uk', 'cc_eu', 'cc_de', 'cc_ch',
]


def read_parquet_files(bucket, prefix, s3_client):
    """Read all parquet files from a given S3 prefix"""
    dfs = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.parquet'):
                    print("Reading: " + key)
                    s3_obj = s3_client.get_object(Bucket=bucket, Key=key)
                    pdf = pd.read_parquet(io.BytesIO(s3_obj['Body'].read()))
                    dfs.append(pdf)
    except Exception as e:
        print("Warning: Error reading from " + prefix + ": " + str(e))
    return dfs


def read_cryptocompare_data(bucket, s3_client, load_type, year_val=None, month_val=None, day_val=None):
    """Read CryptoCompare crypto data and return pivoted dataframe"""
    
    cc_dfs = []
    
    if load_type == "historical":
        cc_prefix = "transformed/asset_class=crypto/source=cryptocompare/load_type=historical/"
        cc_dfs.extend(read_parquet_files(bucket, cc_prefix, s3_client))
        
        cc_prefix_daily = "transformed/asset_class=crypto/source=cryptocompare/load_type=daily/"
        cc_dfs.extend(read_parquet_files(bucket, cc_prefix_daily, s3_client))
    else:
        daily_path_suffix = "load_type=daily/year=" + year_val + "/month=" + month_val + "/day=" + day_val + "/"
        cc_prefix = "transformed/asset_class=crypto/source=cryptocompare/" + daily_path_suffix
        cc_dfs.extend(read_parquet_files(bucket, cc_prefix, s3_client))
    
    if len(cc_dfs) == 0:
        print("No CryptoCompare data found")
        return None
    
    cc_df = pd.concat(cc_dfs, ignore_index=True)
    print("CryptoCompare records loaded: " + str(len(cc_df)))
    
    cc_df = cc_df[cc_df['ticker'].isin(CRYPTOCOMPARE_MAPPING.keys())]
    
    if len(cc_df) == 0:
        print("No BTC/ETH data in CryptoCompare")
        return None
    
    cc_df['column_name'] = cc_df['ticker'].map(CRYPTOCOMPARE_MAPPING)
    cc_df['date'] = pd.to_datetime(cc_df['date'])
    
    cc_df = cc_df.sort_values(['date', 'ticker', 'transform_timestamp']).drop_duplicates(
        subset=['date', 'ticker'], keep='last'
    )
    
    print("CryptoCompare records after dedup: " + str(len(cc_df)))
    print("CryptoCompare tickers: " + str(cc_df['ticker'].unique().tolist()))
    
    cc_wide = cc_df.pivot_table(
        index='date',
        columns='column_name',
        values='close',
        aggfunc='first'
    ).reset_index()
    
    print("CryptoCompare pivoted: " + str(cc_wide.shape))
    print("CryptoCompare columns: " + str(cc_wide.columns.tolist()))
    
    return cc_wide


def filter_weekends(df):
    """Remove weekend rows from dataframe"""
    df['date'] = pd.to_datetime(df['date'])
    df['_day_of_week'] = df['date'].dt.dayofweek
    
    rows_before = len(df)
    df = df[df['_day_of_week'] <= 4].copy()
    df = df.drop(columns=['_day_of_week'])
    
    rows_removed = rows_before - len(df)
    print("Weekend filtering: removed " + str(rows_removed) + " weekend rows (kept " + str(len(df)) + " weekday rows)")
    
    return df.reset_index(drop=True)


def calculate_metrics(df):
    """Calculate derived metrics: returns, volatility, correlation, normalized prices, z-scores"""
    
    trading_days = 252
    sqrt_trading_days = math.sqrt(trading_days)
    
    # daily returns
    return_columns = {
        'bitcoin_price': 'btc_ret',
        'ethereum_price': 'eth_ret',
        'bitcoin_price_cc': 'btc_cc_ret',
        'ethereum_price_cc': 'eth_cc_ret',
        'sp500': 'sp500_ret',
        'nasdaq': 'nasdaq_ret',
        'gold': 'gold_ret',
        'copper': 'copper_ret',
        'oil': 'oil_ret',
    }
    
    for price_col, ret_col in return_columns.items():
        if price_col in df.columns:
            df[ret_col] = df[price_col].pct_change()
    
    # 7-day rolling volatility
    volatility_7d_columns = {
        'btc_ret': ('btc_vol_7d', 'btc_vol_7d_annualized'),
        'eth_ret': ('eth_vol_7d', 'eth_vol_7d_annualized'),
        'sp500_ret': ('sp500_vol_7d', 'sp500_vol_7d_annualized'),
        'nasdaq_ret': ('nasdaq_vol_7d', 'nasdaq_vol_7d_annualized'),
        'gold_ret': ('gold_vol_7d', 'gold_vol_7d_annualized'),
        'copper_ret': ('copper_vol_7d', 'copper_vol_7d_annualized'),
        'oil_ret': ('oil_vol_7d', 'oil_vol_7d_annualized'),
    }
    
    for ret_col, (vol_col, vol_ann_col) in volatility_7d_columns.items():
        if ret_col in df.columns:
            df[vol_col] = df[ret_col].rolling(window=7, min_periods=2).std()
            df[vol_ann_col] = df[vol_col] * sqrt_trading_days
    
    # 30-day rolling volatility (annualized)
    volatility_30d_columns = {
        'btc_ret': 'btc_vol_30d',
        'eth_ret': 'eth_vol_30d',
        'sp500_ret': 'sp500_vol_30d',
        'gold_ret': 'gold_vol_30d',
        'copper_ret': 'copper_vol_30d',
        'oil_ret': 'oil_vol_30d',
    }
    
    for ret_col, vol_col in volatility_30d_columns.items():
        if ret_col in df.columns:
            df[vol_col] = df[ret_col].rolling(window=30, min_periods=5).std() * sqrt_trading_days
    
    # 90-day rolling correlation (BTC vs SP500)
    if 'btc_ret' in df.columns and 'sp500_ret' in df.columns:
        df['corr_btc_sp500_90d'] = df['btc_ret'].rolling(window=90, min_periods=30).corr(df['sp500_ret'])
    
    # normalized prices (base 100)
    normalize_columns = {
        'bitcoin_price': 'btc_norm',
        'ethereum_price': 'eth_norm',
        'bitcoin_price_cc': 'btc_cc_norm',
        'ethereum_price_cc': 'eth_cc_norm',
        'sp500': 'sp500_norm',
        'nasdaq': 'nasdaq_norm',
        'gold': 'gold_norm',
        'copper': 'copper_norm',
        'oil': 'oil_norm',
    }
    
    for price_col, norm_col in normalize_columns.items():
        if price_col in df.columns:
            first_valid = df[price_col].dropna().iloc[0] if df[price_col].dropna().shape[0] > 0 else None
            if first_valid is not None and first_valid != 0:
                df[norm_col] = (df[price_col] / first_valid) * 100
            else:
                df[norm_col] = None
    
    # z-scores (252-day rolling window)
    # set to 0 when std=0 (constant values like held policy rates)
    zscore_columns = [
        ('policy_rate_us', 'policy_rate_us_z'),
        ('treasury_10y', 'treasury_10y_z'),
        ('treasury_2y', 'treasury_2y_z'),
        ('prime_rate', 'prime_rate_z'),
    ]
    
    for col_name, z_name in zscore_columns:
        if col_name in df.columns:
            rolling_mean = df[col_name].rolling(window=252, min_periods=30).mean()
            rolling_std = df[col_name].rolling(window=252, min_periods=30).std()
            df[z_name] = (df[col_name] - rolling_mean) / rolling_std
            df.loc[rolling_std == 0, z_name] = 0.0
            
    policy_rate_cols_z = [
        ('policy_rate_us', 'policy_rate_us_z'),
        ('ecb_main_rate', 'ecb_main_rate_z'),
        ('policy_rate_uk', 'policy_rate_uk_z'),
        ('policy_rate_ch', 'policy_rate_ch_z'),
        ('policy_rate_jp', 'policy_rate_jp_z'),
    ]
    
    for col_name, z_name in policy_rate_cols_z:
        if col_name in df.columns:
            rolling_mean = df[col_name].rolling(window=252, min_periods=30).mean()
            rolling_std = df[col_name].rolling(window=252, min_periods=30).std()
            df[z_name] = (df[col_name] - rolling_mean) / rolling_std
            df.loc[rolling_std == 0, z_name] = 0.0
    
    cc_cols_z = [
        ('cc_us', 'cc_us_z'),
        ('cc_jp', 'cc_jp_z'),
        ('cc_uk', 'cc_uk_z'),
        ('cc_eu', 'cc_eu_z'),
        ('cc_de', 'cc_de_z'),
        ('cc_ch', 'cc_ch_z'),
    ]

    for col_name, z_name in cc_cols_z:
        if col_name in df.columns:
            rolling_mean = df[col_name].rolling(window=252, min_periods=30).mean()
            rolling_std = df[col_name].rolling(window=252, min_periods=30).std()
            df[z_name] = (df[col_name] - rolling_mean) / rolling_std
            df.loc[rolling_std == 0, z_name] = 0.0

    # price difference: yfinance vs cryptocompare
    if 'bitcoin_price' in df.columns and 'bitcoin_price_cc' in df.columns:
        df['btc_price_diff'] = df['bitcoin_price'] - df['bitcoin_price_cc']
        df['btc_price_diff_pct'] = (df['btc_price_diff'] / df['bitcoin_price']) * 100
    
    if 'ethereum_price' in df.columns and 'ethereum_price_cc' in df.columns:
        df['eth_price_diff'] = df['ethereum_price'] - df['ethereum_price_cc']
        df['eth_price_diff_pct'] = (df['eth_price_diff'] / df['ethereum_price']) * 100
    
    return df


# main logic

if load_type == "historical":
    
    # full refresh: read all data, recalculate everything
    print("=== FULL REFRESH MODE ===")
    
    yfinance_assets = ['stocks', 'indices', 'bonds', 'crypto', 'commodities', 'currencies']
    all_market_data = []
    
    for asset_class in yfinance_assets:
        historical_prefix = "transformed/asset_class=" + asset_class + "/source=yfinance/load_type=historical/"
        historical_dfs = read_parquet_files(bucket, historical_prefix, s3)
        all_market_data.extend(historical_dfs)
        
        daily_prefix = "transformed/asset_class=" + asset_class + "/source=yfinance/load_type=daily/"
        daily_dfs = read_parquet_files(bucket, daily_prefix, s3)
        all_market_data.extend(daily_dfs)
    
    if len(all_market_data) == 0:
        print("No market data found")
        job.commit()
        sys.exit(0)
    
    market_df = pd.concat(all_market_data, ignore_index=True)
    print("Total market records before filter: " + str(len(market_df)))
    
    market_df = market_df[market_df['ticker'].isin(TICKER_MAPPING.keys())]
    market_df['column_name'] = market_df['ticker'].map(TICKER_MAPPING)
    market_df['date'] = pd.to_datetime(market_df['date'])
    
    market_df = market_df[market_df['date'] >= DATA_START_DATE]
    print("Market records after date filter (>= " + DATA_START_DATE + "): " + str(len(market_df)))
    
    market_df = market_df.sort_values(['date', 'ticker', 'transform_timestamp']).drop_duplicates(
        subset=['date', 'ticker'], keep='last'
    )
    
    print("Market records after dedup: " + str(len(market_df)))
    print("Unique tickers: " + str(market_df['ticker'].unique().tolist()))
    
    market_wide = market_df.pivot_table(
        index='date',
        columns='column_name',
        values='close',
        aggfunc='first'
    ).reset_index()
    
    print("Market data pivoted: " + str(market_wide.shape))
    print("Market columns: " + str(market_wide.columns.tolist()))
    
    # read cryptocompare data
    print("\n--- Reading CryptoCompare data ---")
    cc_wide = read_cryptocompare_data(bucket, s3, load_type)
    
    if cc_wide is not None:
        cc_wide = cc_wide[cc_wide['date'] >= DATA_START_DATE]
        print("CryptoCompare records after date filter: " + str(len(cc_wide)))
        
        market_wide = pd.merge(market_wide, cc_wide, on='date', how='outer')
        print("After merging CryptoCompare: " + str(market_wide.shape))
    
    # read economic data (FRED + dbnomics)
    all_econ_data = []
    
    for prefix_type in ['historical', 'daily']:
        if prefix_type == 'historical':
            fred_prefix = "transformed/asset_class=economic/source=fred/load_type=historical/"
        else:
            fred_prefix = "transformed/asset_class=economic/source=fred/load_type=daily/"
        
        fred_dfs = read_parquet_files(bucket, fred_prefix, s3)
        for df_temp in fred_dfs:
            df_temp['data_source'] = 'fred'
        all_econ_data.extend(fred_dfs)
    
    dbnomics_providers = ['bis_data', 'ecb_direct', 'eurostat', 'imf', 'oecd', 'worldbank']
    
    for provider in dbnomics_providers:
        for prefix_type in ['historical', 'daily']:
            if prefix_type == 'historical':
                dbnomics_prefix = "transformed/asset_class=economic/source=dbnomics/provider=" + provider + "/load_type=historical/"
            else:
                dbnomics_prefix = "transformed/asset_class=economic/source=dbnomics/provider=" + provider + "/load_type=daily/"
            
            dbnomics_dfs = read_parquet_files(bucket, dbnomics_prefix, s3)
            for df_temp in dbnomics_dfs:
                df_temp['data_source'] = 'dbnomics'
            all_econ_data.extend(dbnomics_dfs)
    
    # also check flat dbnomics path
    for prefix_type in ['historical', 'daily']:
        if prefix_type == 'historical':
            dbnomics_prefix = "transformed/asset_class=economic/source=dbnomics/load_type=historical/"
        else:
            dbnomics_prefix = "transformed/asset_class=economic/source=dbnomics/load_type=daily/"
        
        dbnomics_dfs = read_parquet_files(bucket, dbnomics_prefix, s3)
        for df_temp in dbnomics_dfs:
            df_temp['data_source'] = 'dbnomics'
        all_econ_data.extend(dbnomics_dfs)
    
    if len(all_econ_data) > 0:
        econ_df = pd.concat(all_econ_data, ignore_index=True)
        print("Total economic records: " + str(len(econ_df)))
        
        indicator_col = 'indicator'
        if 'series_id' in econ_df.columns and 'indicator' not in econ_df.columns:
            indicator_col = 'series_id'
            econ_df = econ_df.rename(columns={'series_id': 'indicator'})
        
        print("Unique indicators found: " + str(econ_df['indicator'].unique().tolist()[:30]))
        
        econ_df = econ_df[econ_df['indicator'].isin(ECON_MAPPING.keys())]
        print("Economic records matching known indicators: " + str(len(econ_df)))
        
        if len(econ_df) > 0:
            econ_df['column_name'] = econ_df['indicator'].map(ECON_MAPPING)
            econ_df['date'] = pd.to_datetime(econ_df['date'])
            
            econ_df = econ_df[econ_df['date'] >= DATA_START_DATE]
            print("Economic records after date filter (>= " + DATA_START_DATE + "): " + str(len(econ_df)))
            
            # prefer FRED over dbnomics for same indicator
            econ_df = econ_df.sort_values(['date', 'indicator', 'data_source']).drop_duplicates(
                subset=['date', 'indicator'], keep='first'
            )
            
            print("Economic records after dedup: " + str(len(econ_df)))
            print("Mapped columns: " + str(econ_df['column_name'].unique().tolist()))
            
            econ_wide = econ_df.pivot_table(
                index='date',
                columns='column_name',
                values='value',
                aggfunc='first'
            ).reset_index()
            
            print("Economic data pivoted: " + str(econ_wide.shape))
            print("Economic columns: " + str(econ_wide.columns.tolist()))
            
            df = pd.merge(market_wide, econ_wide, on='date', how='outer')
        else:
            print("No economic records matched known indicators")
            df = market_wide
    else:
        print("No economic data found")
        df = market_wide
    
    print("Full refresh data loaded: " + str(len(df)) + " rows")

else:
    # append mode: read existing curated + new daily data
    print("=== APPEND MODE ===")
    
    max_wait = 600
    wait_interval = 30
    waited = 0
    daily_path_suffix = "load_type=daily/year=" + year_val + "/month=" + month_val + "/day=" + day_val + "/"
    
    while waited < max_wait:
        found_data = False
        for asset_class in ['indices', 'crypto']:
            prefix = "transformed/asset_class=" + asset_class + "/source=yfinance/" + daily_path_suffix
            try:
                response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
                if response.get('Contents'):
                    found_data = True
                    break
            except:
                pass
        
        if found_data:
            print("Transformed data found, proceeding...")
            break
        else:
            print("Waiting for transformed data... (" + str(waited) + "s)")
            time.sleep(wait_interval)
            waited += wait_interval
    
    curated_key = "curated/macro_market_merged.parquet"
    existing_df = None
    
    try:
        s3_obj = s3.get_object(Bucket=bucket, Key=curated_key)
        existing_df = pd.read_parquet(io.BytesIO(s3_obj['Body'].read()))
        existing_df['date'] = pd.to_datetime(existing_df['date'])
        
        base_cols_present = [c for c in BASE_COLUMNS if c in existing_df.columns]
        existing_df = existing_df[base_cols_present]
        print("Existing curated data: " + str(len(existing_df)) + " rows")
    except Exception as e:
        print("No existing curated file found, will do full refresh: " + str(e))
        load_type = "historical"
    
    if existing_df is not None:
        yfinance_assets = ['stocks', 'indices', 'bonds', 'crypto', 'commodities', 'currencies']
        new_market_data = []
        
        for asset_class in yfinance_assets:
            prefix = "transformed/asset_class=" + asset_class + "/source=yfinance/" + daily_path_suffix
            dfs = read_parquet_files(bucket, prefix, s3)
            new_market_data.extend(dfs)
        
        fred_prefix = "transformed/asset_class=economic/source=fred/" + daily_path_suffix
        new_fred_data = read_parquet_files(bucket, fred_prefix, s3)
        
        new_dbnomics_data = []
        dbnomics_providers = ['bis_data', 'ecb_direct', 'eurostat', 'imf', 'oecd', 'worldbank']
        for provider in dbnomics_providers:
            dbnomics_prefix = "transformed/asset_class=economic/source=dbnomics/provider=" + provider + "/" + daily_path_suffix
            new_dbnomics_data.extend(read_parquet_files(bucket, dbnomics_prefix, s3))
        
        dbnomics_prefix_flat = "transformed/asset_class=economic/source=dbnomics/" + daily_path_suffix
        new_dbnomics_data.extend(read_parquet_files(bucket, dbnomics_prefix_flat, s3))
        
        new_econ_data = new_fred_data + new_dbnomics_data
        
        print("\n--- Reading new CryptoCompare daily data ---")
        cc_wide = read_cryptocompare_data(bucket, s3, 'daily', year_val, month_val, day_val)
        
        if len(new_market_data) == 0:
            print("No new market data found for " + process_date)
            df = existing_df
        else:
            market_df = pd.concat(new_market_data, ignore_index=True)
            market_df = market_df[market_df['ticker'].isin(TICKER_MAPPING.keys())]
            market_df['column_name'] = market_df['ticker'].map(TICKER_MAPPING)
            market_df['date'] = pd.to_datetime(market_df['date'])
            market_df = market_df.drop_duplicates(subset=['date', 'ticker'], keep='last')
            
            new_market_wide = market_df.pivot_table(
                index='date',
                columns='column_name',
                values='close',
                aggfunc='first'
            ).reset_index()
            
            if cc_wide is not None:
                new_market_wide = pd.merge(new_market_wide, cc_wide, on='date', how='outer')
                print("After merging CryptoCompare: " + str(new_market_wide.shape))
            
            if len(new_econ_data) > 0:
                econ_df = pd.concat(new_econ_data, ignore_index=True)
                
                if 'series_id' in econ_df.columns and 'indicator' not in econ_df.columns:
                    econ_df = econ_df.rename(columns={'series_id': 'indicator'})
                
                econ_df = econ_df[econ_df['indicator'].isin(ECON_MAPPING.keys())]
                
                if len(econ_df) > 0:
                    econ_df['column_name'] = econ_df['indicator'].map(ECON_MAPPING)
                    econ_df['date'] = pd.to_datetime(econ_df['date'])
                    econ_df = econ_df.drop_duplicates(subset=['date', 'indicator'], keep='last')
                    
                    new_econ_wide = econ_df.pivot_table(
                        index='date',
                        columns='column_name',
                        values='value',
                        aggfunc='first'
                    ).reset_index()
                    
                    new_data = pd.merge(new_market_wide, new_econ_wide, on='date', how='outer')
                else:
                    new_data = new_market_wide
            else:
                new_data = new_market_wide
            
            print("New data rows: " + str(len(new_data)))
            
            # remove existing rows for same dates (handle reprocessing)
            existing_df = existing_df[~existing_df['date'].isin(new_data['date'])]
            
            df = pd.concat([existing_df, new_data], ignore_index=True)
        
        print("Combined data: " + str(len(df)) + " rows")


# sort, filter weekends, forward-fill

df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date').reset_index(drop=True)
df = df.drop_duplicates(subset=['date'], keep='last')

print("\n--- Filtering weekends ---")
df = filter_weekends(df)

print("\n--- Forward-filling macro indicators ---")
for col_name in FORWARD_FILL_COLUMNS:
    if col_name in df.columns:
        df[col_name] = df[col_name].ffill()


# calculate derived metrics

print("\nCalculating derived metrics...")
df = calculate_metrics(df)


# reorder columns

final_columns = []

for col in BASE_COLUMNS:
    if col in df.columns:
        final_columns.append(col)

for col in df.columns:
    if col not in final_columns:
        final_columns.append(col)

df['date'] = pd.to_datetime(df['date']).dt.date

df = df[final_columns]


# write output

print("\nFinal curated data: " + str(df.shape[0]) + " rows, " + str(df.shape[1]) + " columns")
print("Columns: " + str(df.columns.tolist()))

if len(df) > 0:
    print("Date range: " + str(df['date'].min()) + " to " + str(df['date'].max()))

curated_key = "curated/macro_market_merged.parquet"
buffer = io.BytesIO()
df.to_parquet(buffer, engine='pyarrow', index=False)
buffer.seek(0)
s3.put_object(Bucket=bucket, Key=curated_key, Body=buffer.getvalue())
print("Successfully wrote parquet to: s3://" + bucket + "/" + curated_key)

csv_key = "curated/macro_market_merged.csv"
csv_buffer = io.StringIO()
df.to_csv(csv_buffer, index=False)
s3.put_object(Bucket=bucket, Key=csv_key, Body=csv_buffer.getvalue())
print("Successfully wrote CSV to: s3://" + bucket + "/" + csv_key)

print("\nTransformation complete!")

job.commit()