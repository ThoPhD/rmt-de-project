import os
import time
from typing import List, Tuple

import pandas as pd
import requests

BASE_URL = "https://api.binance.com"


class TransactionsMetadata:
    def __init__(self, transactions_path: str):
        self.transactions_path = transactions_path
        self.df = pd.read_csv(transactions_path)

    def get_symbols(self) -> List[str]:
        """
        Get all symbols related to USDT.
        """
        df_usdt = self.df[
            (self.df["source_currency"].str.upper() == "USDT") |
            (self.df["destination_currency"].str.upper() == "USDT")
            ]

        currencies_to_check = pd.concat([
            df_usdt["source_currency"],
            df_usdt["destination_currency"]
        ]).dropna().unique()

        symbols_set = {
            f"{currency.upper()}USDT"
            for currency in currencies_to_check
            if currency.upper() != "USDT"
        }

        return list(symbols_set)

    def get_date_range(self) -> Tuple[int, int]:
        self.df["created_at"] = pd.to_datetime(self.df["created_at"])
        start = int(self.df["created_at"].min().timestamp() * 1000)
        end = int(self.df["created_at"].max().timestamp() * 1000)

        return start, end


class BinanceAPIClient:
    def __init__(self, interval="1h", limit=1000, sleep=0.5):
        self.interval = interval
        self.limit = limit
        self.sleep = sleep

    def fetch_klines(self, symbol: str, start_time: int, end_time: int) -> List[list]:
        """
        Get Klines from Binance API.
        """
        klines_url = os.path.join(BASE_URL, "api/v3/klines")
        all_klines = []
        current_start = start_time

        while True:
            params = {
                "symbol": symbol,
                "interval": self.interval,
                "startTime": current_start,
                "endTime": end_time,
                "limit": self.limit
            }

            response = requests.get(klines_url, params=params)

            if response.status_code != 200:
                print(f"Error fetching {symbol}: {response.text}")
                if response.status_code >= 500:
                    time.sleep(self.sleep * 2)
                    continue
                break

            data = response.json()
            if not data:
                break

            all_klines.extend(data)

            last_close_time = data[-1][6]
            current_start = last_close_time + 1

            if last_close_time >= end_time:
                break

            time.sleep(self.sleep)

        return all_klines


class RatesIngestionPipeline:
    def __init__(self, tx_path: str, output_dir: str = "output/raw_rates"):
        self.tx_metadata = TransactionsMetadata(tx_path)
        self.api_client = BinanceAPIClient()
        self.output_dir = output_dir

        os.makedirs(self.output_dir, exist_ok=True)

    def run(self):
        symbols = self.tx_metadata.get_symbols()
        start, end = self.tx_metadata.get_date_range()

        output_file = os.path.join(self.output_dir, "binance_rates.csv")
        first_write = True
        total_records_saved = 0

        print(f"Date range ms: {start} → {end}")

        iso_format = lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') + f".{x.microsecond // 1000:03d}Z"

        for symbol in symbols:
            print(f"Fetching rates for symbol: {symbol}")

            klines = self.api_client.fetch_klines(symbol, start, end)

            if not klines:
                print(f"No data for {symbol}, skipping.")
                continue

            df = pd.DataFrame(klines, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "num_trades",
                "taker_buy_base", "taker_buy_quote", "ignore"
            ])

            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
            df["open_time"] = df["open_time"].apply(iso_format)
            df["close_time"] = df["close_time"].apply(iso_format)

            df["symbol"] = symbol
            cols = ["symbol"] + [col for col in df.columns if col != "symbol"]
            df = df[cols]

            df.to_csv(
                output_file,
                mode='a',
                header=first_write,
                index=False
            )

            if first_write:
                first_write = False

            print(f"Processed and appended {len(df)} records for {symbol}.")
            total_records_saved += len(df)

        print(f"Successfully saved {total_records_saved} records.")


if __name__ == "__main__":
    pipeline = RatesIngestionPipeline(
        tx_path="data/transactions.csv",
        output_dir="data/output/raw_rates"
    )
    pipeline.run()
