import requests
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf
import fastavro
import pyorc


class ETL:

    years: list[str]

    def __init__(self) -> None:
        self.years = ["2018", "2019", "2020", "2021", "2022", "2023", "2024"]

    def extract_company_data(
        self, symbol: str, start_data: str, date: str
    ) -> pd.DataFrame:
        """
        Extract the data of the company in the S&P 500
        Args:
            symbol: Symbol of the company
            date: Date to extract the data
        Returns:
            data: Data of the company
        """
        data: pd.DataFrame = yf.Ticker(symbol).history(
            period="1d", start=start_data, end=date
        )
        return data

    def extract_sp500_companies(self) -> list[list[str]]:
        """
        Get the list of companies in the S&P 500
        Returns:
            sp500: List of companies in the S&P 500
        """
        url: str = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"class": "wikitable sortable"})
        rows = table.find_all("tr")
        sp500: list = []
        for row in rows[1:]:
            cols = row.find_all("td")
            sp500.append(
                [cols[0].text.strip(), cols[2].text.strip(), cols[6].text.strip()]
            )
        return [i for i in sp500 if i[1] == "Information Technology"]

    def prepare_dfs(self) -> list[pd.DataFrame]:
        """
        Prepare the dataframes of the companies in the S&P 500
        Returns:
            dfs: List of dataframes with the data of the companies in the S&P 500
        """
        sp500: list[list[str]] = self.extract_sp500_companies()
        dfs: list[pd.DataFrame] = []
        for year in self.years:
            data_df: pd.DataFrame = pd.DataFrame(
                {
                    "Date": [],
                    "CIK": [],
                    "Symbol": [],
                    "Open": [],
                    "High": [],
                    "Low": [],
                    "Close": [],
                    "Volume": [],
                    "Dividends": [],
                    "StockSplits": [],
                }
            )
            for company in sp500:
                if year == "2024":
                    data: pd.DataFrame = self.extract_company_data(
                        company[0],
                        f"{year}-01-01",
                        pd.to_datetime("today").strftime("%Y-%m-%d"),
                    ).fillna("ffill")
                else:
                    data: pd.DataFrame = self.extract_company_data(
                        company[0], f"{year}-01-01", f"{int(year)+1}-01-01"
                    )
                aux_df: pd.DataFrame = pd.DataFrame(
                    {
                        "Date": data.index.strftime("%Y-%m-%d"),
                        "CIK": [company[2]] * data.shape[0],
                        "Symbol": [company[0]] * data.shape[0],
                        "Open": data["Open"],
                        "High": data["High"],
                        "Low": data["Low"],
                        "Close": data["Close"],
                        "Volume": data["Volume"],
                        "Dividends": data["Dividends"],
                        "StockSplits": data["Stock Splits"],
                    }
                )
                data_df: pd.DataFrame = pd.concat([data_df, aux_df], ignore_index=True)

            dfs.append(data_df)
        return dfs

    def save_df_as_orc(self, filename: str) -> None:
        """
        Save the dataframe as an orc file. The file must be saved as avro first
        Args:
            df: Dataframe to save
            filename: Name of the file
        """
        avro_file_path: str = f"data/{filename}.avro"
        orc_file_path: str = f"data/{filename}.orc"
        orc_schema: str = (
            "struct<date:string,cik:int,symbol:string,open:double,high:double,low:double,close:double,volume:double,dividends:double,stocksplits:double>"
        )
        # Write the orc file
        with open(orc_file_path, "wb") as f2, open(avro_file_path, "rb") as f:
            reader = fastavro.reader(f)
            writer = pyorc.Writer(f2, orc_schema)
            for record in reader:
                # Writer.write() takes a tuple, but record is a dictionary, therefore we have to convert it to tuple
                writer.write(tuple(record.values()))
            writer.close()

    def get_data_from_csv(self, i: int) -> pd.DataFrame:
        """
        Get the data from the csv file
        """
        data: pd.DataFrame = pd.read_csv(f"data/data_{i}.csv", parse_dates=["Date"])
        return data

    def save_df_as_json(self, df: pd.DataFrame, filename: str) -> None:
        """
        Save the dataframe as a json file
        Args:
            df: Dataframe to save
            filename: Name of the file
        """
        df.to_json(f"data/{filename}.json", date_format="iso", orient="index")

    def save_df_as_csv(self, df: pd.DataFrame, filename: str) -> None:
        """
        Save the dataframe as a csv file
        Args:
            df: Dataframe to save
            filename: Name of the file
        """
        df.to_csv(f"data/{filename}.csv", index=False)

    def save_df_as_parquet(self, df: pd.DataFrame, filename: str) -> None:
        """
        Save the dataframe as a parquet file
        Args:
            df: Dataframe to save
            filename: Name of the file
        """
        df.to_parquet(f"data/{filename}.parquet", engine="pyarrow", index=False)

    def save_df_as_excel(self, df: pd.DataFrame, filename: str) -> None:
        """
        Save the dataframe as an excel file
        Args:
            df: Dataframe to save
            filename: Name of the file
        """
        df.to_excel(f"data/{filename}.xlsx", index=False)

    def save_df_as_avro(self, df: pd.DataFrame, filename: str) -> None:
        """
        Save the dataframe as an avro file
        Args:
            df: Dataframe to save
            filename: Name of the file
        """
        df_formatted: pd.DataFrame = df.copy()
        # Pass it to a dictionary to be able to write it to avro
        data_dict: dict = df_formatted.to_dict(orient="records")
        avro_file_path: str = f"data/{filename}.avro"
        schema: dict = {
            "type": "record",
            "name": f"{filename}",
            "fields": [
                {"name": "Date", "type": "string"},
                {"name": "CIK", "type": "int"},
                {"name": "Symbol", "type": "string"},
                {"name": "Open", "type": "double"},
                {"name": "High", "type": "double"},
                {"name": "Low", "type": "double"},
                {"name": "Close", "type": "double"},
                {"name": "Volume", "type": "double"},
                {"name": "Dividends", "type": "double"},
                {"name": "StockSplits", "type": "double"},
            ],
        }
        # Write the avro file
        with open(avro_file_path, "wb") as f:
            fastavro.writer(f, schema, data_dict)
