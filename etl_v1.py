import requests
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf
import fastavro
import pyorc
import sys

def extract():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'wikitable sortable'})
    rows = table.find_all('tr')
    sp500 = []
    for row in rows[1:]:
        cols = row.find_all('td')
        sp500.append([cols[0].text.strip(), cols[2].text.strip(), cols[6].text.strip()])
    return sp500

def transform(sp500):
    df = pd.DataFrame(sp500, columns=['Symbol', 'Sector', 'CIK'])
    df_info_tech = df[df['Sector'] == 'Information Technology'][['Symbol', 'CIK']]
    symbols_list = df_info_tech['Symbol'].values.T.tolist()
    cik_list = df_info_tech['CIK'].values.T.tolist()


    dates = [['2018-01-01', '2019-01-01'], ['2019-01-01', '2020-01-01'], ['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01'], ['2022-01-01', '2023-01-01'], ['2023-01-01', '2024-01-01']]

    datas = []
    for date in dates:
        data_df = pd.DataFrame(
        {'Date': [],
        'CIK': [],
        'Symbol': [],
        'Open': [],
        'High': [],
        'Low': [],
        'Close': [],
        'Volume': [],
        'Dividends': [],
        'Stock Splits': []
        })
        for i in range(len(symbols_list)):
            data = yf.Ticker(symbols_list[i]).history(period="1d", start=date[0], end=date[1])
            aux_df = pd.DataFrame(
                {'Date': data.index,
                'CIK': [cik_list[i]] * data.shape[0],
                'Symbol': [symbols_list[i]] * data.shape[0],
                'Open': data['Open'],
                'High': data['High'],
                'Low': data['Low'],
                'Close': data['Close'],
                'Volume': data['Volume'],
                'Dividends': data['Dividends'],
                'Stock Splits': data['Stock Splits']
                }
            )
            data_df = pd.concat([data_df, aux_df], ignore_index=True)
        datas.append(data_df)
    return datas

def load(datas):
    for i in range(len(datas)):
        datas[i]["Date"] = datas[i]["Date"].dt.tz_localize(None)
        datas[i]["CIK"] = datas[i]["CIK"].astype(int)
        
        # To csv
        datas[i].to_csv(f"data/data_year_{i+1}.csv")
        
        # To json
        datas[i].to_json(f"data/data_year_{i+1}.json")
        
        # To parquet
        datas[i].to_parquet(f"data/data_year_{i+1}.parquet", engine="pyarrow")
        
        # To excel
        datas[i].to_excel(f"data/data_year_{i+1}.xlsx")
        
        # To avro
        data_formatted = datas[i].copy()
        data_formatted["Date"] = data_formatted["Date"].astype(str)
        
        # Pass it to a dictionary to be able to write it to avro
        data_dict = data_formatted.to_dict(orient="records")
        avro_file_path = f"data/data_year_{i+1}.avro"
        orc_file_path = f"data/data_year_{i+1}.orc"
        schema = {
            "type": "record",
            "name": f"data_year_{i+1}",
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
                {"name": "Stock Splits", "type": "double"}
            ]
        }
        
        # Write the avro file
        with open(avro_file_path, "wb") as f:
            fastavro.writer(f, schema, data_dict)
            
        # Write the orc schema
        orc_schema = "struct<Date:string,CIK:int,Symbol:string,Open:double,High:double,Low:double,Close:double,Volume:double,Dividends:double,StockSplits:double>"
        
        # Write the orc file
        with open(orc_file_path, "wb") as f2, open(avro_file_path, "rb") as f:
            reader = fastavro.reader(f)
            writer = pyorc.Writer(f2, orc_schema)
            
            for record in reader:
                # Writer.write() takes a tuple, but record is a dictionary, therefore we have to convert it to tuple
                writer.write(tuple(record.values()))
            
            writer.close()


def main():
    sp500 = extract()
    datas = transform(sp500)
    load(datas)

if __name__ == '__main__':
    main()


