import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

result = pd.DataFrame()

for i in tqdm(range(10000)):
    headers = {
        "Accept": "*/*",
        "Referer": "http://112.137.129.87/qldt/",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    }

    params = {
        "SinhvienLmh[masvTitle]": "2102" + str(i).zfill(4),
        "SinhvienLmh[hotenTitle]": "",
        "SinhvienLmh[ngaysinhTitle]": "",
        "SinhvienLmh[lopkhoahocTitle]": "",
        "SinhvienLmh[tenlopmonhocTitle]": "",
        "SinhvienLmh[tenmonhocTitle]": "",
        "SinhvienLmh[nhom]": "",
        "SinhvienLmh[sotinchiTitle]": "",
        "SinhvienLmh[ghichu]": "",
        "SinhvienLmh[term_id]": "038",
        "SinhvienLmh_page": "1",
        "ajax": "sinhvien-lmh-grid",
    }

    response = requests.get(
        "http://112.137.129.87/qldt/", params=params, headers=headers, verify=False
    )

    soup = BeautifulSoup(response.content, "html.parser")

    results = soup.find("table", class_="items")

    df = pd.read_html(str(results))[0]

    if len(df.index) <= 2:
        continue
    else:
        df = df.drop(df.index[0])
        df = df.drop(df.columns[0], axis=1)
        df[["Mã SV", "Số TC"]] = df[["Mã SV", "Số TC"]].astype("int")
        df["Ngày sinh"] = pd.to_datetime(df["Ngày sinh"], dayfirst=True)

        result = pd.concat([result, df], ignore_index=True)
        # print(df)
    # print(result)
    # results = results.find("tbody")
file_path = f'result/038_2102.parquet'
result.to_parquet(file_path, engine='fastparquet', index=False)
# print(results)
