import cloudscraper
import pandas as pd
import time
from urllib3.exceptions import ProtocolError,InvalidChunkLength
from http.client import RemoteDisconnected
from requests.exceptions import JSONDecodeError,ConnectionError,ChunkedEncodingError


url="https://ulasimistatistik.ankara.bel.tr/Istatistik/DetayDataTable"
scraper=cloudscraper.CloudScraper()

#Örnek çalışma olması açısından çok büyük data yerine 1 senelik veri seti kazıyacağız.
#Veri sayısı aşırı fazla olursa ram sorunu ile karşı karşıya kalabilirsimiz.

basla=pd.to_datetime("01/01/2021",dayfirst=True) 
son=pd.to_datetime("31/12/2021",dayfirst=True) 

tarihlistesi=pd.date_range(start=basla,end=son).strftime("%d/%m/%Y").tolist()

headers={
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://ulasimistatistik.ankara.bel.tr",
        "Referer": "https://ulasimistatistik.ankara.bel.tr/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"}

tumveri=pd.DataFrame()

for i in tarihlistesi:
    while True:
        payload={
            "start":0,
            "length":100000,
            "GenericSearchParams.dateBetween":i}
        try:
            res=scraper.post(url,data=payload,headers=headers)
            veri=pd.DataFrame(res.json()["data"])

            if tumveri.empty:
                tumveri=veri 
            else:
                tumveri=pd.concat([tumveri,veri],ignore_index=True)  
                print(tumveri)
            break
        except (JSONDecodeError,ConnectionError,ProtocolError,RemoteDisconnected,
                InvalidChunkLength,ChunkedEncodingError):
            print(f"Hata!!!  {i} tarihi için 10 saniye sonra tekrar denenecek...")
            time.sleep(10)

tumveri.to_csv("hamveri.csv",index=False)