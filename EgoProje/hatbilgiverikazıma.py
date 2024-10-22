#Elimizdeki hat numaraları ile ilgili daha fazla bilgi alabilmek adına yeni bir veri kazıma uygulayacağız.

import polars as pl
import pandas as pd
import cloudscraper
from io import StringIO
import numpy as np

veri=pl.read_csv("hamveri.csv")

veri=veri.with_columns([
    pl.col("hat").str.split("-").list.get(0).alias("hat_no"), 
    pl.col("hat").str.split("-").list.get(1).alias("hat_adı")])

veri=veri.drop("hat")

veri=veri.with_columns(
    pl.col("hat_no").str.strip_chars().alias("hat_no"))

veri=veri.with_columns(
    pl.col("hat_no").map_elements(lambda x: f"{x[:3]}-{x[3:]}" if len(x)>3 else x,
                                  return_dtype=pl.Utf8).alias("hat_no"))

uniqhat=veri["hat_no"].unique().to_list()

urls="https://www.ego.gov.tr/hareketsaatleri?hat_no={}&kalkis=&varis="
scraper=cloudscraper.CloudScraper()
df=pd.DataFrame(columns=["hat_no", "Mesafe (Km)", "Süre (Dakika)"])


for hat_no in uniqhat:
    url=urls.format(hat_no)
    req=scraper.get(url)
    veri=StringIO(req.text)

    try:
        tablo=pd.read_html(veri)[0]
        
        hatno=tablo.iloc[1,2]
        mesafe=tablo.iloc[5,2].replace(" km", "")
        sure=tablo.iloc[6,2].replace(" dakika", "")
        
        mesafe=int(mesafe)
        sure=int(sure)
        
        df=df._append({
            "hat_no":hat_no,
            "Mesafe (Km)":mesafe,
            "Süre (Dakika)":sure},ignore_index=True)
    except ValueError:
        df=df._append({
            "hat_no":hat_no,
            "Mesafe (Km)":np.nan,
            "Süre (Dakika)":np.nan},ignore_index=True)

df.to_csv("hatbilgiveri.csv",index=False)