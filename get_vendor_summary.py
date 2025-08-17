import logging

#  ---- Set up independent logger before importing anything else ----
logger = logging.getLogger("get_vendor_summary")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler("logs/get_vendor_summary.log", mode="a")
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)

# Avoid duplicate logs
if not logger.handlers:
    logger.addHandler(fh)

import sqlite3
import pandas as pd
from ingestion_db import ingest_db


def create_vendor_summary(conn):
    """Merge tables to get vendor summary and add new columns."""
    vendor_sales_summary = pd.read_sql_query(
        """
        WITH freightsummary AS (
            SELECT
                vendornumber,
                SUM(freight) AS freightcost
            FROM vendor_invoice
            GROUP BY VendorNumber
        ),
        purchasesummary AS (
            SELECT 
                p.vendornumber,
                p.vendorname,
                p.brand,
                p.description,
                p.purchaseprice,
                pp.price AS actualprice,
                pp.volume,
                SUM(p.quantity) AS totalpurchasequantity,
                SUM(p.dollars) AS totalpurchasedollars
            FROM purchases p
            JOIN purchase_prices pp
                ON p.brand = pp.brand
            WHERE p.purchaseprice > 0
            GROUP BY p.vendornumber, p.vendorname, p.brand, p.description,
                     p.purchaseprice, pp.price, pp.volume
        ),
        salessummary AS (
            SELECT
                vendorNo,
                Brand,
                SUM(salesquantity) AS totalsalesquantity,
                SUM(salesdollars) AS totalsalesdollars,
                SUM(salesprice) AS totalsalesprice,
                SUM(excisetax) AS totalexcisetax
            FROM sales
            GROUP BY VendorNo, Brand
        )

        SELECT 
            ps.vendornumber,
            ps.vendorname,
            ps.brand,
            ps.description,
            ps.actualprice,
            ps.purchaseprice,
            ps.volume,
            ps.totalpurchasequantity,
            ps.totalpurchasedollars,
            ss.totalsalesquantity,
            ss.totalsalesdollars,
            ss.totalsalesprice,
            ss.totalexcisetax,
            fs.freightcost
        FROM purchasesummary ps
        LEFT JOIN salessummary ss
            ON ps.vendornumber = ss.vendorno
           AND ps.brand = ss.brand
        LEFT JOIN freightsummary fs
            ON ps.vendornumber = fs.vendornumber
        ORDER BY ps.totalpurchasedollars DESC
        """,
        conn
    )
    return vendor_sales_summary


def clean_data(df):
    """Clean and transform vendor summary data."""
    df["volume"] = df["volume"].astype(float)
    df.fillna(0, inplace=True)

    df["vendorname"] = df["vendorname"].str.strip()
    df["description"] = df["description"].str.strip()

    # Feature engineering
    df["grossprofit"] = df["totalsalesdollars"] - df["totalpurchasedollars"]
    df["profitmargin"] = (df["grossprofit"] / df["totalsalesdollars"]) * 100
    df["stockturnover"] = df["totalsalesquantity"] / df["totalpurchasequantity"]
    df["salestopurchaseratio"] = df["totalsalesdollars"] / df["totalpurchasedollars"]

    return df


if __name__ == "__main__":
    # Create DB connection
    conn = sqlite3.connect("inventory.db")

    logger.info("Creating Vendor Summary Table.....")
    summary_df = create_vendor_summary(conn)
    logger.info("\n%s", summary_df.head())

    logger.info("Cleaning Data.....")
    clean_df = clean_data(summary_df)
    logger.info("\n%s", clean_df.head())

    logger.info("Ingesting data.....")
    ingest_db(clean_df, "vendor_sales_summary", conn)

    logger.info("Completed")

    

    logging.info('Cleaning Data.....')
    clean_df=clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data.....')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('Completed')
    