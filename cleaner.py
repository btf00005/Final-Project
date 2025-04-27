import marimo

__generated_with = "0.11.18"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    import pandas as pd
    return mo, pd, pl, px


@app.cell
def _():
    return


@app.cell
def _(pl):
    exchange_rates = pl.read_csv("data/Exchange_Rates.csv")
    exchange_rates
    return (exchange_rates,)


@app.cell
def _(exchange_rates, pl):
    exchange = exchange_rates.select(pl.all().name.to_lowercase())
    return (exchange,)


@app.cell
def _(exchange, pl):
    rates = exchange.with_columns(pl.col("date").str.to_date("%m/%d/%Y") )
    return (rates,)


@app.cell
def _(pl, rates):
    final_EXrate = rates.with_columns(pl.col("currency").str.strip_chars() ) ##finished
    final_EXrate
    return (final_EXrate,)


@app.cell
def _(final_EXrate):
    final_EXrate.write_parquet("data/cleansed/exchange_rates.parquet")
    return


@app.cell
def _(pl):
    product = pl.read_csv("data/Products.csv")
    product
    return (product,)


@app.cell
def _(pl, product):
    prod_uct = product.select(pl.all().name.to_lowercase())
    return (prod_uct,)


@app.cell
def _(pl, prod_uct):
    prod = prod_uct.with_columns(pl.col("productkey").cast(pl.String) , 
                               pl.col("subcategorykey").cast(pl.String) , 
                               pl.col("categorykey").cast(pl.String) )
    return (prod,)


@app.cell
def _(pl, prod):
    final_product = prod.select(pl.all().str.strip_chars())
    final_product
    return (final_product,)


@app.cell
def _(final_product):
    final_product.write_parquet("data/cleansed/product.parquet")
    return


@app.cell
def _(pl):
    sales = pl.read_csv("data/Sales.csv")
    sales
    return (sales,)


@app.cell
def _(pl, sales):
    buy = sales.select(pl.all().name.to_lowercase() )
    return (buy,)


@app.cell
def _(buy, pl):
    money = buy.with_columns(pl.col("order number" , "line item" , "customerkey" , "storekey" , "productkey").cast(pl.String) , 
                            pl.col("order date" , "delivery date").str.to_date("%m/%d/%Y") )
    return (money,)


@app.cell
def _(money, pl):
    final_sales = money.with_columns(pl.col("order number" , "line item" , "customerkey" , "storekey" , "productkey" , "currency code").str.strip_chars() )
    final_sales
    return (final_sales,)


@app.cell
def _(final_sales):
    final_sales.write_parquet("data/cleansed/sales.parquet")
    return


@app.cell
def _(pl):
    stores = pl.read_csv("data/Stores.csv")
    stores
    return (stores,)


@app.cell
def _(pl, stores):
    sto = stores.select(pl.all().name.to_lowercase() )
    return (sto,)


@app.cell
def _(pl, sto):
    res = sto.with_columns(pl.col("storekey").cast(pl.String) , 
                          pl.col("open date").str.to_date("%m/%d/%Y") )
    return (res,)


@app.cell
def _(pl, res):
    final_stores = res.with_columns(pl.col("storekey" , "country" , "state").str.strip_chars() )
    final_stores
    return (final_stores,)


@app.cell
def _(final_stores):
    final_stores.write_parquet("data/cleansed/stores.parquet")
    return


@app.cell
def _(pl):
    customers = pl.read_csv("data/Customers.csv", schema_overrides={"Zip Code": pl.String}, encoding='iso-8859-1')
    customers
    return (customers,)


@app.cell
def _(customers, pl):
    cust = customers.select(pl.all().name.to_lowercase() )
    return (cust,)


@app.cell
def _(cust, pl):
    omers = cust.with_columns(pl.col("customerkey").cast(pl.String) ,
                             pl.col("birthday").str.to_date("%m/%d/%Y") )
    return (omers,)


@app.cell
def _(omers, pl):
    final_customers = omers.with_columns(pl.col("customerkey" , "gender" , "name" , "city" , "state code" , "state" , 
                                               "zip code" , "country" , "continent").str.strip_chars() )
    final_customers
    return (final_customers,)


@app.cell
def _(final_customers):
    final_customers.write_parquet("data/cleansed/customers.parquet")
    return


@app.cell
def _(pl):
    d_d = pl.read_csv("data/Data_Dictionary.csv")
    d_d
    return (d_d,)


@app.cell
def _(d_d, pl):
    data_dick = d_d.select(pl.all().name.to_lowercase())
    return (data_dick,)


@app.cell
def _(data_dick, pl):
    d_for_d = data_dick.with_columns(pl.all().str.strip_chars())
    d_for_d
    return (d_for_d,)


@app.cell
def _(d_for_d):
    d_for_d.write_parquet("data/cleansed/data_dictionary.parquet")
    return


if __name__ == "__main__":
    app.run()
