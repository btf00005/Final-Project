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
    exchange
    return (exchange,)


@app.cell
def _(exchange, pl):
    rates = exchange.with_columns(pl.col("date").str.to_date("%m/%d/%Y") )
    rates  
    return (rates,)


@app.cell
def _(pl, rates):
    final_EXrate = rates.with_columns(pl.col("currency").str.strip_chars() ) ##finished
    final_EXrate
    return (final_EXrate,)


@app.cell
def _(pl):
    product = pl.read_csv("data/Products.csv")
    product
    return (product,)


@app.cell
def _(pl, product):
    prod_uct = product.select(pl.all().name.to_lowercase())
    prod_uct
    return (prod_uct,)


@app.cell
def _(pl, prod_uct):
    prod = prod_uct.with_columns(pl.col("productkey").cast(pl.String) , 
                               pl.col("subcategorykey").cast(pl.String) , 
                               pl.col("categorykey").cast(pl.String) )

    prod
    return (prod,)


@app.cell
def _(pl, prod):
    final_product = prod.select(pl.all().str.strip_chars())
    final_product
    return (final_product,)


@app.cell
def _(pl):
    sales = pl.read_csv("data/Sales.csv")
    sales
    return (sales,)


@app.cell
def _(pl, sales):
    buy = sales.select(pl.all().name.to_lowercase() )
    buy
    return (buy,)


@app.cell
def _(buy, pl):
    money = buy.with_columns(pl.col("order number" , "line item" , "customerkey" , "storekey" , "productkey").cast(pl.String) , 
                            pl.col("order date" , "delivery date").str.to_date("%m/%d/%Y") )
    money
    return (money,)


@app.cell
def _(money, pl):
    final_sales = money.select(pl.col("order number" , "line item" , "customerkey" , "storekey" , "productkey" , "currency code").str.strip_chars() )
    final_sales
    return (final_sales,)


@app.cell
def _(pl):
    stores = pl.read_csv("data/Stores.csv")
    stores
    return (stores,)


@app.cell
def _(pl, stores):
    sto = stores.select(pl.all().name.to_lowercase() )
    sto
    return (sto,)


@app.cell
def _(pl, sto):
    res = sto.with_columns(pl.col("storekey").cast(pl.String) , 
                          pl.col("open date").str.to_date("%m/%d/%Y") )
    res
    return (res,)


@app.cell
def _(pl, res):
    final_stores = res.with_columns(pl.col("storekey" , "country" , "state").str.strip_chars() )
    final_stores
    return (final_stores,)


@app.cell
def _(pl):
    customers = pl.read_csv("data/Customers.csv", schema_overrides={"Zip Code": pl.String}, encoding='iso-8859-1')
    return (customers,)


if __name__ == "__main__":
    app.run()
