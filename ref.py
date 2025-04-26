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

    return (exchange_rates,)


@app.cell
def _(pl):
    product = pl.read_csv("data/Products.csv")
    return (product,)


@app.cell
def _(pl):
    sales = pl.read_csv("data/Sales.csv")
    return (sales,)


@app.cell
def _(pl):
    stores = pl.read_csv("data/Stores.csv")


    return (stores,)


@app.cell
def _(pl):
    customers = pl.read_csv("data/Customers.csv", schema_overrides={"Zip Code": pl.String}, encoding='iso-8859-1')
    return (customers,)


if __name__ == "__main__":
    app.run()
