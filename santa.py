import marimo

__generated_with = "0.11.18"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    import polars.selectors as cs
    import marimo as mo
    import re
    import altair as alt
    return alt, cs, mo, pl, re


@app.cell
def _(pl):
    sales = pl.read_parquet("data/cleansed/sales.parquet")
    sales
    return (sales,)


@app.cell
def _(pl):
    sales_2 = pl.read_csv("data/Sales.csv")
    sales_2
    return (sales_2,)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
