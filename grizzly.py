import marimo

__generated_with = "0.11.18"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    import polars.selectors as cs
    import marimo as mo
    import re
    return cs, mo, pl, re


@app.cell
def _(pl):
    product = pl.read_csv("data/Products.csv")
    product
    return (product,)


@app.cell
def _(product):
    all_category = product.select("Subcategory" , "Category").unique("Subcategory", maintain_order= True)
    all_category
    return (all_category,)


@app.cell
def _(pl):
    customers = pl.read_csv("data/Customers.csv", schema_overrides={"Zip Code": pl.String}, encoding='iso-8859-1')
    customers
    return (customers,)


@app.cell
def _():
    return


@app.cell
def _(customers):
    everybody = customers.select("City" , "State" , "Country" , "Continent").unique(
        "City" , maintain_order=True
    )
    everybody
    return (everybody,)


@app.cell
def _(customers, pl):
    total = customers.agg(pl.col("Name").sum)
    return (total,)


@app.cell
def _(customers, pl):
    continent_number = customers.select(pl.col("Continent").value_counts())
    continent_number
    return (continent_number,)


@app.cell
def _(customers):
    states = customers.select("State" , "Country" , "Continent").unique(
        "State" , maintain_order=True
    )
    states
    return (states,)


@app.cell
def _(customers):
    country = customers.select("Country" , "Continent").unique(
        "Country" , maintain_order=True)
    country
    return (country,)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
