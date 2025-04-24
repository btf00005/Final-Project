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
        "State" , maintain_order=True
    )
    everybody
    return (everybody,)


if __name__ == "__main__":
    app.run()
