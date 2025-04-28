import marimo

__generated_with = "0.11.18"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import plotly.express as px
    import pandas as pd
    import polars as pl
    import polars.selectors as cs
    import matplotlib.pyplot as plt
    import altair as alt
    import re
    return alt, cs, mo, pd, pl, plt, px, re


@app.cell
def _(pl):
    customer_beta = pl.read_parquet("data/cleansed/customers.parquet")
    customer_beta
    return (customer_beta,)


@app.cell
def _(customer_beta, pl):
    customer = customer_beta.with_columns(pl.when(pl.col("birthday").dt.month() == 1)
                                           .then(pl.lit("january"))
                                           .when(pl.col("birthday").dt.month() ==  2)
                                           .then(pl.lit("febuary"))
                                           .when(pl.col("birthday").dt.month() == 3 )
                                           .then(pl.lit("march")).when(pl.col("birthday").dt.month() == 4 )
                                           .then(pl.lit("april")).when(pl.col("birthday").dt.month() == 5 )
                                           .then(pl.lit("may")).when(pl.col("birthday").dt.month() == 6 )
                                           .then(pl.lit("june")).when(pl.col("birthday").dt.month() == 7 )
                                           .then(pl.lit("july")).when(pl.col("birthday").dt.month() == 8 )
                                           .then(pl.lit("august")).when(pl.col("birthday").dt.month() == 9 )
                                           .then(pl.lit("september")).when(pl.col("birthday").dt.month() == 10 )
                                           .then(pl.lit("october")).when(pl.col("birthday").dt.month() == 11 )
                                           .then(pl.lit("november"))
                                           .otherwise(pl.lit("december"))
                                           .alias("birthmonth") 
                                          )
    customer
    return (customer,)


@app.cell
def _(pl):
    sales = pl.read_parquet("data/cleansed/sales.parquet")
    sales
    return (sales,)


@app.cell
def _(pl):
    products = pl.read_parquet("data/cleansed/product.parquet")
    products
    return (products,)


@app.cell
def _(customer, sales):
    customer_sale = customer.join(sales,
                                 on = "customerkey", 
                                 how = "full")
    customer_sale 
    return (customer_sale,)


@app.cell
def _(customer_sale, pl):
    chimp = customer_sale.select("quantity" , "country").group_by("country").agg(pl.col("quantity").sum()) 
    chimp
    return (chimp,)


@app.cell
def _(alt, chimp):
    alt.Chart(chimp).mark_arc().encode(
            theta=alt.Theta(field="quantity", type="quantitative"),
            color=alt.Color(field="country", type="nominal"),
            tooltip=["country", "quantity"]
        ).properties(
            title="Amount Countries Have Bought",
            width=300,
            height=300)
    return


@app.cell
def _(customer_sale, pl):
    orangatang = customer_sale.select("quantity" , "continent").group_by("continent").agg(pl.col("quantity").sum())
    orangatang
    return (orangatang,)


@app.cell
def _(alt, orangatang):
    alt.Chart(orangatang).mark_arc().encode(  ##too much to be worth anything
            theta=alt.Theta(field="quantity", type="quantitative"),
            color=alt.Color(field="continent", type="nominal"),
            tooltip=["continent", "quantity"]
        ).properties(
            title="Amount Continents Have Bought",
            width=300,
            height=300)
    return


@app.cell
def _(customer_sale, pl):
    gorilla = customer_sale.select("quantity" , "birthmonth").group_by("birthmonth").agg(pl.col("quantity").sum())
    gorilla
    return (gorilla,)


@app.cell
def _(alt, gorilla):
    alt.Chart(gorilla).mark_arc().encode(  ##too much to be worth anything
            theta=alt.Theta(field="quantity", type="quantitative"),
            color=alt.Color(field="birthmonth", type="nominal"),
            tooltip=["birthmonth", "quantity"]
        ).properties(
            title="Amount Bought by BirthMonth",
            width=300,
            height=300)
    return


@app.cell
def _(products, sales):
    product_sales = products.join(sales,
                                 on = "productkey", 
                                 how = "full")
    product_sales
    return (product_sales,)


@app.cell
def _(pl, product_sales):
    ape = product_sales.select("quantity" , "category").group_by("category").agg(pl.col("quantity").sum())
    ape
    return (ape,)


@app.cell
def _(alt, ape):
    alt.Chart(ape).mark_arc().encode(  ##too much to be worth anything
            theta=alt.Theta(field="quantity", type="quantitative"),
            color=alt.Color(field="category", type="nominal"),
            tooltip=["category", "quantity"]
        ).properties(
            title="Amount Categories Have Bought",
            width=300,
            height=300)
    return


@app.cell
def _(pl, product_sales):
    baboon = product_sales.select("quantity" , "subcategory").group_by("subcategory").agg(pl.col("quantity").sum())
    baboon
    return (baboon,)


@app.cell
def _(alt, baboon):
    alt.Chart(baboon).mark_arc().encode(  ##too much to be worth anything
            theta=alt.Theta(field="quantity", type="quantitative"),
            color=alt.Color(field="subcategory", type="nominal"),
            tooltip=["subcategory", "quantity"]
        ).properties(
            title="Amount Subcategories Have Bought",
            width=300,
            height=300)
    return


@app.cell
def _(pl, product_sales):
    simian = product_sales.select("quantity" , "color").group_by("color").agg(pl.col("quantity").sum())
    simian
    return (simian,)


@app.cell
def _(alt, simian):
    alt.Chart(simian).mark_arc().encode(  ##too much to be worth anything
            theta=alt.Theta(field="quantity", type="quantitative"),
            color=alt.Color(field="color", type="nominal"),
            tooltip=["color", "quantity"]
        ).properties(
            title="Amount Colors Have Bought",
            width=300,
            height=300)
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    print("im really gonna do it this time")
    return


if __name__ == "__main__":
    app.run()
