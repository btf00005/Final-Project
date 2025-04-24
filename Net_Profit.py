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
def _(pl):
    sales=pl.read_csv("C:/Users/dcpau/Documents/IENG 331/Final-Project/data/Sales.csv")
    sales
    return (sales,)


@app.cell
def _(pl):
    products=pl.read_csv("C:/Users/dcpau/Documents/IENG 331/Final-Project/data/Products.csv")
    products
    return (products,)


@app.cell
def _(products, sales):
    sales_products = sales.join(products, on="ProductKey")
    sales_products
    return (sales_products,)


@app.cell
def _(pl, sales_products):
    Filtered_sales_products=sales_products.select("Quantity","Unit Cost USD","Unit Price USD", "Subcategory","Category")
    Prices=Filtered_sales_products.with_columns(
            pl.col("Unit Price USD")
            .str.replace_all(r"[^\d.]", "")      
            .cast(pl.Float64)                                                                
            .alias("Unit Price USD"),
             pl.col("Unit Cost USD")
            .str.replace_all(r"[^\d.]", "")      
            .cast(pl.Float64)                                                                
            .alias("Unit Cost USD")
    )
    Prices
    return Filtered_sales_products, Prices


@app.cell
def _(Prices, pl):
    Net_Profit = Prices.with_columns(
        ((pl.col("Unit Price USD") - pl.col("Unit Cost USD")) * pl.col("Quantity")).round(2).alias("Net Profit")
    )
    Net_Profit
    return (Net_Profit,)


@app.cell
def _(Net_Profit, pl):
    Net_Profit_Category = (
        Net_Profit.group_by(["Category"])
        .agg([
            pl.col("Net Profit").sum().round(2).alias("Total Net Profit")
        ])
    )
    Sorted_Net_Profit_Category=Net_Profit_Category.sort("Total Net Profit", descending=True)
    Sorted_Net_Profit_Category
    return Net_Profit_Category, Sorted_Net_Profit_Category


@app.cell
def _(Sorted_Net_Profit_Category, px):
    Sorted_conv = Sorted_Net_Profit_Category.to_pandas()


    fig_cat = px.bar(
        Sorted_conv,
        x="Category",
        y="Total Net Profit",
        color="Category",
        title="Total Net Profit by Category",
        labels={"Total Net Profit": "Net Profit (USD)"},
        height=600
    )
    fig_cat
    return Sorted_conv, fig_cat


@app.cell
def _(Net_Profit, pl):
    Net_Profit_Subcategory = (
        Net_Profit.group_by(["Subcategory"])
        .agg([
            pl.col("Net Profit").sum().round(2).alias("Total Net Profit")
        ])
    )
    Sorted_Net_Profit_Subcategory=Net_Profit_Subcategory.sort("Total Net Profit", descending=True)
    Sorted_Net_Profit_Subcategory
    return Net_Profit_Subcategory, Sorted_Net_Profit_Subcategory


@app.cell
def _(Sorted_Net_Profit_Subcategory, px):
    Sorted_conv_2 = Sorted_Net_Profit_Subcategory.to_pandas()


    fig_subcat = px.bar(
        Sorted_conv_2,
        x="Subcategory",
        y="Total Net Profit",
        color="Subcategory",
        title="Total Net Profit by Category",
        labels={"Total Net Profit": "Net Profit (USD)"},
        height=600
    )
    fig_subcat
    return Sorted_conv_2, fig_subcat


if __name__ == "__main__":
    app.run()
