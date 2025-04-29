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
    sales=pl.read_csv("data/Sales.csv")
    sales
    return (sales,)


@app.cell
def _(pl):
    products=pl.read_csv("data/Products.csv")
    products
    return (products,)


@app.cell
def _(pl):
    stores=pl.read_csv("data/Stores.csv")
    stores
    return (stores,)


@app.cell(hide_code=True)
def _(products, sales):
    sales_products = sales.join(products, on="ProductKey")
    sales_products
    return (sales_products,)


@app.cell
def _(pl, sales_products):
    Online_Stores = sales_products.with_columns([
        pl.when(pl.col("StoreKey") == 0)
          .then("StoreKey")
          .alias("Online"),

        pl.when(pl.col("StoreKey") != 0)
          .then("StoreKey")
          .alias("Store")
    ])
    Online_Stores
    return (Online_Stores,)


@app.cell
def _(Online_Stores):
    Filtered_Online_Stores=Online_Stores.select("Order Number","Quantity","Unit Cost USD","Unit Price USD","Online","Store")
    Filtered_Online_Stores
    return (Filtered_Online_Stores,)


@app.cell
def _(Filtered_Online_Stores, pl):
    Price=Filtered_Online_Stores.with_columns(
        pl.col("Unit Price USD")
        .str.replace_all(r"[^\d.]", "")      
        .cast(pl.Float64)                                                                
        .alias("Unit Price USD"),
         pl.col("Unit Cost USD")
        .str.replace_all(r"[^\d.]", "")      
        .cast(pl.Float64)                                                                
        .alias("Unit Cost USD")
    )
    Price
    return (Price,)


@app.cell
def _(Price, pl):
    Total_Price=Price.with_columns(
        (pl.col("Unit Price USD") * pl.col("Quantity")).round(2).alias("Total Price")
    )

    Total_Price
    return (Total_Price,)


@app.cell
def _(Total_Price, pl):
    average_order_value = (
        Total_Price.filter(pl.col("Online") == 0)
          .select(pl.col("Total Price").mean().alias("Avg Online Order Price")),
        Total_Price.filter(pl.col("Store") != 1)
          .select(pl.col("Total Price").mean().alias("Avg Store Order Price "))
    )
    average_order_value
    return (average_order_value,)


@app.cell
def _(Total_Price, pl):
    store_avg = (
        Total_Price.filter(pl.col("Store") != 1)
          .select(pl.col("Total Price").mean().alias("Avg Store Order "))
    )
    store_avg
    return (store_avg,)


@app.cell
def _(Total_Price, pl):
    counts=Total_Price.with_columns(
        pl.col("Online").count().alias("Online Count"),
        pl.col("Store").count().alias("Store Count"),

    )

    counts
    return (counts,)


@app.cell
def _(counts):
    Order_counts=counts.select("Online Count","Store Count")
    Order_counts
    return (Order_counts,)


@app.cell
def _(pl):
    grizzly_territory = pl.DataFrame({
            "place": ["online" , "store"], 
            "value": [13165 , 49719] })
    
    return (grizzly_territory,)


@app.cell
def _(grizzly_territory):
    grizzly_territory.plot.bar(x="place", y="value" , color="place"
        ).properties(
            title="Order Placement",
            width=400,
            height=300)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
