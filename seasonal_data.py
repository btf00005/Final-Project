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
def _(pl, sales):
    seasonal_sale = sales.with_columns(pl.when(pl.col("order date").dt.month().is_between(3,5))
                                       .then(pl.lit("spring"))
                                       .when(pl.col("order date").dt.month().is_between(6,8))
                                       .then(pl.lit("summer"))
                                       .when(pl.col("order date").dt.month().is_between(9,11))
                                       .then(pl.lit("fall"))
                                       .otherwise(pl.lit("winter"))
                                       .alias("season") 
                                      )
    seasonal_sale
    return (seasonal_sale,)


@app.cell
def _(pl, seasonal_sale):
    seasonal_count = seasonal_sale.with_columns( (pl.col("season") == "winter").sum().alias("frost") , 
                                (pl.col("season") == "spring").sum().alias("rabbit") , 
                                (pl.col("season") == "summer").sum().alias("beach") ,
                                (pl.col("season") == "fall").sum().alias("oktober")

    )
    seasonal_count
    return (seasonal_count,)


@app.cell
def _(pl, seasonal_count):
    season_numbers = seasonal_count.select(pl.col("frost" , "rabbit" , "beach" , "oktober").unique())
    season_numbers
    return (season_numbers,)


@app.cell
def _(pl):
    rape_central = pl.DataFrame({
        "category": ["winter", "spring", "summer", "fall"],  
        "value": [24802, 8741, 14121, 15220]         
    })

    rape_central
    return (rape_central,)


@app.cell
def _(rape_central):
    bar = rape_central.plot.bar(x="category", y="value" , color="category"
    ).properties(
        title="Seasonal Orders",
        width=400,
        height=300)
    bar
    return (bar,)


@app.cell
def _(pl, seasonal_sale):
    fuck = seasonal_sale.select("quantity" , "season").group_by("season").agg(pl.col("quantity").sum())
    fuck
    return (fuck,)


@app.cell
def _(fuck):
    bar_2 = fuck.plot.bar(x="season", y="quantity" , color="season"
    ).properties(
        title="Seasonal Quantity",
        width=400,
        height=300)
    bar_2
    return (bar_2,)


@app.cell
def _(pl, sales):
    sale_year =  sales.with_columns(
        pl.col("order date").dt.year().alias("order year") )

    sale_year
    return (sale_year,)


@app.cell
def _(pl, sale_year):
    kys = sale_year.with_columns(pl.col("order year").cast(pl.String))
    kys
    return (kys,)


@app.cell
def _(kys, pl):
    freaky_gorilla = kys.select("quantity" , "order year").group_by("order year").agg(pl.col("quantity").sum() )
    freaky_gorilla
    return (freaky_gorilla,)


@app.cell
def _(freaky_gorilla):
    freaky_gorilla.plot.bar(x="order year", y="quantity" , color="order year"
    ).properties(
        title="Yearly Quantity",
        width=400,
        height=300)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
