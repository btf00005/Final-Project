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
    sales = pl.read_csv("data/Sales.csv")
    sales
    return (sales,)


@app.cell
def _(pl, sales):
    delivery_times = sales.filter(pl.col("Delivery Date").is_not_null()).with_columns(
            (pl.col("Delivery Date").str.to_date("%m/%d/%Y") - pl.col("Order Date").str.to_date("%m/%d/%Y"))
            .dt.total_days()
            .alias("Delivery Time (Days)")
        )
    delivery_times
    return (delivery_times,)


@app.cell
def _(delivery_times, pl):
    avg_delivery_time = delivery_times.select(
            pl.col("Delivery Time (Days)").mean().round(2).alias("Average Delivery Time (Days)")
        )
    avg_delivery_time
    return (avg_delivery_time,)


@app.cell
def _(delivery_times, pl):
    delivery_time_by_year = delivery_times.with_columns(
            pl.col("Order Date").str.to_date("%m/%d/%Y").dt.year().alias("Order Year")
        ).group_by("Order Year").agg(
            pl.col("Delivery Time (Days)").mean().round(2).alias("Average Delivery Time (Days)")
        ).sort("Order Year")
    return (delivery_time_by_year,)


@app.cell
def _(alt, delivery_time_by_year):
    alt.Chart(delivery_time_by_year).mark_bar().encode(
            x=alt.X("Order Year:N", title="Year"),
            y=alt.Y("Average Delivery Time (Days):Q", title="Average Delivery Time (Days)"),
            color=alt.Color("Order Year:N", legend=None),
            tooltip=["Order Year", "Average Delivery Time (Days)"]
        ).properties(
            title="Average Delivery Time by Year",
            width=300,
            height=300
        )
    return


@app.cell
def _(pl):
    sales_beta = pl.read_parquet("data/cleansed/sales.parquet")
    sales_beta
    return (sales_beta,)


@app.cell
def _():
    return


@app.cell
def _(pl, sales_beta):
    sales_month = sales_beta.with_columns(pl.when(pl.col("order date").dt.month() == 1)
                                           .then(pl.lit("january"))
                                           .when(pl.col("order date").dt.month() ==  2)
                                           .then(pl.lit("febuary"))
                                           .when(pl.col("order date").dt.month() == 3 )
                                           .then(pl.lit("march")).when(pl.col("order date").dt.month() == 4 )
                                           .then(pl.lit("april")).when(pl.col("order date").dt.month() == 5 )
                                           .then(pl.lit("may")).when(pl.col("order date").dt.month() == 6 )
                                           .then(pl.lit("june")).when(pl.col("order date").dt.month() == 7 )
                                           .then(pl.lit("july")).when(pl.col("order date").dt.month() == 8 )
                                           .then(pl.lit("august")).when(pl.col("order date").dt.month() == 9 )
                                           .then(pl.lit("september")).when(pl.col("order date").dt.month() == 10 )
                                           .then(pl.lit("october")).when(pl.col("order date").dt.month() == 11 )
                                           .then(pl.lit("november"))
                                           .otherwise(pl.lit("december")).alias("order month")
                                    )
    sales_month
    return (sales_month,)


@app.cell
def _(pl, sales_month):
    bear = sales_month.filter(pl.col("delivery date").is_not_null()).with_columns(
            (pl.col("delivery date") - pl.col("order date"))
            .dt.total_days()
            .alias("delivery time (days)")
        )
    bear
    return (bear,)


@app.cell
def _(bear, pl):
    delivery_time_by_month = bear.with_columns(
            pl.col("order month")
        ).group_by("order month").agg(
            pl.col("delivery time (days)").mean().round(2).alias("Average Delivery Time (Days)")
        ).sort("order month")
    delivery_time_by_month
    return (delivery_time_by_month,)


@app.cell
def _(alt, delivery_time_by_month):
    alt.Chart(delivery_time_by_month).mark_bar().encode(
            x=alt.X("order month:N", title="Month"),
            y=alt.Y("Average Delivery Time (Days):Q", title="Average Delivery Time (Days)"),
            color=alt.Color("order month:N", legend=None),
            tooltip=["order month", "Average Delivery Time (Days)"]
        ).properties(
            title="Average Delivery Time by Month",
            width=300,
            height=300
        )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
