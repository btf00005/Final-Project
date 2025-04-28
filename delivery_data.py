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
    return (sales,)


@app.cell
def _(pl, sales):
    delivery_times = sales.filter(pl.col("Delivery Date").is_not_null()).with_columns(
            (pl.col("Delivery Date").str.to_date("%m/%d/%Y") - pl.col("Order Date").str.to_date("%m/%d/%Y"))
            .dt.total_days()
            .alias("Delivery Time (Days)")
        )
    return (delivery_times,)


@app.cell
def _(delivery_times, pl):
    avg_delivery_time = delivery_times.select(
            pl.col("Delivery Time (Days)").mean().round(2).alias("Average Delivery Time (Days)")
        )
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


if __name__ == "__main__":
    app.run()
