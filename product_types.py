import marimo

__generated_with = "0.11.18"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return mo, pl, px


@app.cell
def _(pl):
    product_types = pl.read_parquet("data/cleansed/product.parquet")
    product_types
    return (product_types,)


@app.cell
def _(product_types):
    subcategory_counts = product_types.group_by("subcategory").len(name="amount").sort("amount", descending=True)
    return (subcategory_counts,)


@app.cell
def _(px, subcategory_counts):
    fig = px.bar(subcategory_counts, x="subcategory", y="amount", color="subcategory", title="Product Types by Subcategory", labels={"amount": "Number of Products"}, height=600)
    fig

    return (fig,)


if __name__ == "__main__":
    app.run()
