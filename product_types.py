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
    category_counts = product_types.group_by("category").len(name="amount").sort("amount", descending=True)
    return (category_counts,)


@app.cell
def _(category_counts, px):
    px.bar(category_counts, x="category", y="amount", color="category", title="Product Types by Category", labels={"amount": "Number of Products"}, height=600)
    return


@app.cell
def _(product_types):
    subcategory_counts = product_types.group_by("subcategory").len(name="amount").sort("amount", descending=True)
    return (subcategory_counts,)


@app.cell
def _(px, subcategory_counts):
    fig = px.bar(subcategory_counts, x="subcategory", y="amount", color="subcategory", title="Product Types by Subcategory", labels={"amount": "Number of Products"}, height=600)
    fig
    return (fig,)


@app.cell
def _(product_types):
    color_counts = product_types.group_by("color").len(name="amount").sort("amount", descending=True)
    return (color_counts,)


@app.cell
def _(color_counts, px):
    px.bar(color_counts, x="color", y="amount", color="color", title="Product Types by Color", labels={"amount": "Number of Products"}, height=600)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
