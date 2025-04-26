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
def _(customers):
    continent_number = customers.group_by("Continent").len(name = "amount")
    best_continent = continent_number.top_k(5, by = "amount")
    best_continent
    return best_continent, continent_number


@app.cell
def _(alt, continent_number):
    alt.Chart(continent_number).mark_arc().encode(
        theta=alt.Theta(field="amount", type="quantitative"),
        color=alt.Color(field="Continent", type="nominal"),
        tooltip=["Continent", "amount"]
    ).properties(
        title="Continents who buy from us",
        width=300,
        height=300)
    return


@app.cell
def _(customers):
    country_number = customers.group_by("Country").len(name = "amount")
    best_country = country_number.top_k(5, by = "amount")
    best_country
    return best_country, country_number


@app.cell
def _(alt, country_number):
    alt.Chart(country_number).mark_arc().encode(
        theta=alt.Theta(field="amount", type="quantitative"),
        color=alt.Color(field="Country", type="nominal"),
        tooltip=["Country", "amount"]
    ).properties(
        title="Countries who buy from us",
        width=300,
        height=300)
    return


@app.cell
def _(customers):
    state_number = customers.group_by("State").len(name = "amount")
    best_state = state_number.top_k(5, by = "amount")
    best_state
    return best_state, state_number


@app.cell
def _(alt, state_number):
    alt.Chart(state_number).mark_arc().encode(
        theta=alt.Theta(field="amount", type="quantitative"),
        color=alt.Color(field="State", type="nominal"),
        tooltip=["State", "amount"]
    ).properties(
        title="States who buy from us",
        width=300,
        height=300)
    return


@app.cell
def _(customers):
    city_number = customers.group_by("City").len(name = "amount")
    best_city = city_number.top_k(5, by = "amount")
    best_city
    return best_city, city_number


@app.cell
def _(alt, city_number):
    alt.Chart(city_number).mark_arc().encode(
        theta=alt.Theta(field="amount", type="quantitative"),
        color=alt.Color(field="City", type="nominal"),
        tooltip=["City", "amount"]
    ).properties(
        title="Cities who buy from us",
        width=300,
        height=300)
    return


if __name__ == "__main__":
    app.run()
