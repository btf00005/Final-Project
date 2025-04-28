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
    return cs, mo, pd, pl, plt, px


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
