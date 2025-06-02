import streamlit as st
import pandas as pd
import asyncio
from Sign_classification import main  # your async normalization logic

st.title("Software Classification Tool")

uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
if uploaded_file:
    df = pd.read_csv(uploaded_file)
    df.to_csv("input.csv", index=False)

    if st.button("Run Normalization"):
        asyncio.run(main("input.csv", "output.csv"))
        st.success("Done!")
        with open("output.csv", "rb") as f:
            st.download_button("Download Output", f, "normalized_output.csv")
