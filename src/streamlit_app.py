import streamlit as st  # type: ignore
import requests  # type: ignore
import pandas as pd  # type: ignore
import plotly.express as px  # type: ignore
import os

st.set_page_config(
    page_title="Song Recommendation Dashboard",
    page_icon="🎵",
    layout="wide"
)

st.title("🎵 Song Recommendation Dashboard")

# Get API URL from environment
API_URL = os.getenv('API_URL', 'http://localhost:5000')

# Sidebar
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Choose a page", ["Home", "Recommendations", "Analytics"])

if page == "Home":
    st.header("Welcome to Song Recommendation System")
    st.write("This dashboard provides insights into song recommendations using big data technologies.")

elif page == "Recommendations":
    st.header("Get Song Recommendations")
    user_id = st.text_input("Enter User ID:")
    if st.button("Get Recommendations"):
        if user_id:
            # Call API for recommendations
            st.write(f"Recommendations for User {user_id}")
        else:
            st.warning("Please enter a User ID")

elif page == "Analytics":
    st.header("System Analytics")
    st.write("Analytics and metrics will be displayed here")