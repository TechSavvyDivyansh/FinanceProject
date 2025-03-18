import streamlit as st
import pandas as pd
import requests
import io

# Set page configuration
st.set_page_config(page_title="CSV Dashboard", layout="wide")


# Sidebar for navigation
st.sidebar.title("Navigation")
tab_selection = st.sidebar.radio("", ["📂 Upload CSV", "📊 Data Preview", "📈 Analysis"])

# Session state to store uploaded CSV
if "csv_data" not in st.session_state:
    st.session_state.csv_data = None

if "uploaded_file" not in st.session_state:
    st.session_state.uploaded_file = None






# Tab 1: Upload CSV
if tab_selection == "📂 Upload CSV":
    st.title("📂 Upload your CSV File")

    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            st.session_state.csv_data = df
            st.session_state.uploaded_file = uploaded_file
            st.success("✅ File uploaded successfully!")
            st.write("Preview of uploaded data:")
            st.dataframe(df.head(), use_container_width=True)

            if st.button("Submit CSV"):
                # Prepare CSV as bytes
                uploaded_file.seek(0)  # Reset file pointer to start
                csv_bytes = uploaded_file.read()
                files = {
                    "file": (uploaded_file.name, io.BytesIO(csv_bytes), "text/csv")
                }

                try:
                    response = requests.post("http://127.0.0.1:5000/upload", files=files)

                    if response.status_code == 200:
                        st.success("✅ CSV submitted to backend successfully!")
                        st.write(response.json())  # Show backend response
                    else:
                        st.error(f"❌ Backend Error: {response.status_code}")
                        st.text(response.text)

                except requests.exceptions.RequestException as e:
                    st.error(f"⚠️ Request failed: {e}")
        except Exception as e:
            st.error(f"⚠️ Error reading CSV: {e}")







# Tab 2: Data Preview
elif tab_selection == "📊 Data Preview":
    st.title("📊 Data Preview")
    if st.session_state.csv_data is not None:
        st.dataframe(st.session_state.csv_data, use_container_width=True)
    else:
        st.warning("No CSV uploaded yet. Please upload from the first tab.")










# Tab 3: Analysis
elif tab_selection == "📈 Analysis":
    st.title("📈 Data Analysis")
    if st.session_state.csv_data is not None:
        numeric_df = st.session_state.csv_data.select_dtypes(include=['float', 'int'])
        if not numeric_df.empty:
            st.write("Bar Chart of First 20 Rows (Numeric Columns)")
            st.bar_chart(numeric_df.head(20))
        else:
            st.info("No numeric data available for plotting.")
    else:
        st.warning("No CSV uploaded yet. Please upload from the first tab.")
