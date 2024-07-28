import streamlit as st
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType

# Define a function to create a Snowpark session
def create_session():
    connection_parameters = {
        'account': st.secrets["snowflake"]["account"],
        'user': st.secrets["snowflake"]["user"],
        'password': st.secrets["snowflake"]["password"],
        'role': st.secrets["snowflake"]["role"],
        'warehouse': st.secrets["snowflake"]["warehouse"],
        'database': st.secrets["snowflake"]["database"],
        'schema': st.secrets["snowflake"]["schema"]
    }
    session = snowpark.Session.builder.configs(connection_parameters).create()
    return session

# Main function to handle the data operations
def main():
    st.title("Snowflake Snowpark with Streamlit")

    # Create a Snowpark session
    session = create_session()

    # Define the schema for the dummy data
    schema = StructType([
        StructField("language", StringType()),
        StructField("package_name", StringType())
    ])

    # Create a list of dummy data
    data = [
        {"language": "python", "package_name": "numpy"},
        {"language": "python", "package_name": "pandas"},
        {"language": "javascript", "package_name": "react"},
        {"language": "java", "package_name": "spring"},
        {"language": "python", "package_name": "scikit-learn"}
    ]

    # Create a DataFrame from the dummy data
    dataframe = session.create_dataframe(data, schema=schema)

    # Filter the DataFrame to include only rows where language is 'python'
    filtered_dataframe = dataframe.filter(col("language") == 'python')

    # Collect the results to display in Streamlit
    results = filtered_dataframe.collect()

    # Display the results in Streamlit
    st.write("Filtered DataFrame (only Python packages):")
    st.dataframe(results)

    # Close the session
    session.close()

if __name__ == "__main__":
    main()
