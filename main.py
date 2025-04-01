import streamlit as st
from query_generator import generate_sql_query
from sf_utils import execute_sql_query
from response_generator import generate_human_response


user_question = st.text_input("Ask a business-related question:")

if st.button("Get Answer"):
    if user_question:
        # Step 1: Convert question to SQL
        sql_query = generate_sql_query(user_question)
        st.write(f"🔍 Generated SQL: `{sql_query}`")
        
        # Step 2: Execute SQL in Snowflake
        sql_result = execute_sql_query(sql_query)
        
        # Step 3: Generate human-like response
        response = generate_human_response(sql_result, user_question)
        
        # Step 4: Show response to user
        st.success(response)
