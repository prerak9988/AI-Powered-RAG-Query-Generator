import openai
from langchain.chat_models import ChatOpenAI

def generate_sql_query(user_question):
    """Convert user input into an SQL query"""
    llm = ChatOpenAI(model_name="gpt-3")
    
    prompt = f"""
    You are an AI that converts natural language queries into Snowflake SQL queries.
    Here are the table details:
    - Table: PRODUCTS_D (Columns: PRODUCT_ID, LEAD_TIME, BU, CATEGORY)
    
    Convert the following question into a SQL query:
    "{user_question}"
    """
    
    sql_query = llm.predict(prompt)
    return sql_query.strip()

#In the actual Scenario we added the complete meta data and schema of our database so LLM can process which table to query from
#Also, we had to manually train the model on a lot of scenarios teaching which table to consider 

