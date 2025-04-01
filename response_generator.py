from langchain.chat_models import ChatOpenAI

def generate_human_response(sql_result, user_question):
    """Convert raw SQL result into a natural language response"""
    llm = ChatOpenAI(model_name="gpt-4")

    prompt = f"""
    You are an AI that explains SQL query results in human-like responses.
    
    User Question: "{user_question}"
    SQL Result: {sql_result}
    
    Provide a clear, professional, and user-friendly response.
    """
    
    response = llm.predict(prompt)
    return response
