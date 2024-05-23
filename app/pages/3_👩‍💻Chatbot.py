from dotenv import load_dotenv
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_community.utilities import SQLDatabase
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from langchain_groq import ChatGroq
import streamlit as st
import os
import time

st.set_page_config(page_title="Chat with Olist", page_icon=":speech_balloon:")

db = None

os.environ["OPENAI_API_KEY"] = (
    "sk-proj-Oloir1lMSnOOES2O8ZSnT3BlbkFJuQiT6i1f8jefuEtJznqt"
)
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_API_KEY"] = "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"


def init_database(user: str, password: str, host: str, database: str) -> SQLDatabase:
    db_uri = (
        f"mysql+mysqlconnector://{user}:admin@{host}/{database}?password={password}"
    )
    return SQLDatabase.from_uri(db_uri)


def get_sql_chain(db):
    template = """
    You are a data analyst at a company. You are interacting with a user who is asking you questions about the company's database.
    Based on the table schema below, write a SQL query that would answer the user's question. Take the conversation history into account.
    
    <SCHEMA>{schema}</SCHEMA>
    
    Conversation History: {chat_history}
    
    Write only the SQL query and nothing else. Do not wrap the SQL query in any other text, not even backticks.
    
    Your turn:
    
    Question: {question}
    SQL Query:
    """

    prompt = ChatPromptTemplate.from_template(template)

    # llm = ChatOpenAI(model="gpt-4o")
    llm = ChatOpenAI(model="gpt-3.5-turbo-0125")

    def get_schema(_):
        return db.get_table_info()

    return (
        RunnablePassthrough.assign(schema=get_schema) | prompt | llm | StrOutputParser()
    )


# add new function
def get_non_db_response(question: str, chat_history: list):
    template = """
    You are an intelligent assistant. A user is asking you a question that might not be related to the database. Provide a helpful response to the user's query.

    Conversation History: {chat_history}

    Question: {question}
    Response:
    """

    prompt = ChatPromptTemplate.from_template(template)
    llm = ChatOpenAI(model="gpt-3.5-turbo-0125")

    chain = (
        RunnablePassthrough.assign(
            question=lambda _: question, chat_history=lambda _: chat_history
        )
        | prompt
        | llm
        | StrOutputParser()
    )

    return chain.invoke(
        {
            "question": question,
            "chat_history": chat_history,
        }
    )


def get_response(user_query: str, db: SQLDatabase, chat_history: list):

    # non_db_template = """
    # Determine if the following question is related to a database or not.
    # Question: {question}
    # Answer with "yes" or "no".
    # """

    # non_db_prompt = ChatPromptTemplate.from_template(non_db_template)
    # llm = ChatOpenAI(model="gpt-3.5-turbo-0125")

    # chain = non_db_prompt | llm | StrOutputParser()
    # is_db_related = (
    #     chain.invoke(
    #         {
    #             "question": user_query,
    #         }
    #     )
    #     .strip()
    #     .lower()
    # )

    # if is_db_related == "no":
    #     return get_non_db_response(user_query, chat_history)

    sql_chain = get_sql_chain(db)

    template = """
    You are a data analyst at a company. You are interacting with a user who is asking you questions about the company's database.
    Based on the table schema below, question, sql query, and sql response, write a natural language response.
    <SCHEMA>{schema}</SCHEMA>

    Conversation History: {chat_history}
    SQL Query: <SQL>{query}</SQL>
    User question: {question}
    SQL Response: {response}"""

    prompt = ChatPromptTemplate.from_template(template)

    llm = ChatOpenAI(model="gpt-3.5-turbo-0125")
    # llm = ChatOpenAI(model="gpt-4o")

    chain = (
        RunnablePassthrough.assign(query=sql_chain).assign(
            schema=lambda _: db.get_table_info(),
            response=lambda vars: db.run(vars["query"]),
        )
        | prompt
        | llm
        | StrOutputParser()
    )

    # return chain.invoke(
    #     {
    #         "question": user_query,
    #         "chat_history": chat_history,
    #     }
    # )
    try:
        return chain.invoke(
            {
                "question": user_query,
                "chat_history": chat_history,
            }
        )
    except Exception as e:
        # return f"An error occurred while processing your query: {e}"
        # return f"The question is not related with database. Please ask new question."
        return get_non_db_response(user_query, chat_history)


if "chat_history" not in st.session_state:
    st.session_state.chat_history = [
        AIMessage(
            content="Hello! I'm a SQL assistant. Ask me anything about your database."
        ),
    ]

load_dotenv()
if not db:
    db = init_database("root", "admin", "mysql", "olist")


st.session_state.db = db

# st.set_page_config(page_title="Chat with Olist", page_icon=":speech_balloon:")

st.title("Chat with Olist")

# with st.sidebar:
#     st.text_input("Host", value="mysql", key="Host")
#     st.text_input("Port", value="3307", key="Port")
#     st.text_input("User", value="root", key="User")
#     st.text_input("Password", type="password", value="admin", key="Password")
#     st.text_input("Database", value="olist", key="Database")

#     if st.button("Connect"):
#         with st.spinner("Connecting to database..."):
#             db = init_database(
#                 st.session_state["User"],
#                 st.session_state["Password"],
#                 st.session_state["Host"],
#                 # st.session_state["Port"],
#                 st.session_state["Database"],
#             )
#             st.session_state.db = db
#             st.success("Connected to database!")

for message in st.session_state.chat_history:
    if isinstance(message, AIMessage):
        with st.chat_message("AI", avatar="🤖"):
            st.markdown(message.content)
    elif isinstance(message, HumanMessage):
        with st.chat_message("Human", avatar="👩‍🎤"):
            st.markdown(message.content)


user_query = st.chat_input("Type a message...")
if user_query is not None and user_query.strip() != "":
    st.session_state.chat_history.append(HumanMessage(content=user_query))

    with st.chat_message("Human", avatar="👩‍🎤"):
        st.markdown(user_query)
    # with st.spinner("Please wait...AI is responding"):
    with st.chat_message("AI", avatar="🤖"):
        response = get_response(
            user_query, st.session_state.db, st.session_state.chat_history
        )
        st.markdown(response)

    st.session_state.chat_history.append(AIMessage(content=response))
