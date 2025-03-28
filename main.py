import streamlit as st
import speech_recognition as sr
from db_handler import execute_query
from ai_generator import get_gemini_response
from schema_handler import load_schema
from googletrans import Translator

st.set_page_config(page_title="Query Wizard", layout="wide", page_icon="logo.png")

st.title("Query Wizard")

# Load Schema (Table Names)
schema = load_schema()
tables = list(schema.keys())

# Sidebar: Show list of tables
selected_table = st.sidebar.selectbox("Select a Table", ["None"] + tables)

def translate_prompt(user_input):
    """
    Translates the user input into English while preserving table names.
    """
    translator = Translator()
    words = user_input.split()
    translated_words = []

    for word in words:
        # Preserve table names by checking against schema
        if any(word.lower() == table.lower() for table in schema.keys()):
            translated_words.append(word)  # Keep table name as is
        else:
            try:
                translated_words.append(translator.translate(word, src='auto', dest='en').text)
            except:
                translated_words.append(word)  # If translation fails, keep original

    return " ".join(translated_words)

# State management for show/hide details
if "show_details" not in st.session_state:
    st.session_state["show_details"] = False  # Default: Hide details

if "generated_sql" not in st.session_state:
    st.session_state["generated_sql"] = ""

if "user_input" not in st.session_state:
    st.session_state["user_input"] = ""

# Show structure of selected table (Field names & data types initially)
if selected_table and selected_table != "None":
    st.sidebar.subheader(f" `{selected_table}`")
    table_columns = schema.get(selected_table, {})

    # Display column names & data types
    for col, details in table_columns.items():
        st.sidebar.write(f"🔹 **{col}** : `{details['type']}`")

    # Toggle buttons for Show More / Hide Details
    if not st.session_state["show_details"]:
        if st.sidebar.button("SHOW MORE"):
            st.session_state["show_details"] = True
            st.rerun()  # Refresh UI
    else:
        st.sidebar.subheader("Details")
        st.sidebar.json(table_columns)  # Show complete structure

        if st.sidebar.button("SHOW LESS"):
            st.session_state["show_details"] = False
            st.rerun()  # Refresh UI

    # Button to display all records from the selected table
    if st.sidebar.button("DISPLAY ALL RECORDS"):
        query = f"SELECT * FROM {selected_table} LIMIT 100;"
        st.session_state["generated_sql"] = query
        execute_query(query)  # Display records on click

# Function to update user input
def update_user_input():
    st.session_state["user_input"] = st.session_state["input_text"]

# Speech-to-Text Function
def speech_to_text():
    recognizer = sr.Recognizer()
    with sr.Microphone() as source:
        st.write("🎙️ Listening... Please speak your query.")
        recognizer.adjust_for_ambient_noise(source, duration=1)  # Adjusts for background noise
        audio = recognizer.listen(source, timeout=None, phrase_time_limit=None)  # Listen indefinitely

    try:
        # Using Google's free API for speech recognition
        text = recognizer.recognize_google(audio)
        st.session_state["user_input"] = text
        st.success(f" You said: {text}")
    except sr.UnknownValueError:
        st.error(" Could not understand the speech.")
    except sr.RequestError as e:
        st.error(f"Could not request results from Google Speech Recognition service; {e}")

# User Input for AI Query Generation
col1, col2 = st.columns([10, 1])
with col1:
    st.subheader("Enter Prompt:")
with col2:
    if st.button("Clear"):
        st.session_state["user_input"] = ""  # Reset stored input
        st.session_state["input_text"] = ""  # Reset text area
        st.session_state["generated_sql"] = ""  # Reset query
        st.rerun()  # Refresh UI to clear input

# Text Area (Linked to Session State)
st.text_area(
    " ",
    key="input_text",  # Different key from session state
    value=st.session_state["user_input"],
    on_change=update_user_input
)

# Button to capture speech input
if st.button("VOICE COMMAND"):
    speech_to_text()  # Capture and display speech input

# Generate SQL Query
if st.button("Generate SQL"):
    if st.session_state["user_input"]:
        # Translate the user input before passing to the AI model
        translated_input = translate_prompt(st.session_state["user_input"])
        sql_query = get_gemini_response(translated_input)  # Generate SQL using translated input
        if sql_query:
            st.session_state["generated_sql"] = sql_query
            st.subheader("Generated SQL Query")
            st.code(sql_query, language='sql')
        else:
            st.error("⚠️ Failed to generate query.")
    else:
        st.warning("⚠️ Please enter a query first.")

# Execute SQL Query
if st.button("Execute SQL"):
    if st.session_state["generated_sql"]:
        execute_query(st.session_state["generated_sql"])
    else:
        st.warning("⚠️ Generate a query first.")
