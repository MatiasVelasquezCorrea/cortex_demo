"""
Cortex Analyst App
====================
This app allows users to interact with their data using natural language.
"""

import json  # Para manejar datos en formato JSON
import time
from typing import Dict, List, Optional, Tuple # Para especificar tipos de datos

import _snowflake  # Para interactuar con APIs específicas de Snowflake
import pandas as pd # Manipular y analizar datos
import streamlit as st  # Biblioteca Streamlit para construir la aplicación web
from snowflake.snowpark.context import (
    get_active_session,
)  # Para interactuar con sesiones de Snowflake
from snowflake.snowpark.exceptions import SnowparkSQLException # Para manejar excepciones específicas de Snowpark

# Lista de rutas disponibles para modelos semánticos en el formato: <DATABASE>.<SCHEMA>.<STAGE>/<FILE-NAME>
# Cada ruta apunta a un archivo YAML que define un modelo semántico
AVAILABLE_SEMANTIC_MODELS_PATHS = [
    #"CORTEX_ANALYST_DEMO.REVENUE_TIMESERIES.RAW_DATA/revenue_timeseries.yaml", # Demo
    "IA_CORTEX.CORTEX_SCHEMA.RAW_DATA/elden_modelo_semantico.yaml", # Elden Rings Weapons
    #"CORTEX_ANALYST_DEMO.REVENUE_TIMESERIES.RAW_DATA/covid19.yaml" # Muertes Covid19
]
API_ENDPOINT = "/api/v2/cortex/analyst/message"
API_TIMEOUT = 50000  # en milisegundos

# Inicializar una sesión de Snowpark para ejecutar consultas
session = get_active_session()


def main():
    # Inicializar el estado de la sesión
    if "messages" not in st.session_state:
        reset_session_state() # Inicializar historial de mensajes
    show_header_and_sidebar() # Muestra header y barra lateral, selector para elegir el modelo semantico
    if len(st.session_state.messages) == 0: # si no hay mensaje, se solicita ingresar pregunta
        process_user_input("¿Que pregunta quieres hacer?")
    display_conversation() # Mostrar conversacion
    handle_user_inputs()
    handle_error_notifications()


def reset_session_state(): # Resetea el estado de la sesión, borrando los mensajes previos y la sugerencia activa
    """Reset important session state elements."""
    st.session_state.messages = []  # Lista para almacenar mensajes de la conversación
    st.session_state.active_suggestion = None  # Sugerencia seleccionada actualmente


def show_header_and_sidebar():
    """Display the header and sidebar of the app."""
    # Establecer el título y texto introductorio de la aplicación
    st.title("Cortex Analyst")
    st.markdown(
        "¡Bienvenido a Cortex Analyst! Escribe tus preguntas a continuación para interactuar con tus datos. "
    )

    # Barra lateral con un botón de reinicio
    with st.sidebar:
        st.selectbox(
            "Modelo Semantico seleccionado:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )
        st.divider()
        # Centrar este botón
        _, btn_container, _ = st.columns([2, 6, 2])
        if btn_container.button("Limpiar historial de chat", use_container_width=True):
            reset_session_state()


def handle_user_inputs(): # Manejar la entrada del usuario desde un cuadro de texto
    """Handle user inputs from the chat interface."""
    # Manejar entrada de chat
    user_input = st.chat_input("¿Cual es tu pregunta?")
    if user_input: # Verificar si usuario ingreso algo
        process_user_input(user_input)
    # Manejar clic en pregunta sugerida
    elif st.session_state.active_suggestion is not None: # Si usuario elige una de las preguntas sugeridas
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion) # Procesa la sugerencia como pregunta


def handle_error_notifications(): # Mostrar error de API
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occured!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False


def process_user_input(prompt: str):
    """
    Process user input and update the conversation history.

    Args:
        prompt (str): The user's input.
    """

    # Crear un nuevo mensaje, agregar al historial y mostrar inmediatamente
    # Crear diccionario que representa el mensaje del usuario
    new_user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}], # Contiene lista con diccionario que describe tipo de mensaje y el texto
    }
    st.session_state.messages.append(new_user_message) # Se agrega al historial de la conversacion
    with st.chat_message("user"): # Mostrar mensaje en la interfaz con el rol usuario
        user_msg_index = len(st.session_state.messages) - 1 # Devuelve total de mensajes, -1 es para obtener el ultimo mensaje
        display_message(new_user_message["content"], user_msg_index) # Se muestra en la interfaz

    # Mostrar indicador de progreso mientras se espera la respuesta
    with st.chat_message("analyst"): # Mensaje proviene de analista
        with st.spinner("Esperando la respuesta del Analista..."): # Indicador de carga
            time.sleep(1) # 1 segundo de espera
            response, error_msg = get_analyst_response(st.session_state.messages) # Devuelve mensaje de analista
            if error_msg is None: # Si no hay error se muestra respuesta de analista
                analyst_message = {
                    "role": "analyst",
                    "content": response["message"]["content"],
                    "request_id": response["request_id"],
                }
            else:
                analyst_message = { # Si hay error se muestra respuesta de analista como texto
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_msg}],
                    "request_id": response["request_id"],
                }
                st.session_state["fire_API_error_notify"] = True
            st.session_state.messages.append(analyst_message) # Se agrega al historial
            st.rerun() # Recargar app para mostrar nueva respuesta


def get_analyst_response(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    """
    Send chat history to the Cortex Analyst API and return the response.

    Args:
        messages (List[Dict]): The conversation history.

    Returns:
        Optional[Dict]: The response from the Cortex Analyst API.
    """
    # Preparar el cuerpo de la solicitud con el prompt del usuario
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }

    # Enviar una solicitud POST a la API de Cortex Analyst
    # Ajustado para usar argumentos posicionales según el requerimiento de la API
    resp = _snowflake.send_snow_api_request(
        "POST",  # method
        API_ENDPOINT,  # path
        {},  # headers
        {},  # params
        request_body,  # body
        None,  # request_guid
        API_TIMEOUT,  # timeout in milliseconds
    )

    # El contenido es un string con un objeto JSON serializado
    parsed_content = json.loads(resp["content"])

    # Verificar si la respuesta fue exitosa
    if resp["status"] < 400:
        # Return the content of the response as a JSON object
        return parsed_content, None
    else:
        # Crear un mensaje de error legible
        error_msg = f"""
🚨 An Analyst API error has occurred 🚨

* response code: `{resp['status']}`
* request-id: `{parsed_content['request_id']}`
* error code: `{parsed_content['error_code']}`

Message:
```
{parsed_content['message']}
```
        """
        return parsed_content, error_msg


def display_conversation():
    """
    Display the conversation history between the user and the assistant.
    """
    for idx, message in enumerate(st.session_state.messages): # Recorrer todos los mensajes almacenados
        role = message["role"]
        content = message["content"]
        with st.chat_message(role): # Mostrar mensaje en la interfaz
            display_message(content, idx)


def display_message(content: List[Dict[str, str]], message_index: int):
    """
    Display a single message content.

    Args:
        content (List[Dict[str, str]]): The message content.
        message_index (int): The index of the message.
    """
    for item in content: # Ciclo que recorre cada contenido del mensaje, content es una lista de diccionarios
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            # Mostrar sugerencias como botones
            for suggestion_index, suggestion in enumerate(item["suggestions"]):
                if st.button(
                    suggestion, key=f"suggestion_{message_index}_{suggestion_index}"
                ):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            # Mostrar la consulta SQL y los resultados
            display_sql_query(item["statement"], message_index)
        else:
            # Manejar otros tipos de contenido si es necesario
            pass


@st.cache_data(show_spinner=False) # Funcion que se encarga de ejecutar la consulta SQL y convierte los resultados a un Dataframe de Pandas
def get_query_exec_result(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    # Tupla con dos elementos:
    # Si la consulta es exitos, este valor sera un dataframe de pandas
    # Si ocurre un error durante la ejecucion, este valor sera un mensaje de error
    """
    Execute the SQL query and convert the results to a pandas DataFrame.

    Args:
        query (str): The SQL query.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: The query results and the error message.
    """
    global session # Sesion de Snowflake 
    try:
        df = session.sql(query).to_pandas() # Utilizar session para ejecutar consulta SQL
        return df, None # Devuelve el DataFrame con los resultados de la consulta
    except SnowparkSQLException as e:
        return None, str(e)


def display_sql_query(sql: str, message_index: int): # Mostrar la consulta SQL, ejecutar la consulta para obtener datos y presentar los resultados en un formato claro y visual
    """
    Executes the SQL query and displays the results in form of data frame and charts.

    Args:
        sql (str): The SQL query.
        message_index (int): The index of the message.
    """

    # Mostrar la consulta SQL
    # WITH asegura que los componentes de Streamlit (como spinners, expanders y otros) sean correctamente inicializados y gestionados
    with st.expander("SQL Query", expanded=False): # Expander para ver la consulta SQL
        st.code(sql, language="sql")

    # Mostrar los resultados de la consulta SQL
    with st.expander("Resultados", expanded=True):
        with st.spinner("Corriendo consulta SQL..."):
            df, err_msg = get_query_exec_result(sql) # Llama a una función que ejecuta la consulta SQL
            if df is None:
                st.error(f"Could not execute generated SQL query. Error: {err_msg}")
                return

            if df.empty:
                st.write("Query returned no data")
                return

            # Mostrar resultados de consulta en dos pestañas
            data_tab, chart_tab = st.tabs(["Datos 📄", "Grafico 📈 "])
            with data_tab:
                st.dataframe(df, use_container_width=True)

            with chart_tab:
                display_charts_tab(df, message_index)


def display_charts_tab(df: pd.DataFrame, message_index: int) -> None: # Mostrar pestaña con graficos
    """
    Display the charts tab.

    Args:
        df (pd.DataFrame): The query results.
        message_index (int): The index of the message.
    """
    # Debe haber al menos 2 columnas para dibujar gráficos
    if len(df.columns) >= 2:
        all_cols_set = set(df.columns) # Se crea un conjunto de todas las columnas disponibles en el DataFrame
        col1, col2 = st.columns(2) # Divide la interfaz en dos columnas para que las opciones del eje X y el eje Y aparezcan lado a lado
        x_col = col1.selectbox(
            "Eje X", all_cols_set, key=f"x_col_select_{message_index}"
        )
        y_col = col2.selectbox(
            "Eje Y",
            all_cols_set.difference({x_col}),
            key=f"y_col_select_{message_index}",
        )
        chart_type = st.selectbox( # Usuario selecciona Lineal o Barras
            "Select chart type",
            options=["Grafico Lineal 📈", "Grafico Barras 📊"],
            key=f"chart_type_{message_index}", # Asegura que múltiples gráficos puedan coexistir sin interferencias
        )
        if chart_type == "Grafico Lineal 📈":
            st.line_chart(df.set_index(x_col)[y_col])
        elif chart_type == "Grafico Barras 📊":
            st.bar_chart(df.set_index(x_col)[y_col])
    else:
        st.write("Al menos 2 columnas requeridas")

# Estructura que asegura que cierto código solo se ejecute cuando el archivo actual es ejecutado
# directamente como un programa principal, y no cuando es importado como un módulo en otro script
if __name__ == "__main__":
    main()

