import logging
from datetime import datetime, timedelta
import pandas as pd
from bertopic import BERTopic
from src.predize_utils import fetch_api_order_ids, convert_tickets_to_df
from src.predize import Predize

# Configuración del logger
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("pipeline_logs.log"),  # Guarda en un archivo
        logging.StreamHandler()  # Imprime en la consola
    ]
)

def paso_1_crear_instancia():
    """
    Crea una instancia de Predize.
    """
    try:
        predize_creds = {
            'email': 'nocnoc@predize.com',
            'password': 'nocnocapi*'
        }
        predize_instance = Predize(
            email=predize_creds['email'],
            password=predize_creds['password']
        )
        logging.info("Instancia creada exitosamente.")
        return predize_instance
    except Exception as e:
        logging.error(f"Error al crear instancia de Predize: {e}")
        raise

def paso_2_obtener_tickets(predize_instance):
    """
    Obtiene los tickets de los últimos 15 minutos.
    """
    try:
        now = datetime.utcnow()
        fifteen_minutes_ago = now - timedelta(minutes=15)

        tickets_response = predize_instance.get_tickets(
            last_message_to=fifteen_minutes_ago.isoformat() + "Z",
            last_message_from=now.isoformat() + "Z"
        )
        tickets = tickets_response.get('items', [])

        if not tickets:
            logging.warning("No hay tickets para procesar.")
            return pd.DataFrame()

        df_tickets = convert_tickets_to_df(tickets)
        df_tickets['id'] = df_tickets['id'].astype(str)
        logging.info(f"Tickets obtenidos: {len(df_tickets)}")
        return df_tickets
    except Exception as e:
        logging.error(f"Error al obtener tickets: {e}")
        raise

def paso_3_filtrar_por_tipo(df_tickets):
    """
    Filtra los tickets para dejar solo los que tienen type='POST_ORDER'.
    """
    try:
        df_filtered = df_tickets[df_tickets['type'] == 'POST_ORDER']
        if df_filtered.empty:
            logging.warning("No hay tickets con type='POST_ORDER'.")
        else:
            logging.info(f"Tickets filtrados por type='POST_ORDER': {len(df_filtered)}")
        return df_filtered
    except Exception as e:
        logging.error(f"Error al filtrar tickets por tipo: {e}")
        raise

def paso_4_obtener_mensajes(predize_instance, df_tickets):
    """
    Obtiene los mensajes asociados a los tickets.
    """
    try:
        ticket_ids = df_tickets['id'].tolist()
        last_messages = predize_instance.get_last_message_for_tickets(ticket_ids)

        df_messages = pd.DataFrame.from_dict(last_messages, orient='index').reset_index()
        df_messages.rename(columns={'index': 'ticket_id'}, inplace=True)

        df_combined = df_tickets.merge(df_messages, left_on='id', right_on='ticket_id', how='left', suffixes=('', '_message'))
        logging.info(f"Mensajes obtenidos y combinados: {len(df_messages)}")
        return df_combined
    except Exception as e:
        logging.error(f"Error al obtener mensajes: {e}")
        raise

def paso_5_filtrar_por_seller(df_combined):
    """
    Filtra el DataFrame combinado para dejar solo los mensajes con seller=false.
    """
    try:
        if 'seller' in df_combined.columns:
            df_filtered = df_combined[df_combined['seller'] == False]
            logging.info(f"Mensajes filtrados por seller=false: {len(df_filtered)}")
        else:
            logging.warning("La columna 'seller' no está presente. No se aplicó el filtro.")
            df_filtered = df_combined
        return df_filtered
    except Exception as e:
        logging.error(f"Error al filtrar mensajes por seller: {e}")
        raise

def paso_6_filtrar_por_channel(df_combined):
    """
    Filtra el DataFrame combinado para dejar solo los mensajes con channelAccount_channel='mercadolivre'.
    """
    try:
        if 'channelAccount_channel' in df_combined.columns:
            df_filtered = df_combined[df_combined['channelAccount_channel'] == 'mercadolivre']
            logging.info(f"Mensajes filtrados por channelAccount_channel='mercadolivre': {len(df_filtered)}")
        else:
            logging.warning("La columna 'channelAccount_channel' no está presente. No se aplicó el filtro.")
            df_filtered = df_combined
        return df_filtered
    except Exception as e:
        logging.error(f"Error al filtrar mensajes por channelAccount_channel: {e}")
        raise

def paso_7_simplificar_dataframe(df_combined):
    """
    Simplifica el DataFrame combinado para incluir solo ticket_id, message y order_id.
    """
    try:
        columnas_necesarias = ['ticket_id', 'message', 'order_id']
        df_simplificado = df_combined[columnas_necesarias]
        logging.info(f"DataFrame simplificado: {len(df_simplificado)} filas.")
        return df_simplificado
    except Exception as e:
        logging.error(f"Error al simplificar el DataFrame: {e}")
        raise

def paso_8_cargar_modelo_bertopic():
    """
    Carga el modelo BERTopic desde la ruta especificada.
    """
    try:
        ruta_modelo = 'bertopic_model_post'
        modelo = BERTopic.load(ruta_modelo)
        logging.info("Modelo BERTopic cargado.")
        return modelo
    except Exception as e:
        logging.error(f"Error al cargar el modelo BERTopic: {e}")
        raise

def paso_9_asignar_topics(df_simplificado, modelo_bertopic):
    """
    Pasa los mensajes por el modelo BERTopic y asigna topic_number y probabilidad.
    """
    try:
        mensajes = df_simplificado['message'].tolist()
        topics, probs = modelo_bertopic.transform(mensajes)
        df_simplificado['topic_number'] = topics
        df_simplificado['probability'] = [prob.max() for prob in probs]
        logging.info("Topics asignados a los mensajes.")
        return df_simplificado
    except Exception as e:
        logging.error(f"Error al asignar topics con BERTopic: {e}")
        raise

def paso_10_cargar_topic_names():
    """
    Carga el archivo de topic names y devuelve un diccionario.
    """
    try:
        ruta_topic_names = 'topic_names.txt'
        topic_map = {}
        with open(ruta_topic_names, 'r') as f:
            for line in f:
                topic_number, topic_name = line.strip().split(': ')
                topic_map[int(topic_number)] = topic_name
        logging.info("Topic names cargados.")
        return topic_map
    except Exception as e:
        logging.error(f"Error al cargar topic names: {e}")
        raise

def paso_11_asignar_topic_names(df_simplificado, topic_map):
    """
    Asigna topic_name a cada mensaje en base a topic_number.
    """
    try:
        df_simplificado['topic_name'] = df_simplificado['topic_number'].map(topic_map)
        logging.info("Topic names asignados.")
        return df_simplificado
    except Exception as e:
        logging.error(f"Error al asignar topic names: {e}")
        raise

def paso_12_filtrar_por_tracking(df_final):
    """
    Filtra los mensajes para dejar solo aquellos con topic_name='tracking'.
    """
    try:
        if 'topic_name' in df_final.columns:
            df_tracking = df_final[df_final['topic_name'] == 'tracking']
            logging.info(f"Mensajes filtrados por topic_name='tracking': {len(df_tracking)}")
        else:
            logging.warning("La columna 'topic_name' no está presente. No se aplicó el filtro.")
            df_tracking = df_final
        return df_tracking
    except Exception as e:
        logging.error(f"Error al filtrar mensajes por tracking: {e}")
        raise

def paso_13_filtrar_por_probabilidad(df_final):
    """
    Filtra los mensajes para dejar solo aquellos con probabilidad mayor al 80%.
    """
    try:
        df_filtered = df_final[df_final['probability'] > 0.8]
        logging.info(f"Mensajes filtrados con probabilidad > 80%: {len(df_filtered)}")
        return df_filtered
    except Exception as e:
        logging.error(f"Error al filtrar mensajes por probabilidad: {e}")
        raise

if __name__ == "__main__":
    # Paso 1: Crear instancia
    predize_instance = paso_1_crear_instancia()

    # Paso 2: Obtener tickets
    df_tickets = paso_2_obtener_tickets(predize_instance)
    if df_tickets.empty:
        print("No hay tickets para procesar.")
        exit()

    # Paso 3: Filtrar por tipo
    df_tickets = paso_3_filtrar_por_tipo(df_tickets)

    # Paso 4: Obtener mensajes
    df_combined = paso_4_obtener_mensajes(predize_instance, df_tickets)

    # Paso 5: Filtrar por seller
    df_combined = paso_5_filtrar_por_seller(df_combined)

    # Paso 6: Filtrar por channelAccount_channel
    df_combined = paso_6_filtrar_por_channel(df_combined)

    # Paso 7: Simplificar DataFrame
    df_simplificado = paso_7_simplificar_dataframe(df_combined)

    # Paso 8: Cargar modelo BERTopic
    modelo_bertopic = paso_8_cargar_modelo_bertopic()

    # Paso 9: Asignar topics
    df_simplificado = paso_9_asignar_topics(df_simplificado, modelo_bertopic)

    # Paso 10: Cargar topic names
    topic_map = paso_10_cargar_topic_names()

    # Paso 11: Asignar topic names
    df_final = paso_11_asignar_topic_names(df_simplificado, topic_map)

    # Paso 12: Filtrar mensajes de tracking
    df_final = paso_12_filtrar_por_tracking(df_final)

    # Paso 13: Filtrar mensajes con probabilidad > 80%
    df_final = paso_13_filtrar_por_probabilidad(df_final)

    # Resultado final
    print("DataFrame final con topic names y filtrado:")
    print(df_final.head())
