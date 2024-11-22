import requests
import logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import os
from bertopic import BERTopic
from tqdm import tqdm
from src.predize_utils import fetch_api_order_ids, convert_tickets_to_df
from src.predize import Predize
from database.get_data import get_data
from database_credentials.db_credentials_nocnoc import db_credentials_nocnoc as creds

last_message_to = datetime.utcnow() - timedelta(minutes=15)  # 
LAST_MESSAGE_MINUTES = 90 
# Configuración del logger
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("pipeline_logs.log"),  # Guarda en un archivo
        logging.StreamHandler()  # Imprime en la consola
    ]
)

# Cargar variables de entorno
load_dotenv()

def paso_1_crear_instancia():
    """
    Crea una instancia de Predize utilizando las credenciales del archivo .env.
    """
    try:
        # Obtener credenciales del entorno
        predize_creds = {
            'email': os.getenv('PREDIZE_EMAIL'),
            'password': os.getenv('PREDIZE_PASSWORD')
        }

        # Verificar que las credenciales están definidas
        if not predize_creds['email'] or not predize_creds['password']:
            raise ValueError("Faltan las credenciales de Predize en las variables de entorno.")

        # Crear instancia de Predize
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
        fifteen_minutes_ago = now - timedelta(minutes=LAST_MESSAGE_MINUTES)

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

def paso_5_traer_channel_order_id(predize_instance, df_combined):
    """
    Obtiene el campo channelOrderId desde la API para cada ticket y lo agrega al DataFrame combinado.
    """
    try:
        def get_channel_order_id(ticket_id):
            url = f"{predize_instance.MAIN_URL}/v1/tickets/{ticket_id}/order"
            headers = predize_instance.headers
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data[0].get('channelOrderId') if data else None

        ticket_ids = df_combined['ticket_id'].tolist()
        df_combined['channelOrderId'] = [
            get_channel_order_id(ticket_id) for ticket_id in ticket_ids
        ]
        logging.info("Campo channelOrderId agregado.")
        return df_combined
    except Exception as e:
        logging.error(f"Error al obtener channelOrderId: {e}")
        raise

def paso_5_1_buscar_order_id_con_channelOrderId(df_combined):
    """
    Utiliza el channelOrderId para buscar el order_id en la tabla warehouse.raw_merchant_invoice_id.

    Args:
        df_combined (pd.DataFrame): DataFrame combinado después de traer mensajes.

    Returns:
        pd.DataFrame: DataFrame actualizado con la columna order_id.
    """
    try:
        # Filtrar filas que tienen `channelOrderId` no nulo
        df_with_channel_order_id = df_combined.dropna(subset=['channelOrderId'])
        
        if df_with_channel_order_id.empty:
            logging.info("No hay registros con channelOrderId válido.")
            return df_combined

        # Construir lista de channelOrderId para consulta
        channel_order_ids = df_with_channel_order_id['channelOrderId'].unique()
        if len(channel_order_ids) == 0:
            logging.warning("No se encontraron channelOrderIds.")
            return df_combined

        # Convertir lista a string para consulta SQL
        channel_order_ids_str = ','.join([f"'{x}'" for x in channel_order_ids])

        # Consultar en la tabla `warehouse.raw_merchant_invoice_id`
        query = f"""
        SELECT raw_merchant_invoice_id, order_id
        FROM warehouse.raw_merchant_invoice_id
        WHERE raw_merchant_invoice_id IN ({channel_order_ids_str})
        """
        df_order_ids = get_data(query, creds['postgres_admin'])

        if df_order_ids.empty:
            logging.warning("La consulta a warehouse.raw_merchant_invoice_id no devolvió resultados.")
            return df_combined

        # Separar registros por channelAccount_channel para tratamiento especial de 'b2w'
        df_b2w = df_with_channel_order_id[df_with_channel_order_id['channelAccount_channel'] == 'b2w']
        df_others = df_with_channel_order_id[df_with_channel_order_id['channelAccount_channel'] != 'b2w']

        # Tratamiento para registros de 'b2w'
        if not df_b2w.empty:
            df_b2w = df_b2w.merge(
                df_order_ids,
                left_on='channelOrderId',
                right_on='raw_merchant_invoice_id',
                how='left'
            )
            logging.info(f"Registros actualizados para 'b2w': {df_b2w['order_id'].notnull().sum()} filas.")

        # Tratamiento para otros registros
        if not df_others.empty:
            df_others = df_others.merge(
                df_order_ids,
                left_on='channelOrderId',
                right_on='raw_merchant_invoice_id',
                how='left'
            )
            logging.info(f"Registros actualizados para otros canales: {df_others['order_id'].notnull().sum()} filas.")

        # Combinar resultados
        df_combined = pd.concat(
            [df_b2w, df_others, df_combined[~df_combined['channelOrderId'].notnull()]],
            ignore_index=True
        )

        # Eliminar columnas innecesarias
        df_combined = df_combined.drop(columns=['raw_merchant_invoice_id'], errors='ignore')

        return df_combined

    except Exception as e:
        logging.error(f"Error al buscar order_id con channelOrderId: {e}")
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
    Simplifica el DataFrame combinado para incluir solo ticket_id, message, order_id y channelOrderId.
    """
    try:
        columnas_necesarias = ['ticket_id', 'message','channelOrderId','order_id']
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
    try:
        # Paso 1: Crear instancia
        predize_instance = paso_1_crear_instancia()

        # Paso 2: Obtener tickets
        df_tickets = paso_2_obtener_tickets(predize_instance)
        if df_tickets.empty:
            logging.info("No hay tickets para procesar. Finalizando flujo.")
            exit()

        # Paso 3: Filtrar por tipo
        df_tickets = paso_3_filtrar_por_tipo(df_tickets)
        if df_tickets.empty:
            logging.info("No hay tickets de tipo POST_ORDER. Finalizando flujo.")
            exit()

        # Paso 4: Obtener mensajes
        df_combined = paso_4_obtener_mensajes(predize_instance, df_tickets)
        print(creds)
        # Paso 5: Traer channelOrderId
        df_combined = paso_5_traer_channel_order_id(predize_instance, df_combined)
        
        # Paso 5.1: Buscar order_id con channelOrderId
        df_combined = paso_5_1_buscar_order_id_con_channelOrderId(df_combined)
        print(df_combined)

        # Paso 6: Filtrar por channelAccount_channel
        df_combined = paso_6_filtrar_por_channel(df_combined)

        # Paso 7: Simplificar DataFrame
        df_simplificado = paso_7_simplificar_dataframe(df_combined)
        print(df_simplificado)

        # Paso 8: Cargar modelo BERTopic
        modelo_bertopic = paso_8_cargar_modelo_bertopic()

        # Paso 9: Asignar topics
        df_simplificado = paso_9_asignar_topics(df_simplificado, modelo_bertopic)

        # Paso 10: Cargar topic names
        topic_map = paso_10_cargar_topic_names()

        # Paso 11: Asignar topic names
        df_final = paso_11_asignar_topic_names(df_simplificado, topic_map)

        # Paso 12: Filtrar por tracking
        df_final = paso_12_filtrar_por_tracking(df_final)

        # Paso 13: Filtrar por probabilidad > 80%
        df_final = paso_13_filtrar_por_probabilidad(df_final)

        # Resultado final
        logging.info("Flujo completado exitosamente. DataFrame final:")
        print(df_final.head())

    except Exception as e:
        logging.critical(f"El flujo se detuvo debido a un error crítico: {e}")
