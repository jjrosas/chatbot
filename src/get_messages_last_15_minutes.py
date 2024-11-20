from datetime import datetime, timedelta
from src.predize import Predize
from predize_utils import unpack_messages, convert_messages_to_df, convert_tickets_to_df, add_nocnoc_orders, fetch_api_order_ids
import pandas as pd
from datetime import datetime, timedelta
import pandas as pd
from src.predize_utils import fetch_api_order_ids, convert_tickets_to_df
from src.predize import Predize

def paso_1_crear_instancia():
    """
    Crea una instancia de Predize.
    """
    # Credenciales para Predize
    predize_creds = {
        'email': 'nocnoc@predize.com',
        'password': 'nocnocapi*'
    }
    predize_instance = Predize(
        email=predize_creds['email'],
        password=predize_creds['password']
    )
    print("Instancia creada.")
    return predize_instance

def paso_2_obtener_tickets(predize_instance):
    """
    Obtiene los tickets de los últimos 15 minutos.
    """
    now = datetime.utcnow()
    fifteen_minutes_ago = now - timedelta(minutes=15)

    # Obtener tickets de la API
    tickets_response = predize_instance.get_tickets(
        last_message_to=fifteen_minutes_ago.isoformat() + "Z",
        last_message_from=now.isoformat() + "Z"
    )
    tickets = tickets_response.get('items', [])

    if not tickets:
        print("No tickets found in the last 15 minutes.")
        return pd.DataFrame()  # Retornar un DataFrame vacío si no hay tickets

    # Convertir tickets a DataFrame
    df_tickets = convert_tickets_to_df(tickets)
    df_tickets['id'] = df_tickets['id'].astype(str)  # Asegurar que `id` sea string
    print(f"Tickets obtenidos: {len(df_tickets)}")
    return df_tickets

def paso_3_obtener_mensajes(predize_instance, df_tickets):
    """
    Obtiene los mensajes asociados a los tickets.
    """
    # Obtener el último mensaje por ticket
    ticket_ids = df_tickets['id'].tolist()
    last_messages = predize_instance.get_last_message_for_tickets(ticket_ids)

    # Convertir el diccionario de mensajes a DataFrame
    df_messages = pd.DataFrame.from_dict(last_messages, orient='index').reset_index()
    df_messages.rename(columns={'index': 'ticket_id'}, inplace=True)

    # Combinar tickets y mensajes
    df_combined = df_tickets.merge(df_messages, left_on='id', right_on='ticket_id', how='left', suffixes=('', '_message'))
    print(f"Mensajes obtenidos: {len(df_messages)}")
    return df_combined

def paso_4_obtener_order_ids(predize_instance, df_combined):
    """
    Obtiene los order_id asociados a los tickets usando la API.
    """
    df_combined = fetch_api_order_ids(df_combined, predize_instance)
    print("Order IDs agregados.")
    return df_combined

if __name__ == "__main__":
    # Paso 1: Crear instancia
    predize_instance = paso_1_crear_instancia()

    # Paso 2: Obtener tickets
    df_tickets = paso_2_obtener_tickets(predize_instance)
    if df_tickets.empty:
        print("No hay tickets para procesar.")
        exit()

    # Paso 3: Obtener mensajes
    df_combined = paso_3_obtener_mensajes(predize_instance, df_tickets)

    # Paso 4: Obtener order IDs
    df_final = paso_4_obtener_order_ids(predize_instance, df_combined)

    # Resultado final
    print("DataFrame final:")
    print(df_final.head())
