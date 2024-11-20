import pandas as pd
import numpy as np
from functools import partial
from itertools import chain
from database.get_data import get_data
from database_credentials.db_credentials_nocnoc import db_credentials_nocnoc as creds


# ver estos imports locos en el server
#from src.MercadoLibreClaims import MercadoLibreClaims
#from src.utils import get_meli_token

MELI_MERCHANT_IDS = [23,37,49,50,52]

def get_sku(attributes):
    for elem in attributes:
        if elem.get('name')=='SKU':
            return elem.get('value')



def unpack_relevant_tickets(predize_tickets:list):

    predize_tickets = [x for x in predize_tickets if 'statusCode' not in predize_tickets]
    predize_tickets = [x.get('items') for x in predize_tickets]
    predize_tickets = [x for x in predize_tickets if x is not None]

    predize_tickets = [item for sublist in predize_tickets for item in sublist]


    return predize_tickets


def convert_tickets_to_df(predize_tickets:list)->pd.DataFrame:

    df = pd.json_normalize(predize_tickets)

    df['id'] = df['id'].astype(int)

    df['channelDate'] = np.where(df['channelDate'].isin( [
                            "0001-12-30T00:00:00.000Z",
                            "0000-12-30T00:00:00.000Z"
                            ]),
                            df['lastUpdate'],
                            df['channelDate']
                            )

    df =  df.drop_duplicates(subset='id')

    timestamp_cols = ['closeDate','lastUpdate','channelDate','targetSla']
    for col in timestamp_cols:
        df[col] = pd.to_datetime(df[col],utc=True)

    df.columns = [x.replace('.','_') for x in df.columns]

    df['closeDate'] = np.where(((df['closeDate'].isnull()) & (df['status'] == 'CLOSED')),
                                    df['lastUpdate'],
                                    df['closeDate'])

    df['merchant_id'] = df['channelAccount_id']
    

    for col in ['context','whoResponded','apiId','buyer','observation','tags','lastMessageDate','reasons']:
        if col in df.columns:
            df.drop(columns=col,inplace=True)

    # claim_meli = []
    # pre_order_tickets = []
    # for merchant_id, df_ticket_merchant in df.groupby('merchant_id'):
    #     if merchant_id in MELI_MERCHANT_IDS:
    #         ml = MercadoLibreClaims(get_meli_token(merchant_id),partial(get_meli_token,merchant_id),merchant_id)
    #         for ticket_type,df_ticket_type_merchant in df_ticket_merchant.groupby('type'):
    #             claim_ids = df_ticket_type_merchant['ticketChannelId'].dropna().to_list()
    #             if ticket_type=='PRE_ORDER':
    #                 meli_presale = ml._run_parallel(ml.get_presale_question_by_id,claim_ids)
    #                 # se necesita saber el pais
    #                 meli_presale
    #             else:
    #                 print(ticket_type)
    #                 meli_claims = ml._run_parallel(ml.get_claim,claim_ids)
    #                 meli_claims = [x for  x in  meli_claims if x is not None]
    #                 print(len(meli_claims),len(claim_ids))



    return df


def unpack_messages(predize_messages:list):

    all_messages = []
    if isinstance(predize_messages,dict):
        predize_messages = [predize_messages]

    for m_page in predize_messages:
        for item in m_page.get('items'):
            if 'ticket_id' not in item:
                item['ticket_id'] = m_page.get('ticket_id')
            all_messages.append(item)


    return all_messages

def convert_messages_to_df(predize_messages:list)->pd.DataFrame:

    df_messages = pd.json_normalize(predize_messages)

    df_messages['id'] = df_messages['id'].astype(int)

    df_messages['createDate'] = pd.to_datetime(df_messages['createDate'],errors='coerce')

    for col in ['context','apiId','ticketChatContextId']:
        if col in df_messages.columns:
            df_messages.drop(columns=col,inplace=True)

    df_messages['whoResponded'] = np.where(df_messages['seller']==True,df_messages['whoResponded'],None)

    return df_messages

def add_nocnoc_orders(df_orders):

    order_ids = ','.join(df_orders['id'].astype(str).values)

    q = f"select id,order_id,merchant_invoice_id from predize_info.orders o where o.id in ({order_ids})"

    df = get_data(q,creds['postgres_admin'])

    df_orders = df_orders.join(df.set_index('id'),on='id',how='left')

    df_orders['order_id'] = df_orders['order_id'].astype('Int64')

    return df_orders


def convert_orders_to_df(orders):

    df_orders = pd.json_normalize(
        list(chain.from_iterable(orders))
    )[['code','creationDate','status','channelOrderId','invoiceNumber','invoiceKey','ticket_id']].rename(columns={'code':'id'})
    df_orders['id'] =df_orders['id'].astype(int)
    df_orders['creationDate'] = pd.to_datetime(df_orders['creationDate'],format='mixed',errors="coerce")
    df_orders.sort_values(by='ticket_id').drop_duplicates(subset=['id'],keep='first')

    df_orders = add_nocnoc_orders(df_orders)

    return df_orders

def convert_publications_to_df(publications):

    df = pd.json_normalize(publications)

    if 'statusCode' in df.columns:
        df = df[df['statusCode'].isna()]

        remove_cols = ['statusCode','message','error']

        for col in remove_cols:
            if col in df.columns:
                df.drop(columns=col, inplace=True)

    df['sku'] = np.where(df['sku'].isna(),df['attributes'].apply(get_sku),df['sku'])

    df = df.drop_duplicates(subset=['id','ticket_id'])

    df['attributes'] = "'"+df['attributes'].astype(str)+"'"

    return df

import requests
from concurrent.futures import ThreadPoolExecutor

def fetch_api_order_ids(df_tickets, predize_instance, max_workers=10):
    """
    Consulta la API para obtener el `order_id` de cada ticket y lo añade al DataFrame.

    Args:
        df_tickets (pd.DataFrame): DataFrame con información de tickets.
        predize_instance (Predize): Instancia de la clase Predize para interactuar con la API.
        max_workers (int): Número máximo de hilos para ejecutar en paralelo.

    Returns:
        pd.DataFrame: DataFrame actualizado con una columna `order_id`.
    """
    def get_order_id(ticket_id):
        url = f"{predize_instance.MAIN_URL}/v1/tickets/{ticket_id}/order"
        headers = predize_instance.headers
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            # Devolver el `code` si está disponible, o None si no
            return data[0]['code'] if data else None
        except Exception as e:
            print(f"Error al obtener order_id para ticket_id {ticket_id}: {e}")
            return None

    # Crear una lista de ticket_ids
    ticket_ids = df_tickets['id'].tolist()

    # Usar ThreadPoolExecutor para consultar en paralelo
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        order_ids = list(executor.map(get_order_id, ticket_ids))

    # Añadir la columna `order_id` al DataFrame
    df_tickets['order_id'] = order_ids

    return df_tickets
