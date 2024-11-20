from database.get_data import get_data
import numpy as np
import pandas as pd
from database.update_from_df import update_from_df
from database_credentials import db_credentials_nocnoc as creds
from tqdm import tqdm

def add_missing_orders():

    query = """
    select o.id,
            "channelOrderId" as merchant_invoice_id_predize ,
            t."channelAccount_channel" channel_name
    from predize_info.orders o
    inner join predize_info.tickets t
    on o.ticket_id =t.id
    where o.order_id is null
    """

    df = get_data(query,creds['postgres_admin'])

    df['merchant_invoice_id_predize'] = np.where(
        df['channel_name']=='magalu',
        'LU-'+df['merchant_invoice_id_predize'],
        df['merchant_invoice_id_predize']
    )

    df_americanas = df[df['channel_name']=='b2w']
    df = df[df['channel_name']!='b2w']

    df_americanas['merchant_invoice_id_predize'] = df_americanas['merchant_invoice_id_predize'].str.replace('Lojas Americanas-','').str.replace('Sou Barato-','').str.replace('soubarato-','')
    df_americanas['contains_delimiter'] = df_americanas['merchant_invoice_id_predize'].str.contains('-')
    df_americanas['code_length'] = df_americanas['merchant_invoice_id_predize'].str.len()
    df = pd.concat([
                    df,
                    df_americanas[df_americanas['contains_delimiter']==False][['id','merchant_invoice_id_predize','channel_name']]
                    ])

    df_americanas = df_americanas[df_americanas['contains_delimiter']==True]

    raw_merchant_invoce_id = ','.join([f"'{x}'" for x in df['merchant_invoice_id_predize'].values])

    df_rmii = get_data(f"SELECT * from warehouse.raw_merchant_invoice_id ffi where ffi.raw_merchant_invoice_id in ({raw_merchant_invoce_id})",creds['postgres_admin'])

    df_americanas['search_str'] = df_americanas['merchant_invoice_id_predize'].str.split('-').str[-1]

    df = df.join(
        df_rmii.set_index('raw_merchant_invoice_id'),
        on='merchant_invoice_id_predize',
        how='left'
    ).dropna(subset='order_id')


    df['order_id'] = df['order_id'].astype('Int64').astype(str).astype(int)

    if df.shape[0]>0:

        update_from_df(
            df=df[['id','order_id','merchant_invoice_id']],
            conn_string=creds['postgres_admin'],
            match_columns=['id'],
            update_columns=['order_id','merchant_invoice_id'],
            target_table='orders',
            target_schema='predize_info'
        )

    df_americanas_rmii = get_data("""
    with mi as (
        select *
        from warehouse.raw_merchant_invoice_id rmii
        where rmii.merchant_id  in (
            select merchant_id
            from warehouse.dim_merchant dm
            where lower(merchant_name)  like '%%america%%'
        )
    )
    select mi.*
    from mi
    left join (
        select o.order_id
        from predize_info.orders o
        inner join predize_info.tickets t
        on o.ticket_id =t.id
        where t."channelAccount_channel" = 'b2w'
    ) _to
    on mi.order_id = _to.order_id
    where _to.order_id is null
    """, creds['postgres_admin'])

    list_results = []

    for search_str in tqdm(df_americanas['search_str'].unique()):

        df_search = df_americanas_rmii[df_americanas_rmii['raw_merchant_invoice_id'].str.contains(search_str)]

        if df_search.shape[0]>0:
            list_results.append(df_search.assign(search_str=search_str))


    if len(list_results)>0:
        df_search_results = pd.concat(list_results)

        df_americanas = df_search_results.join(
                    df_americanas.set_index('search_str'),on='search_str',how='inner'
                    )[['id','order_id','merchant_invoice_id']].sort_values(by=['order_id'],ascending=False).drop_duplicates(subset=['id'])

        update_from_df(
            df=df_americanas[['id','order_id','merchant_invoice_id']],
            conn_string=creds['postgres_admin'],
            match_columns=['id'],
            update_columns=['order_id','merchant_invoice_id'],
            target_table='orders',
            target_schema='predize_info'
        )