suffix_list = ['del','reenvio','wrong','old','au','faltante','corregido','final','unidad','envio','send','cancelled']

count_last_part = [str(x) for x in range(1,100)]

def check_if_bad_suffix(last_part:str):

    if last_part in count_last_part:
        return True

    for suff in suffix_list:
        if suff in last_part.lower():
            return True

    return False

def get_raw_merchant_invoice_id(merchant_invoice_id:str):

    if '-' not in merchant_invoice_id:
        return merchant_invoice_id

    last_part = str(merchant_invoice_id.split('-')[-1])

    flag_bad_last_part = check_if_bad_suffix(last_part)

    if flag_bad_last_part:
        return '-'.join(merchant_invoice_id.split('-')[:-1])

    return merchant_invoice_id