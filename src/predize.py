import requests
from tenacity import retry, stop_after_attempt, wait_fixed
from time import sleep
from random import randint
from typing import Iterable
from tqdm import tqdm
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor


def unpack_messages(messages):
    """
    Procesa los mensajes y los estructura en una lista.
    """
    return [msg for sublist in messages for msg in sublist.get('messages', [])]


def return_error_ticket_id(retry_state):
    ticket_id = retry_state.args[0] or retry_state.kwargs.get('ticket_id')
    _exception = retry_state.outcome._exception
    return {'id': ticket_id, 'exception': _exception}


class Predize:
    def __init__(self, email: str, password: str) -> None:
        self.MAIN_URL = "https://api.predize.com"
        self.token_info = self.get_token(email, password)
        self.token = self.token_info.get('accessToken')
        self.headers = self.build_headers(self.token)
        self.refresh_token = self.token_info.get('refreshToken')
        self.tenant_id = 'efb0b1c4-32ae-4355-8465-4013e27f88be'

    def _refresh_token(self):
        print(datetime.now(), 'Refreshing token')
        url = f"{self.MAIN_URL}/v1/auth/refresh"
        params = {'refreshToken': self.refresh_token}
        response = requests.post(url, json=params)
        self.validate_response(response)
        self.token_info = response.json()
        self.token = self.token_info.get('accessToken')
        self.headers = self.build_headers(self.token)
        self.refresh_token = self.token_info.get('refreshToken')

    def validate_response(self, response, raise_unauthorized=True):
        if not response.ok:
            sleep(randint(5, 7))
            if response.json().get('message') == 'Unauthorized':
                self._refresh_token()
                if raise_unauthorized:
                    raise Exception('Not Authorized')
            elif response.json().get('error') == 'Not Found':
                pass
            else:
                raise Exception(response.json())

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def get_token(self, email: str, password: str) -> dict:
        params = {"email": email, "password": password}
        headers = {'Content-Type': 'application/json', 'accept': 'application/json'}
        url = f"{self.MAIN_URL}/v1/auth/login"
        response = requests.post(url=url, headers=headers, json=params)
        self.validate_response(response)
        return response.json()

    def build_headers(self, token) -> dict:
        return {"accept": "application/json", "Authorization": f"Bearer {token}"}

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def get_tickets(self, page=1, limit=100, status=None, type=None, claim_type=None,
                    greater_than_date: str = None, less_than_date: str = None,
                    last_message_from: str = None, last_message_to: str = None):
        params = {
            'status': status,
            'type': type,
            'claimType': claim_type,
            'greaterThanDate': greater_than_date,
            'lessThanDate': less_than_date,
            'lastMessageFrom': last_message_from,
            'lastMessageTo': last_message_to
        }
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.MAIN_URL}/v1/tickets?page={page}&limit={limit}"
        response = requests.get(url=url, headers=self.headers, params=params)
        self.validate_response(response)
        sleep(randint(1, 3))
        return response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def get_messages_by_ticket_id(self, ticket_id, page=1, limit=100, raise_unauthorized=True):
        url = f"{self.MAIN_URL}/v1/tickets/{ticket_id}/messages?page={page}&limit={limit}"
        response = requests.get(url=url, headers=self.headers)
        self.validate_response(response, raise_unauthorized)
        return response.json()

    def get_messages_in_parallel(self, tickets=[], raise_unauthorized=True, max_workers=50):
        list_params = [{'ticket_id': x, 'raise_unauthorized': raise_unauthorized} for x in tickets]
        return self._run_parallel(self.get_messages_by_ticket_id, list_params, max_workers=max_workers)

    def get_tickets_in_parallel(self, pages=[], max_workers=50):
        list_params = [{'page': x} for x in pages]
        return self._run_parallel(self.get_tickets, list_params, max_workers=max_workers)

    def _run_parallel(self, func, list_params: Iterable, max_workers=50):
        max_workers = min(len(list_params), max_workers)
        if max_workers <= 0:
            return []  # No hay tareas que procesar
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            res = list(tqdm(executor.map(lambda x: func(**x) if isinstance(x, dict) else func(x), list_params),
                            total=len(list_params)))
        return res

    def get_last_message_for_tickets(self, ticket_ids, max_workers=10):
        """
        Obtiene el último mensaje para cada ticket en la lista de ticket_ids.

        Args:
            ticket_ids (list): Lista de IDs de tickets.
            max_workers (int): Número máximo de hilos para paralelizar la consulta.

        Returns:
            dict: Diccionario donde las claves son ticket_ids y los valores son los últimos mensajes.
        """
        def fetch_last_message(ticket_id):
            try:
                messages = self.get_messages_by_ticket_id(ticket_id)
                if not messages or not messages.get('items'):
                    return None
                # Ordenar mensajes por fecha de creación para obtener el último
                sorted_messages = sorted(messages['items'], key=lambda x: x['createDate'], reverse=True)
                return sorted_messages[0]  # El último mensaje
            except Exception as e:
                print(f"Error al obtener el último mensaje para el ticket {ticket_id}: {e}")
                return None

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(fetch_last_message, ticket_ids))

        # Crear un diccionario de ticket_id a último mensaje
        return {ticket_id: message for ticket_id, message in zip(ticket_ids, results) if message}
