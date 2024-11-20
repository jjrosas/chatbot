import requests
import functools
import traceback
import os


def _build_slack_message(step_name:str, error_message:str):

    header_block = {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f":alert: Predize Error - {step_name} :alert:",
                        "emoji": True
                    }
                }

    error_detail_block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text":f"```{error_message}```"
                }
            }

    messages = {
        "blocks": [
            header_block,
            error_detail_block
        ]
    }

    return messages


def send_error_message(step_name:str,
                       error_message:str):
    # This function sends an error message to a given API endpoint.
    # Customize the URL and headers as per the API requirements.
    slack_url = os.environ.get('WEBHOOK_PREDIZE_ERROR_URL')
    headers = {'Content-type': 'application/json'}
    message = _build_slack_message(step_name, error_message)
    headers = {"Content-Type": "application/json"}
    response = requests.post(slack_url,
                            json = message,
                            headers = headers)

def predize_error_handling(step_name:str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_traceback = traceback.format_exc()  # Get full traceback as a string
                error_message = str(e)
                send_error_message(step_name, error_traceback)
                raise  # Re-raise the exception after logging
        return wrapper
    return decorator
