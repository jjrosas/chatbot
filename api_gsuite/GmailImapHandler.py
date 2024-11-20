import imaplib
import email
from datetime import datetime
from typing import Union




# Connect to the IMAP server
class GmailImapHandler:


    def _convert_to_datetime(self,datetime_str):

        imap_date_format = "%a, %d %b %Y %H:%M:%S %z"
        try:
            datetime_obj = datetime.strptime(datetime_str, imap_date_format)
            return datetime_obj
        except ValueError as e:
            print(f"Error: {e}")
            return None

    # Define the format of the input date string
    date_format = "%a, %d %b %Y %H:%M:%S %z"

    def __init__(self,email_str:str,password_str:str,inbox:str='inbox') -> None:


        self.mail = imaplib.IMAP4_SSL('imap.gmail.com',993)

        # Login to your account
        self.mail.login(email_str, password_str)

        # Select the mailbox you want to work with (e.g., 'INBOX')
        self.mail.select('inbox')

    def search_emails(self,
                      from_email:str=None,
                      subject:str=None,
                      since_date:Union[str,datetime]=None,
                      before_date:Union[str,datetime]=None):

        filters = []

        if isinstance(since_date,str):
            since_date = datetime.strptime(since_date,'%Y-%m-%d')
        if isinstance(before_date,str):
            before_date = datetime.strptime(before_date,'%Y-%m-%d')

        if since_date:
            since_date = since_date.strftime('%d-%b-%Y')
        if since_date:
            before_date = before_date.strftime('%d-%b-%Y')

        if from_email:
            filters.append(f'FROM "{from_email}"')

        if subject:
            filters.append(f'SUBJECT "{subject}"')

        if since_date:
            filters.append(f'SINCE "{since_date}"')

        if before_date:
            filters.append(f'BEFORE "{before_date}"')


        if len(filters)==0:
            filters = None
        else:
            filters = ' '.join(filters)

        status, message_ids = self.mail.search(None, (filters or 'ALL'))
        message_ids = message_ids[0].split()

        return message_ids

    def fetch_email_by_id(self,msg_id):

        status,msg_data =self.mail.fetch(msg_id, '(RFC822)')

        raw_email = msg_data[0][1]

        return email.message_from_bytes(raw_email)


    def download_attachment_by_extension(self,
                                          message_ids:list,
                                          extension:str='xlsx',
                                          add_received_timestamp:bool=False):

        downloaded_files = []

        # Loop through the message IDs and retrieve the emails
        for msg_id in message_ids:
            status, msg_data = self.mail.fetch(msg_id, '(RFC822)')
            raw_email = msg_data[0][1]

            # Convert raw email data to an email object
            msg = email.message_from_bytes(raw_email)

            for part in msg.walk():

                if 'application' in (part.get_content_type() or '') and  part.get('Content-Disposition') is not None:
                    file_name = part.get_filename().replace(':','')
                    if file_name.endswith(extension):
                        received_date =  self._convert_to_datetime(msg['date']).strftime('%Y%m%d')
                        if add_received_timestamp:
                            file_name = file_name.replace(f'.{extension}',f'-{received_date}.{extension}')
                        with open(file_name, 'wb') as f:
                            f.write(part.get_payload(decode=True))
                        downloaded_files.append(file_name)

        return downloaded_files

    def logout(self):
        print("Closing email")
        # Logout and close the connection
        self.mail.logout()



if __name__=='__main__':

    email_data = 'data@nocnocstore.com'
    pass_data = 'pxsn yamt fshc ngga'

    # Search for all emails
    NOBORDIST_EMAIL = "noreply@metabaseapp.com"
    NOBODRIST_SUBJECT = "Smart DDU Operational Overview - NocNoc"

    gmail = GmailImapHandler(email_data,pass_data)

    messages = gmail.search_emails(from_email=NOBORDIST_EMAIL,
                                   subject=NOBODRIST_SUBJECT,
                                   since_date='2023-10-23')

    if len(messages) == 0:
        print('No file for today!')
    else:
        downloaded_files = gmail.download_attachment_by_extension(messages,
                                                                    extension='xlsx',
                                                                    add_received_timestamp=True)

        print(downloaded_files)