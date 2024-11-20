from oauth2client.service_account import ServiceAccountCredentials
import warnings
import pandas as pd


class Calendar:
    """
    #TODO
    """
    def __init__(self, service_credentials_path):
        self.service = self._get_service(service_credentials_path)
        self.events = None

    def _get_service(self, service_credentials_path):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(service_credentials_path, 
                           ['https://www.googleapis.com/auth/calendar.readonly'])
        return discovery.build('calendar', 'v3', credentials=credentials)

    def _event_to_dict(self, event):
            """
            #TODO
            """
            event_dict = {"id": event.get('id'),
                        "starts_at" : event.get('start').get('dateTime'),
                        "ends_at": event.get('end').get('dateTime'), 
                        "status": event.get('status')}
            
            if event.get('visibility') != 'private':
                event_dict.update({"creator": event.get('creator').get('email'),
                                "name" : event.get('summary')})
            else:
                event_dict.update({"creator": None,
                                "name" : None})
                
            if event.get('attendees'): #check if event has attendees
                event_dict.update({"attendees" : sum([1 for attendee in event.get('attendees') if attendee.get('responseStatus') == 'accepted']), 
                                "attendees_emails" : [attendee.get('email') for attendee in event.get('attendees') if attendee.get('responseStatus') == 'accepted'],
                                "invited": sum([1 for attendee in event.get('attendees')]), 
                                "calendar_owner_attends": len([1 for attendee in event.get('attendees') if (attendee.get('responseStatus') == 'accepted') and (attendee.get('self') == True)])})
            else:
                event_dict.update({"attendees": None, 
                                "invited" : None, 
                                "calendar_owner_attends": None})
        
            return event_dict
    def get_events(self, start_at, end_at, calendar_name):
        """
        #TODO
        """
        events = self.service.events().list(calendarId=calendar_name, 
                                        timeMin=pd.Timestamp(start_at).isoformat() + 'Z',
                                        timeMax=pd.Timestamp(end_at).isoformat() + 'Z',
                                        singleEvents=True,
                                        orderBy='startTime',
                                        maxResults=2499).execute()

        if events.get('nextPageToken'):
            warnings.warn('Results may not be complete. Try using a smaller time frame')

        self.events = events.get('items')

    def to_dict(self):
        """
        #TODO
        """
        return [self._event_to_dict(event) for event in self.events]
        

    def to_dataframe(self):
        """
        #TODO
        """
        return pd.DataFrame(self.to_dict())
