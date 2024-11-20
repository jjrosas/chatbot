import pandas as pd
import requests as r
from google.auth.transport import requests
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from typing import Union
from PIL import Image
from oauth2client.service_account import ServiceAccountCredentials
import json

class GslidesHandler():

    def __init__(self,creds:Union[str,dict]):

        """
        Class for google slides handling
        """

        SCOPES=['https://www.googleapis.com/auth/presentations']
        
        try:
            print('trying as service account')
            if isinstance(creds,str):
                creds = service_account.Credentials.from_service_account_file(creds,scopes=SCOPES)
            elif isinstance(creds,dict):
                creds = ServiceAccountCredentials.from_json_keyfile_dict(creds,scopes=SCOPES)
            self.service = build('slides', 'v1', credentials=creds)
            print('trying as service account succeeded!')
        except:
            print('trying as flow application failed')
            try:
                print('trying as service account')
                flow = InstalledAppFlow.from_client_secrets_file(creds, SCOPES)
                creds = flow.run_local_server(port=8090)
                self.service = build('slides', 'v1', credentials=creds)
            except:
                print('trying as flow application failed')
                pass

        self.presentation = None
        self.presentation_id = None
        self.slides = None

        # convetion matrix
        if True:
            INCH_2_EMU = 914400.0 # Emus/inch
            INCH_2_PIXEL = 96 #pixel/inch
            MM_2_INCH = 25.4 #mm/inch
            PIX_2_EMU = INCH_2_EMU/INCH_2_PIXEL # Emus/pixel
            MM_2_EMU = INCH_2_EMU/MM_2_INCH # Emus/mm
            PIX_2_MM = INCH_2_PIXEL/MM_2_INCH  # pixel/mm

            self.convertion_matrix = pd.DataFrame( 
                                [['EMU' , 1.0       , 1.0/INCH_2_EMU    , 1.0/MM_2_EMU , 1/PIX_2_EMU],
                                ['in'   , INCH_2_EMU, 1.0               , MM_2_INCH    , INCH_2_PIXEL],
                                ['mm'   , MM_2_EMU  , 1.0/MM_2_INCH     , 1.0          , 1/PIX_2_MM ],
                                ['pixel', PIX_2_EMU , 1.0/INCH_2_PIXEL  , PIX_2_MM     , 1.0]],
                                columns=['UNIT','EMU','in','mm','pixel'])

    def set_presentation(self,presentation_id:str):
        
        """
        sets a presentation for the gslides object
        """

        self.presentation = self.service.presentations().get(
                                        presentationId=presentation_id).execute()
        self.slides = self.presentation.get('slides')
        self.presentation_id = presentation_id

    def get_slides(self,presentation_id:str = None)->dict:
        
        """
        gets the slides of a presentation as json
        """

        if self.presentation is None and presentation_id is not None:
            presentation = self.service.presentations().get(
                                presentationId=presentation_id).execute()
            slides = presentation.get('slides')
        else:
            slides = self.presentation.get('slides')

        return slides

    def get_slide_id(self,pagenum:int):
        
        """
        returns de page element by page num
        """


        for i, slide in enumerate(self.slides):
            if i+1==pagenum:
                return slide.get('objectId')

    def get_page_elemet_by_id(self,page_element_id:str)->list:

        """
        gets a page element by id
        """

        ouput_element = None
        page_id = None
        for slide in self.slides:
            for elem in slide.get('pageElements'):
                if elem.get('objectId')==page_element_id:
                    ouput_element = elem
                    break
            if ouput_element is not None:
                page_id = slide.get('objectId')
                break

        return ouput_element

    def get_text_box_by_slide_number(self,slide_number:int):

        pass


    def get_slide_by_number(self,slide_number:int)->dict:
        
        """
        returns a slide page element by slide number
        """

        slide_id = self.get_slide_id(slide_number)
        
        for elem in self.get_slides():
            if elem.get('objectId') == slide_id:
                return elem
                

    def get_original_pixel_size(self,image_url:str)->tuple:

        """
        returns the original image size in pixel
        """

        im = Image.open(r.get(image_url, stream=True).raw)

        return im.size # in pixels

    def convert_units(self,unit_from:str,unit_to:str):

        df = self.convertion_matrix

        return df[df.UNIT==unit_from][unit_to].iloc[0]

    def adjust_new_image_size():
        pass

    def upload_image(self,
                    image_url:str,
                    image_id:str,
                    page_id:str,
                    pos_x:int=100000,
                    pos_y:int=100000,
                    presentation_id:str=None):
        
        """
        uploads an image to a presentation
        """
        
        width_px,height_px = self.get_original_pixel_size(image_url)

        width_EMU,height_EMU = width_px*self.convert_units('pixel','EMU'),height_px*self.convert_units('pixel','EMU')

        requests_body = []

        emu4M_h = {
            'magnitude': height_EMU,
            'unit': 'EMU'
            }

        emu4M_w = {
            'magnitude': width_EMU,
            'unit': 'EMU'
            }

        request_1 = {
            'createImage': {
                'objectId': image_id,
                'url': image_url,
                'elementProperties': {
                    'pageObjectId': page_id,
                    'size': {
                        'height':emu4M_h,
                        'width':emu4M_w
                    },
                    'transform': {
                        'scaleX': 1,
                        'scaleY': 1,
                        'translateX': pos_x,
                        'translateY': pos_y,
                        'unit': 'EMU'
                    }
                }
            }
            }

        request_2 =  {
            'createImage': {
                'objectId': image_id,
                'url': image_url,
                'elementProperties': {
                    'pageObjectId': page_id
                    ,
                    'transform': { 
                        'scaleX': 1,
                        'scaleY': 1,
                        'translateX': pos_x,
                        'translateY': pos_y,
                        'unit': 'EMU'
                    }    
                    }
                }
            }
    
        requests_body.append(request_2)

        body = {'requests': requests_body}

        print(body)

        if self.presentation_id is not None and presentation_id is None: 
            response = self.service.presentations().batchUpdate(presentationId=self.presentation_id,
                                                             body=body).execute()
        elif self.presentation_id is not None: 
            response = self.service.presentations().batchUpdate(presentationId=self.presentation_id,
                                                             body=body).execute()

        return response
    
    def delete_object_by_id(self,object_id:str,presentation_id:str=None):

        """
        deletes an object from a object_id
        """

        requests = []
        requests.append({'deleteObject':{
                     "objectId": object_id}})        
        body = {'requests': requests}
        if self.presentation_id is not None and presentation_id is None: 
            response = self.service.presentations().batchUpdate(presentationId=self.presentation_id,
                                                             body=body).execute()
        elif self.presentation_id is not None: 
            response = self.service.presentations().batchUpdate(presentationId=self.presentation_id,
                                                             body=body).execute()

    def clean_report_images(self):

        """
        clean a report from reporting images
        """

        for slide in self.slides:
            for elem in slide['pageElements']:
                object_id = elem['objectId']
                if 'test' in object_id:
                    self.delete_object_by_id(object_id)
             
    def replace_image(self,old_image_id:str,new_image_url:str):
        
        old_image,page_id = self.get_page_elemet_by_id(old_image_id)
        requests = []
        requests.append({
            'createImage': {
                'objectId': old_image_id+'test',
                'url': new_image_url,
                'elementProperties': {
                    'pageObjectId': page_id,
                    'size': {
                        'height': old_image.get('size').get('height'),
                        'width': old_image.get('size').get('width')
                    }
                    ,
                    'transform': {
                        'scaleX': old_image.get('transform').get('scaleX'),
                        'scaleY': old_image.get('transform').get('scaleY'),
                        'translateX': old_image.get('transform').get('translateX'),
                        'translateY': old_image.get('transform').get('translateY'),
                        'unit': 'EMU'
                    }
                }
            }
            })

        body = {'requests': requests}

        response = self.service.presentations().batchUpdate(presentationId=self.presentation_id,
                                                             body=body).execute()

    def replace_simple_text(self,page_element_id:str,new_text:str):

        body_request = []

        body_request.append({
            'deleteText': {
                'objectId': page_element_id,
                'textRange': {
                    'type': 'ALL'
                }
            }
        })
        body_request.append({
            'insertText': {
                'objectId': page_element_id,
                'insertionIndex': 0,
                'text': new_text
            }
        })

        body = {
            'requests': body_request
        }
        response = self.service.presentations().batchUpdate(
            presentationId=self.presentation_id, body=body).execute()

        return response

    def formating_text_update(self,shape_id:str,new_style_dict):
        requests = [
            {
                'updateTextStyle': {
                    'objectId': shape_id,
                    'textRange': {
                        'type': 'ALL'
                    },
                    'style': 
                        new_style_dict,
                    'fields':'*'
                }
            }]
        
        body = {
        'requests': requests
        }

        response = self.service.presentations().batchUpdate(
            presentationId=self.presentation_id, body=body).execute()

    def replace_all_text(self,old_text:str,new_text:str):
        requests = [
                    {"replaceAllText":
                        {"replaceText": new_text,
                            "containsText": {
                            "text": old_text,
                            "matchCase": True
                            }
                        }
                    }
                ]
        
        body = {
        'requests': requests
        }

        response = self.service.presentations().batchUpdate(
            presentationId=self.presentation_id, body=body).execute()

class TextWithFormat():

    def __init__(self):
        pass

