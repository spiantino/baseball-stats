# from __future__ import print_function

import os
import requests
import httplib2

from apiclient import discovery
from oauth2client import client, tools
from oauth2client.file import Storage

SLIDESID = '1ZughGrRUSmcI77RiBrh6Fk1UOI31ksJVxM2UCg9HggY'
SCOPES = 'https://www.googleapis.com/auth/presentations'


# def save_pdf():
#     """
#     Save slides as pdf
#     """
#     driveAPI = discovery.build('drive', 'v3', http=http)
#     data = driveAPI.files().export(fileId=SLIDESID,
#                                    mimeType='application/pdf').execute()
#     with open('test.pdf', 'wb') as pdf:
#         pdf.write(data)

# def refresh():
#     """
#     Refresh slides using
#     refreshSheetsChart method
#     """
#     slidesAPI = discovery.build('slides', 'v1', http=http)
#     slidesAPI.presentations()
#     # presentation = slidesAPI.presentations()\
#     #                         .get(presentationId=SLIDESID)\
#     #                         .execute()
#     # print(type(presentation))
#     # slides = presentation.get('slides')
#     # print(slides)

class ApiController():
    def __init__(self, slidesID, http):
        self._slidesID = slidesID
        self._http = http

    def save_pdf(self):
        """
        Save slides as pdf
        """
        driveAPI = discovery.build('drive', 'v3', http=self._http)
        data = driveAPI.files().export(fileId=self._slidesID,
                                       mimeType='application/pdf').execute()
        with open('test.pdf', 'wb') as pdf:
            pdf.write(data)

    def refresh(self):
        """
        Refresh slides using
        refreshSheetsChart method
        """
        slidesAPI = discovery.build('slides', 'v1', http=self._http)
        slidesAPI.presentations()


if __name__ == '__main__':

    auth_path = os.path.join(os.path.expanduser("~"), ".oauth", "drive.json")
    store = Storage(auth_path)
    creds = store.get()

    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
        creds = tools.run_flow(flow, store)

    http = creds.authorize(httplib2.Http())

    # Create API object
    api = ApiController(slidesID=SLIDESID, http=http)
    api.save_pdf()
