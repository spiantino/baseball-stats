from __future__ import print_function

import os
import sys
import httplib2

from apiclient import discovery
from oauth2client import client, tools
from oauth2client.file import Storage

SLIDESID = '1ZughGrRUSmcI77RiBrh6Fk1UOI31ksJVxM2UCg9HggY'
SCOPES = ('https://www.googleapis.com/auth/presentations',
          'https://www.googleapis.com/auth/drive.readonly')

class ApiController():
    def __init__(self, slidesID, http):
        self._slidesID = slidesID
        self._http = http

    def save_pdf(self):
        """
        Save slides as pdf
        """
        service = discovery.build('drive', 'v3', http=self._http)
        data = service.files().export(fileId=self._slidesID,
                                       mimeType='application/pdf').execute()
        with open('test.pdf', 'wb') as pdf:
            pdf.write(data)

    def refresh(self):
        """
        Refresh slides using
        refreshSheetsChart method
        """
        service = discovery.build('slides', 'v1', http=self._http)

        # Collect page ID's
        pages = service.presentations()\
                       .get(presentationId=self._slidesID,
                            fields='slides.objectId')\
                       .execute()['slides']

        page_ids = [v for x in pages for v in x.values()]

        # Get object ID's from pages
        for id_ in page_ids:
            objects = service.presentations()\
                             .pages()\
                             .get(presentationId=self._slidesID,
                                  pageObjectId=id_,
                                  fields='pageElements')\
                             .execute()['pageElements']

            # Extract chart object ID's
            charts = [{'objectId' : x['objectId']}
                      for x in objects
                      if 'sheetsChart' in x.keys()]

            # Refresh charts one at a time
            if charts:
                for chart in charts:
                    reqs = [{'refreshSheetsChart' : chart}]
                    print(reqs)
                    service.presentations()\
                           .batchUpdate(body={'requests':reqs},
                                        presentationId=self._slidesID)\
                           .execute()

if __name__ == '__main__':

    # Authentication
    auth_path = os.path.join(os.path.expanduser("~"), ".oauth", "drive.json")
    store = Storage(auth_path)
    creds = store.get()

    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
        creds = tools.run_flow(flow, store)

    http = creds.authorize(httplib2.Http())

    # Create API controller object
    api = ApiController(slidesID=SLIDESID, http=http)

    if sys.argv[1] in ['r', 'refresh']:
        api.refresh()

    elif sys.argv[1] in ['s', 'save', 'pdf']:
        api.save_pdf()
