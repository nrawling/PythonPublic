from zeep import Client
import datetime as dt

#debug only
import pprint
from zeep.plugins import HistoryPlugin

# Output: for now CSV, later write back directly to SF
import csv

## Cvent credentials - move these to AWS environment variables
# sandbox
cvent_wsdl = 'file:///Users/xxxx/git/Python/cvent/cvent_sandbox.wsdl'
cvent_accountNumber = ''
cvent_username = ''
cvent_password = ''


refresh_window_offset = dt.timedelta(days=120)

# Debug SSL stream
soap_history = HistoryPlugin()

cvent_client = Client(wsdl=cvent_wsdl,plugins=[soap_history]) # debug Soap

cvent_session_header = cvent_client.get_type('ns0:CventSessionHeader')
cvent_response = cvent_client.service.Login(cvent_accountNumber,cvent_username,cvent_password)
cvent_session_header = cvent_response.CventSessionHeader

if (cvent_response.LoginSuccess != True):
    raise ValueError('Unable to log in to Cvent: ',cvent_response.ErrorMessage)

#print "Cvent session: ", cvent_session_header
cvent_soap_headers = {'CventSessionHeader': {'CventSessionValue': cvent_session_header}}
#print cvent_soap_headers

cvent_search_filter_type = cvent_client.get_type('ns1:Filter')
#print cvent_search_filter_type

cvent_search_filter = cvent_search_filter_type(Field = 'EventStatus', Operator= 'Equals', Value= 'Active')
#print cvent_search_filter

cvent_CvSearchObject = {'Filter': cvent_search_filter, 'SearchType': 'AndSearch'}
#print cvent_CvSearchObject

cvent_event_ids = cvent_client.service.Search(ObjectType = 'Event', CvSearchObject =  cvent_CvSearchObject, _soapheaders = cvent_soap_headers)

#print cvent_event_ids
cvent_active_events = cvent_event_ids['body']['SearchResult']['Id']

events_for_sf = []

for cvent_event in cvent_active_events:
    # Retrieve event details
    cvent_event_details = cvent_client.service.Retrieve(ObjectType = 'Event', Ids = cvent_event, _soapheaders = cvent_soap_headers)
    #print cvent_event_details

    # remember, Id is in cvent_event
    cvent_event_title = cvent_event_details['body']['RetrieveResult']['CvObject'][0]['EventTitle']
    cvent_event_code = cvent_event_details['body']['RetrieveResult']['CvObject'][0]['EventCode']
    cvent_event_status = cvent_event_details['body']['RetrieveResult']['CvObject'][0]['EventStatus']

    if cvent_event_status == 'Active':
        cvent_event_status = 1
    else:
        cvent_event_status = 0

    event_record = {'Cvent_Event_ID__c': cvent_event, 'Name': cvent_event_title, 'Event_Code__c': cvent_event_code, 'Active__c':cvent_event_status}

    events_for_sf.append(event_record)

    print "Found event: " + cvent_event_title + " (" + cvent_event_details['body']['RetrieveResult']['CvObject'][0]['Id'] + ")"

# for now, let's write these to a CSV file
#print events_for_sf
with open('/tmp/events.csv', 'w') as event_csvfile:
    fieldnames = ['Cvent_Event_ID__c', 'Name', 'Event_Code__c', 'Active__c']
    writer = csv.DictWriter(event_csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
    writer.writeheader()
    writer.writerows(events_for_sf)

# Fetch Registrations - NOTE: this is not currently filtered by event
print "Searching for updated Registrations..."
cvent_updated_registrations = cvent_client.service.GetUpdated(ObjectType = 'Registration',
                                                              StartDate =  dt.datetime.today() - refresh_window_offset,
                                                              EndDate =  dt.datetime.today() + refresh_window_offset,
                                                              _soapheaders = cvent_soap_headers)
#print cvent_updated_registrations

regs_for_sf = []
orders_for_sf = []
discounts_for_sf = []

# That just gave us a list of registration IDs, now we get to iterate over those
for cvent_registration in cvent_updated_registrations['body']['GetUpdatedResult']['Id']:
    print "Fetching information for ID: " + cvent_registration
    cvent_registration_details = cvent_client.service.Retrieve(ObjectType = 'Registration', Ids = cvent_registration, _soapheaders = cvent_soap_headers)
    if cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['EventId'] not in cvent_active_events:
        # if the event for this registration is not active, skip it
        continue
    # debugging only:
    #pr = pprint.PrettyPrinter(indent=2)
    #pr.pprint(cvent_registration_details)

# fields we need
# Id__c -- cvent id of reg
# Name -- confirmation number
# email address of billing contact
# Cvent_event__c -- Salesforce id of cvent event
# Cvent_name__c -- name of billing contact
# Cvent_school__c -- name of Account
# Description__c -- long description of registration details
#    Name: 
#    Title: 
#    School State: 
#    School Type: 
#    School Name: 
#    Primary Email: 
#    Secondary Email: 
#    Work Phone: 
#    Address:
#    City:
#    State: 
#    Zip Code: 
#    Country: 
# Registration_Date__c -- cvent RegistrationDate
# Status__c -- cvent Status
# Type__c -- cvent RegistrationType

    # remember, Id is in cvent_registration
    reg_record = {
        'Id__c': cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['Id'],
        'Name': cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['ConfirmationNumber'],
        'Cvent_event__c': cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['EventId'],
        'Cvent_email__c': cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['EmailAddress'],
        'Cvent_name__c': cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['FirstName'] + ' ' +
                         cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['LastName'],
        'Cvent_school__c': cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['Company'],
        'Description__c': cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['Id'], # have to fix this later
        'Registration_Date__c': (cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['RegistrationDate']+dt.timedelta(hours=5)).isoformat() + 'Z', # have to format this to string
        'Status__c':  cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['Status'],
        'Type__c': cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['RegistrationType']
    }

    regs_for_sf.append(reg_record)

    # Orders are in 'OrderDetail'
    order_details = cvent_registration_details['body']['RetrieveResult']['CvObject'][0]['OrderDetail']
    #pr = pprint.PrettyPrinter(indent=2)
    #pr.pprint(order_details)
    for order in order_details:
        org_record = {
            'List_Price__c': order['Amount'],
            'Amount_Due__c': order['AmountDue'],
            'Amount_Paid__c': order['AmountPaid'],
            'Cvent_Registration__c': cvent_registration,
            'Detail_Id__c': order['OrderDetailId'],
            'Item_Id__c': order['OrderDetailItemId'],
            'Name': order['OrderNumber'],
            'Product_Id__c': order['ProductId'],
            'Product_Name__c': order['ProductName']
        }

        orders_for_sf.append(org_record)

        discount_details = order['DiscountDetail']

        for discount in discount_details:
            print "Found discount"

            discount_record = {
                'Amount__c': discount['DiscountAmount'],
                'Cvent_Order__c': order['OrderDetailItemId'],
                'Id__c': discount['DiscountDetailId'],
                'Name': discount['DiscountName']
            }

            discounts_for_sf.append(discount_record)

# for now, let's write these to a CSV file
with open('/tmp/regs.csv', 'w') as regs_csvfile:
    fieldnames = ['Id__c', 'Name', 'Cvent_email__c', 'Cvent_event__c', 'Cvent_name__c', 'Cvent_school__c', 'Description__c', 'Registration_Date__c', 'Status__c', 'Type__c']
    writer = csv.DictWriter(regs_csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
    writer.writeheader()
    writer.writerows(regs_for_sf)
with open('/tmp/orders.csv', 'w') as orders_csvfile:
    fieldnames = ['List_Price__c', 'Amount_Due__c', 'Amount_Paid__c', 'Cvent_Registration__c',
                  'Detail_Id__c', 'Item_Id__c', 'Name', 'Product_Id__c', 'Product_Name__c']
    writer = csv.DictWriter(orders_csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
    writer.writeheader()
    writer.writerows(orders_for_sf)
with open('/tmp/discounts.csv', 'w') as discounts_csvfile:
    fieldnames = ['Amount__c', 'Cvent_Order__c', 'Id__c', 'Name']
    writer = csv.DictWriter(discounts_csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
    writer.writeheader()
    writer.writerows(discounts_for_sf)

# debugging
#pp = pprint.PrettyPrinter(indent=2)
#pp.pprint(soap_history.last_sent)
#pp.pprint(soap_history.last_received)
