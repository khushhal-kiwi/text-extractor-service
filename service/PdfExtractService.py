from .main import extractDataFromPdf
from util.PdfExtractHelper import extractPdfFromEncodedData
import requests
import boto3
import json

# Initialize SQS client
sqs = boto3.client('sqs', region_name="ap-south-1")

# URL of the SQS queue
queue_url = 'https://sqs.ap-south-1.amazonaws.com/032270126675/EmailDataExtractorQueue'


def extractData(parsePdfRequest):
    extractPdfFromEncodedData(parsePdfRequest.encodedPdfData, parsePdfRequest.pdfPassword)
    filePath = "s3://jar-artefacts-preprod/email-preprod/attachement.pdf"
    return extractDataFromPdf(filePath)


def process_message(message):
    try:
        # Process the message
        print("Received message:", message)
        # For example, you can parse and handle the message content here
    except Exception as e:
        print(message)
        print("Error processing message:", e)

def get_pdf_data_from_message_id(message_id):
    url = "https://api-preprod.gokiwi.in/emailer/user/email/info?messageId="+message_id

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    return response.json()["data"]["attachement"]

def make_callback_to_emailer(extracted_data):
    url = "http://localhost:8099/emailer/callback/email/bill-statement"
    payload = json.dumps(extracted_data)
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)


def extractDataFromEmailQueue():
    while True:
        # Receive messages from the queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=4,
            WaitTimeSeconds=5  # Long polling
        )
        
        # Check if there are messages
        if 'Messages' in response:
            for message in response['Messages']:
                try:
                    # Process each message
                    message_body = json.loads(message['Body'])
                    process_message(message_body)

                    message_id = message_body["messageId"]
                    # user_id = message_body["userId"]
                    user_id = "3845d441-dc08-49eb-a334-5b1916e57ee0";
                    
                    pdfData = get_pdf_data_from_message_id(message_id)

                    # password_array = []
                    # for passwords in password_array:
                    #     pass;

                    try:
                        extractPdfFromEncodedData(pdfData, "AKSH0312", message_id)
                    except Exception as e:
                        extractPdfFromEncodedData(pdfData, "AJAY0903", message_id)
                        
                    filePath = "s3://jar-artefacts-preprod/email-preprod/"+message_id+".pdf"
                    data = {"email_data" : extractDataFromPdf(filePath), "bank_name" : "AXIS", "user_id" : user_id}
                    print("Make callback to Emailer")
                    make_callback_to_emailer(data)
                    
                    # Delete the message from the queue
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except json.JSONDecodeError as e:
                    print(message)
                    print("Error decoding JSON:", e)
                    # Delete the message from the queue if it's not in a valid JSON format
                except Exception as e:
                    print("Error processing message:", e)
                    # Handle other exceptions here
        else:
            # No messages received, continue polling
            print("No messages received. Waiting...")
