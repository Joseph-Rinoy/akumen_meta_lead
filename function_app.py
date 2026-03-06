import azure.functions as func
import logging
import json
import requests
import os
from azure.storage.queue import QueueClient
from datetime import datetime, timezone, timedelta

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
CRM_URL = os.getenv("CRM_URL")
STORAGE_CONN = os.getenv("AzureWebJobsStorage")
QUEUE_NAME = os.getenv("QUEUE_NAME")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
ALERT_EMAIL = os.getenv("ALERT_EMAIL")
FIELD_MAP = {
    "name": ["full_name", "name", "contact_name"],
    "phone": ["phone", "phone_number", "mobile", "mobile_number","contact_number"],
    "email": ["email", "email_address"],
}

@app.route(route="lead_id_obtainer", methods=["GET", "POST"])
def lead_id_obtainer(req: func.HttpRequest) -> func.HttpResponse:

    # -------------------------------
    # GET → Webhook verification
    # -------------------------------
    if req.method == "GET":
        mode = req.params.get("hub.mode")
        token = req.params.get("hub.verify_token")
        challenge = req.params.get("hub.challenge")

        if mode == "subscribe" and token == VERIFY_TOKEN:
            logging.info("Webhook verified successfully")
            return func.HttpResponse(
                challenge,
                status_code=200,
                mimetype="text/plain"
            )

        logging.warning("Webhook verification failed")
        return func.HttpResponse("Verification failed", status_code=403)

    # -------------------------------
    # POST → Push lead IDs to Queue
    # -------------------------------
    logging.info("Meta webhook hit")
    if req.method == "POST":
        try:
            try:
                payload = req.get_json()
            except ValueError:
                logging.error("Invalid JSON received")
                return func.HttpResponse("Invalid JSON", status_code=400)

            logging.info("Webhook payload received")
            logging.info(json.dumps(payload, indent=2))

            lead_ids = []

            if "entry" in payload:
                for entry in payload.get("entry", []):
                    for change in entry.get("changes", []):
                        value = change.get("value", {})
                        leadgen_id = value.get("leadgen_id")
                        if leadgen_id:
                            lead_ids.append(leadgen_id)

            elif "sample" in payload:
                value = payload.get("sample", {}).get("value", {})
                leadgen_id = value.get("leadgen_id")
                if leadgen_id:
                    lead_ids.append(leadgen_id)

            queue_client = QueueClient.from_connection_string(
                conn_str=STORAGE_CONN,
                queue_name=QUEUE_NAME
            )
            try:
                queue_client.create_queue()
            except Exception:
                pass 
            for lid in lead_ids:
                queue_client.send_message(json.dumps({"lead_id": lid}))
                

            logging.info(f"{len(lead_ids)} lead(s) pushed to queue") 

            return func.HttpResponse(
                json.dumps({"status": "queued", "lead_ids": lead_ids}),
                status_code=200,
                mimetype="application/json"
            )

        except Exception:
            logging.exception("Unhandled exception in POST processing")
            return func.HttpResponse("Server error", status_code=500)

# queue trigger
@app.queue_trigger(
    arg_name="msg",
    queue_name="meta-ad-leads-queue",
    connection="AzureWebJobsStorage"
)
def process_queue(msg: func.QueueMessage):
    logging.warning(f"hit queue trigger")
    try:
        logging.info(f"hit queue trigger")
        data = json.loads(msg.get_body().decode())
        lead_id = data.get("lead_id")

        logging.info(f"Processing lead from queue: {lead_id}")

        graph_url = f"https://graph.facebook.com/v24.0/{lead_id}"
        params = {
            "access_token": PAGE_ACCESS_TOKEN,
            "fields": "created_time,field_data"
        }

        response = requests.get(graph_url, params=params, timeout=10)
        response.raise_for_status()

        lead_data = response.json()
        logging.info("Meta Payload %s", json.dumps(lead_data, indent=2))

        cleaned_lead = extract_lead_fields(lead_data)
        logging.info("Lead processed successfully")
        logging.info("Sending to crm")
        send_to_crm(cleaned_lead)


    except Exception:
        logging.exception("Queue processing failed")
        raise  # Important: enables Azure retry


def extract_lead_fields(lead_data):
    name = None
    phone = None
    email = None
    other_details = {}

    for field in lead_data.get("field_data", []):
        field_name = field.get("name").lower()
        field_value = field.get("values", [None])[0]


        # Check against FIELD_MAP
        if field_name in FIELD_MAP["name"]:
            name = field_value

        elif field_name in FIELD_MAP["phone"]:
            phone = field_value

        elif field_name in FIELD_MAP["email"]:
            email = field_value

        else:
            other_details[field_name] = field_value

    return {
        "name": name or "No Name",
        "mobile": phone or "No Phone Number",
        "email": email or "No Email",
        "other_details": other_details if other_details else "No Other Details",
    }

def send_to_crm(payload):
    try:
        if not CRM_URL:
            logging.error("CRM_URL not configured")
            return

        logging.info("Sending payload to CRM: %s", json.dumps(payload, indent=2))

        response = requests.post(CRM_URL, json=payload, timeout=30)

        logging.info("CRM Response Status: %s", response.status_code)
        logging.info("CRM Response Body: %s", response.text)

        response.raise_for_status()

        logging.info("Lead sent to CRM successfully")

    except requests.exceptions.RequestException as e:
        error_message = str(e)

        response_body = ""
        if hasattr(e, "response") and e.response is not None:
            response_body = e.response.text

        full_error = f"{error_message} | CRM Response: {response_body}"

        logging.error(f"CRM response error: {full_error}")
        logging.exception("Failed to send lead to CRM")
        
        if "Mobile Number already exists" in full_error:
            logging.warning("Duplicate mobile number detected. Skipping failure email.")
            return

        # Send email for all other errors
        send_failure_email(payload, full_error)

def get_graph_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    data = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
    }

    response = requests.post(url, data=data)
    response.raise_for_status()

    return response.json()["access_token"]

def send_failure_email(payload, error_message):
    try:
        access_token = get_graph_token()

        url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"
        ist = timezone(timedelta(hours=5, minutes=30))
        current_time = datetime.now(ist).strftime("%d %B %Y | %I:%M:%S %p IST")
        email_body = f"""
CRM Lead Sending Failed

Time: {current_time}

Error:
{error_message}

Payload:
{json.dumps(payload, indent=2)}
"""

        message = {
            "message": {
                "subject": "🚨 CRM Lead Failed",
                "body": {
                    "contentType": "Text",
                    "content": email_body,
                },
                "toRecipients": [
                    {
                        "emailAddress": {
                            "address": ALERT_EMAIL
                        }
                    }
                ],
            }
        }

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        response = requests.post(url, headers=headers, json=message)
        response.raise_for_status()

        logging.info("Failure email sent successfully")

    except Exception as e:
        logging.exception("Failed to send failure notification email")

