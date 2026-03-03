import azure.functions as func
import logging
import json
import requests
import os
from azure.storage.queue import QueueClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
CRM_URL = os.getenv("CRM_URL")
STORAGE_CONN = os.getenv("AzureWebJobsStorage")
QUEUE_NAME = os.getenv("QUEUE_NAME")


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

        response = requests.get(graph_url, params=params, timeout=30)
        response.raise_for_status()

        lead_data = response.json()

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
        field_name = field.get("name")
        field_value = field.get("values", [None])[0]

        if field_name == "full_name":
            name = field_value
        elif field_name == "phone_number":
            phone = field_value
        elif field_name == "email":
            email = field_value
        else:
            other_details[field_name] = field_value

    return {
        "name": name or "No Name",
        "mobile": phone or "No Phone Number",
        "email": email or "No email",
        "other_details": other_details or "No Other Details",
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

    except requests.exceptions.HTTPError:
        logging.error(f"CRM response error: {response.text}")
        logging.exception("Failed to send lead to CRM")



