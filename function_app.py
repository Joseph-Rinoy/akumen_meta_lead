from urllib import response
import azure.functions as func
import logging
import json
import requests
import os
from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
CRM_URL = os.getenv("CRM_URL")
STORAGE_CONN = os.getenv("AzureWebJobsStorage")
QUEUE_NAME = os.getenv("QUEUE_NAME")


@app.route(route="lead_id_obtainer", methods=["GET", "POST"])
def lead_id_obtainer(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Meta webhook hit")

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

            # Use Managed Identity when STORAGE_ACCOUNT_NAME is provided; otherwise fall back to connection string
            storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
            if storage_account_name:
                account_url = f"https://{storage_account_name}.queue.core.windows.net"
                credential = DefaultAzureCredential()
                queue_client = QueueClient(account_url=account_url, queue_name=QUEUE_NAME, credential=credential)
            else:
                queue_client = QueueClient.from_connection_string(
                    conn_str=STORAGE_CONN,
                    queue_name=QUEUE_NAME
                )
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
    queue_name="%QUEUE_NAME%",
    connection="AzureWebJobsStorage"
)
def process_queue(msg: func.QueueMessage):
    try:
        data = json.loads(msg.get_body().decode())
        lead_id = data.get("lead_id")

        logging.info(f"Processing lead from queue: {lead_id}")

        graph_url = f"https://graph.facebook.com/v24.0/{lead_id}"
        params = {
            "access_token": PAGE_ACCESS_TOKEN,
            "fields": "created_time,field_data"
        }

        response = requests.get(graph_url, params=params, timeout=10)
        if not response.ok:
            logging.error("Graph API error %s: %s", response.status_code, response.text)
            response.raise_for_status()
        lead_data = response.json()

        cleaned_lead = extract_lead_fields(lead_data)

        send_to_crm(cleaned_lead)

        logging.info("Lead processed successfully")

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
        "name": name,
        "mobile": phone,
        "email": email,
        "other_details": other_details
    }

def send_to_crm(payload):
    try:
        if not CRM_URL:
            logging.error("CRM_URL not configured")
            return

        response = requests.post(CRM_URL, json=payload, timeout=10)
        if not response.ok:
            logging.error("CRM error %s: %s", response.status_code, response.text)
        response.raise_for_status()

        logging.info("Lead sent to CRM successfully")

    except requests.exceptions.HTTPError:
        logging.error(f"CRM response error: {response.text}")
        logging.exception("Failed to send lead to CRM")



# def fetch_lead_details(lead_ids):
#     for lid in lead_ids:
#         try:
#             logging.info(f"Lead ID captured: {lid}")

#             if not PAGE_ACCESS_TOKEN:
#                 logging.error("PAGE_ACCESS_TOKEN is not set; skipping lead fetch")
#                 continue

#             graph_url = f"https://graph.facebook.com/v24.0/{lid}"
#             params = {
#                         "access_token": PAGE_ACCESS_TOKEN,
#                         "fields": "created_time,field_data"
#                     }


#             try:
#                 response = requests.get(graph_url,params=params, timeout=10)
#                 response.raise_for_status()
#             except requests.exceptions.HTTPError as e:
#                 logging.error(f"Graph API error for lead {lid}: {response.text}")
#                 continue

#             try:
#                 lead_data = response.json()
#             except ValueError:
#                 logging.exception(f"Invalid JSON received for lead {lid}")
#                 continue

#             # ----------------------------------------
#             # Extract only required fields
#             # ----------------------------------------
#             name = None
#             phone = None
#             email = None
#             other_details = {}

#             for field in lead_data.get("field_data", []):
#                 field_name = field.get("name")
#                 field_value = field.get("values", [None])[0]

#                 # Main required fields
#                 if field_name == "full_name":
#                     name = field_value

#                 elif field_name == "phone_number":
#                     phone = field_value

#                 elif field_name == "email":
#                     email = field_value

#                 # Everything else automatically goes inside other_details
#                 else:
#                     other_details[field_name] = field_value

#             cleaned_lead = {
#                 "name": name,
#                 "phone": phone,
#                 "email": email,
#                 "other_details": other_details
#             }

#             logging.info(f"Cleaned Lead Data: {json.dumps(cleaned_lead, indent=2)}")

#             send_to_crm(cleaned_lead)

#         except Exception:
#             logging.exception(f"Unexpected error processing lead {lid}")
#             # continue with next lead

# def send_to_crm(lead_data):
#     try:
#         if not CRM_URL:
#             logging.error("CRM_URL is not configured")
#             return
#         payload = {
#             "name": lead_data.get("name"),
#             "mobile": lead_data.get("phone"),  
#             "email": lead_data.get("email"),
#             "other_details": lead_data.get("other_details")
#         }
#         response = requests.post(
#             CRM_URL,
#             json=payload,
#             timeout=10
#         )

#         response.raise_for_status()

#         logging.info(f"Lead successfully sent to CRM: {payload}")

#     except requests.exceptions.HTTPError:
#         logging.error(f"CRM response error: {response.text}")
#         logging.exception("Failed to send lead to CRM")

