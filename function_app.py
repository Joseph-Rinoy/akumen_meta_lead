import azure.functions as func
import logging
import json
import requests
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")


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
    # POST → Receive leadgen payload
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

            # Call fetch function (guarded so a failure doesn't crash the webhook)
            try:
                fetch_lead_details(lead_ids)
            except Exception:
                logging.exception("Error while fetching lead details")

            return func.HttpResponse(
                json.dumps({"status": "ok", "lead_ids": lead_ids}),
                status_code=200,
                mimetype="application/json"
            )

        except Exception:
            logging.exception("Unhandled exception in POST processing")
            return func.HttpResponse("Server error", status_code=500)


def fetch_lead_details(lead_ids):
    for lid in lead_ids:
        try:
            logging.info(f"Lead ID captured: {lid}")

            if not PAGE_ACCESS_TOKEN:
                logging.error("PAGE_ACCESS_TOKEN is not set; skipping lead fetch")
                continue

            graph_url = f"https://graph.facebook.com/v24.0/{lid}"
            headers = {
                "Authorization": f"Bearer {PAGE_ACCESS_TOKEN}"
            }
            params = {
                "fields": "created_time,field_data"
            }

            try:
                response = requests.get(graph_url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException:
                logging.exception(f"Request failed for lead {lid}")
                continue

            try:
                lead_data = response.json()
            except ValueError:
                logging.exception(f"Invalid JSON received for lead {lid}")
                continue

            # ----------------------------------------
            # Extract only required fields
            # ----------------------------------------
            name = None
            phone = None
            email = None

            for field in lead_data.get("field_data", []):
                field_name = field.get("name")
                field_value = field.get("values", [None])[0]

                if field_name == "full_name":
                    name = field_value

                elif field_name == "phone_number":
                    phone = field_value

                elif field_name == "email":
                    email = field_value

            cleaned_lead = {
                "name": name,
                "phone": phone,
                "email": email
            }

            logging.info("Cleaned Lead Data:")
            logging.info(json.dumps(cleaned_lead, indent=2))

        except Exception:
            logging.exception(f"Unexpected error processing lead {lid}")
            # continue with next lead

        