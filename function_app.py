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

        # Call fetch function
        fetch_lead_details(lead_ids)

        return func.HttpResponse(
            json.dumps({"status": "ok", "lead_ids": lead_ids}),
            status_code=200,
            mimetype="application/json"
        )


def fetch_lead_details(lead_ids):
    for lid in lead_ids:
        logging.info(f"Lead ID captured: {lid}")

        graph_url = f"https://graph.facebook.com/v19.0/{lid}"
        params = {
            "access_token": PAGE_ACCESS_TOKEN
        }

        response = requests.get(graph_url, params=params)
        lead_data = response.json()

        # ----------------------------------------
        # Extract only required fields
        # ----------------------------------------
        name = None
        phone = None
        email = None
        place = None

        for field in lead_data.get("field_data", []):
            field_name = field.get("name")
            field_value = field.get("values", [None])[0]

            if field_name == "full_name":
                name = field_value

            elif field_name == "phone_number":
                phone = field_value

            elif field_name == "email":
                email = field_value

            elif field_name == "city":
                place = field_value

        cleaned_lead = {
            "name": name,
            "phone": phone,
            "email": email,
            "place": place
        }

        logging.info("Cleaned Lead Data:")
        logging.info(json.dumps(cleaned_lead, indent=2))

        