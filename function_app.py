import azure.functions as func
import logging
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

VERIFY_TOKEN = "my_meta_verify_123"  # same token used in Meta UI


@app.route(route="lead_id_obtainer", methods=["GET", "POST"])
def lead_id_obtainer(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(" Meta webhook hit")

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
        return func.HttpResponse(
            "Verification failed",
            status_code=403
        )

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

        # REAL webhook payload
        if "entry" in payload:
            for entry in payload.get("entry", []):
                for change in entry.get("changes", []):
                    value = change.get("value", {})
                    leadgen_id = value.get("leadgen_id")
                    if leadgen_id:
                        lead_ids.append(leadgen_id)

        # TEST webhook payload (Meta dashboard)
        elif "sample" in payload:
            value = payload.get("sample", {}).get("value", {})
            leadgen_id = value.get("leadgen_id")
            if leadgen_id:
                lead_ids.append(leadgen_id)

        # Log extracted lead IDs
        for lid in lead_ids:
            logging.info(f" Lead ID captured: {lid}")
            # TODO: Fetch lead details using Marketing API

        return func.HttpResponse(
            json.dumps({"status": "ok", "lead_ids": lead_ids}),
            status_code=200,
            mimetype="application/json"
        )
