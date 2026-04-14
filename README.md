# Crossmatch Alert to Skymaps

A Python service that consume new alerts from [Boom/Babamul](https://babamul.caltech.edu/) broker and filters them by crossmatching with recent skymaps.

## Setup
```bash
git clone https://github.com/antoine-le-calloch/crossmatch-alert-to-skymaps.git
cd crossmatch-alert-to-skymaps
pip install -r requirements.txt
cp .env.default .env
```

## Configuration
Edit the `.env` file to set your configuration:
- `SKYPORTAL_URL`: The URL of your SkyPortal instance.
- `SKYPORTAL_API_KEY`: Your SkyPortal API key.
- `BOOM_KAFKA_SERVER`: The url of the Kafka broker (e.g., `babamul.umn.edu:9093`).
- `BOOM_KAFKA_USERNAME`: The username to connect to the Kafka broker.
- `BOOM_KAFKA_PASSWORD`: The password to connect to the Kafka broker.
- `BOOM_KAFKA_TOPIC`: The Kafka topic to consume alerts from (e.g., `ZTF_alerts_results`).
- `BOOM_KAFKA_FILTER`: The filter name to consume alerts from (e.g., `fast_transient_ztf`).

## Running the Service
```bash
python crossmatch_alert_to_skymaps.py
```

## Acknowledgments

The Babamul alerts broker and BOOM software infrastructure (du Laz et al. 2026) is co-developed by the California Institute of Technology and the University of Minnesota. This work acknowledges support from the National Science Foundation through AST Award No. 2432476 (PI Kasliwal; co-PI Coughlin) and leverages experience from the Zwicky Transient Facility (co-PIs Graham and Kasliwal).
