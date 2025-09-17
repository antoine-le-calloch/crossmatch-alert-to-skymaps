# Followup Request Listener

A Python service that listens for skyportal new objects and filters them by crossmatching with recent skymaps.

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

## Running the Service
```bash
python crossmatch_alert_to_skymaps.py
```
