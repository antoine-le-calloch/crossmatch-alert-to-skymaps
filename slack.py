import os
import time

from dotenv import load_dotenv
from slack_sdk import WebClient
from datetime import datetime
from plot_skymaps import get_crossmatch_plot

load_dotenv()

skyportal_url = os.getenv("SKYPORTAL_URL")
slack_bot_token = os.getenv("SLACK_BOT_TOKEN")
slack_channel_name = os.getenv("SLACK_CHANNEL_NAME")

client = WebClient(token=slack_bot_token)


def get_channel_id(channel_name):
    """Get the Slack channel ID for the specified channel name."""
    channels = (
            client.conversations_list().get("channels", []) +
            client.conversations_list(types="private_channel").get("channels", [])
    )
    for channel in channels:
        if channel["name"] == channel_name:
            return channel["id"]
    return None


def delete_all_bot_messages():
    """Delete all messages sent by the bot in the specified Slack channel."""
    history = client.conversations_history(channel=slack_channel_id)
    for message in history["messages"]:
        if message.get("bot_id"):
            client.chat_delete(channel=slack_channel_id, ts=message["ts"])


slack_channel_id = get_channel_id(slack_channel_name)
if not slack_channel_id:
    exit("No slack channel found.")

delete_all_bot_messages()

def send_to_slack(obj, matching_skymaps):
    """Send a message to Slack about a new object in Skymaps localization."""
    slack_text = (
            f"*New object in Skymaps localization*\n"
            f"*Date:* {datetime.utcnow().isoformat()}\n"
            f"*Object:* <{skyportal_url}/source/{obj['id']}|{obj['id']}>\n"
            f"*Crossmatches:* \n"
    )

    client.chat_postMessage(
        channel=f"#{slack_channel_name}",
        text=slack_text,
        mrkdwn=True
    )

    for date, moc in matching_skymaps:
        client.files_upload_v2(
            channel=slack_channel_id,
            filename=f"{obj['id']}_{date}.png",
            file=get_crossmatch_plot(obj, moc),
            initial_comment=f"<{skyportal_url}/gcn_events/{date}|{date}>",
        )
    time.sleep(1.5)  # To let the files upload properly