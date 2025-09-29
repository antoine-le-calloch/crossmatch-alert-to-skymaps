import os

from dotenv import load_dotenv
from slack_sdk import WebClient
from datetime import datetime

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
    slack_channel_id = get_channel_id(slack_channel_name)
    if not slack_channel_id:
        exit("No slack channel found.")

    history = client.conversations_history(channel=slack_channel_id)
    for message in history["messages"]:
        if message.get("bot_id"):
            client.chat_delete(channel=slack_channel_id, ts=message["ts"])


def send_to_slack(obj, matching_skymaps):
    """Send a message to Slack about a new object in Skymaps localization."""
    delete_all_bot_messages()
    slack_text = (
            f"*New object in Skymaps localization:*\n"
            f"*Date:* {datetime.utcnow().isoformat()}\n"
            f"*Object:* <{skyportal_url}/source/{obj['id']}|{obj['id']}>\n"
            f"*Crossmatches:* \n" +
            "\n".join(f"<{skyportal_url}/gcn_events/{skymap}|{skymap}>" for skymap in matching_skymaps)
    )

    client.chat_postMessage(
        channel=f"#{slack_channel_name}",
        text=slack_text,
        mrkdwn=True
    )