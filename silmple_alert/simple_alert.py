import asyncio
from functools import wraps
import clickhouse_connect
from typing import Callable
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError

# token - Slack Bot Token
slack_client = AsyncWebClient(token="")
# in this channel we post message
slack_channel = ""
# it can be any db client
click_client = clickhouse_connect.get_client(host="", username="", password="")


# this is decorator for post message in Slack channel
def slack_message(alert: Callable) -> Callable:
    @wraps(alert)
    async def wrapper():
        msg = await alert()
        if msg == "":
            return
        try:
            await slack_client.chat_postMessage(channel=slack_channel, text=msg)
        except SlackApiError as e:
            # log error
            print(f"Got an error: {e.response['error']}")

    return wrapper


@slack_message
async def alert1() -> str:
    # run some useful work here
    query = ''
    result = await click_client.query(query)
    # here we process result
    # return alert message, if msg is empty - do nothing
    return "Alert #1!"


@slack_message
async def alert2() -> str:
    # run some useful work here
    query = ''
    result = await click_client.query(query)
    # here we process result
    # return alert message, if msg is empty - do nothing
    return "Alert #3!"


@slack_message
async def alert3() -> str:
    # run some useful work here
    query = ''
    result = await click_client.query(query)
    # here we process result
    # return alert message, if msg is empty - do nothing
    return "Alert #3!"


async def run_at_interval(t: float, alert: Callable):
    while True:
        await asyncio.sleep(t)
        await alert()


async def main():
    # This alert will raise every 5 sec
    a1 = asyncio.create_task(run_at_interval(5, alert1))
    # This alert will raise every 10 sec
    a2 = asyncio.create_task(run_at_interval(10, alert2))
    # This alert will raise every 15 sec
    a3 = asyncio.create_task(run_at_interval(15, alert3))
    await asyncio.wait({a1, a2, a3})


if __name__ == '__main__':
    asyncio.run(main())
