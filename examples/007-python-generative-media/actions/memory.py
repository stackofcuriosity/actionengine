import asyncio
import os
import random
import traceback

import actionengine
from ollama import AsyncClient, Options as OllamaOptions
from redis import Redis


def get_redis_client():
    if not hasattr(get_redis_client, "_redis_client"):
        get_redis_client._redis_client = Redis(
            os.environ.get("REDIS_HOST", "localhost")
        )
    return get_redis_client._redis_client


INFER_UPDATED_FACTS_SCHEMA = actionengine.ActionSchema(
    name="infer_updated_facts",
    description=(
        "Extracts facts from the given conversation, "
        "updating existing ones and adding new ones in the store."
    ),
    inputs=[
        ("session_token", "text/plain"),
        ("fact_node_id", "text/plain"),
    ],
    outputs=[("facts", "text/plain")],
)


SYSTEM_INSTRUCTIONS = """You are a digital assistant that extracts and updates 
facts from conversations to form a memory footprint about the user. The facts 
you extract should be relevant to the user's current situation and the 
conversation so far, as well as general characteristics of the user.

Your actual task is to analyze the existing facts in the store and extract 
new ones or update existing ones using the latest conversation data. You will
be provided with a list of existing facts. These
will be enclosed between <fact> and </fact> tags, and each fact will be on a 
separate line. Example:

<fact>The user is a software engineer.</fact>
<fact>The user mentioned liking long walks under overcast skies.</fact>

You will then be provided with a conversation between the user 
and the assistant, which will end with a message from the user "<now-facts>", 
and you must provide as your reply a list of facts with both new and 
updated facts. Facts worthy of adding include user's name, birthday, 
location, social relations, interests, and any other information that
would help future assistants in understanding the user better.

Note, facts do not have to be updated if the only difference
is their wording, but not their meaning or substance. You must 
provide a list of facts in the same format as the example, with 
each fact on a separate line and enclosed in <fact> and </fact> tags without 
any other text. Existing facts should be preserved (updated as needed) and 
new ones should be added.

Existing facts:
{existing_facts}
"""


async def make_ollama_stream(instruction: str, previous_messages: list[str]):
    messages = [
        {"role": "system", "content": instruction},
    ]
    message_idx = 0
    for message in previous_messages:
        role = "user" if message_idx % 2 == 0 else "assistant"
        if len(message) > 500 and role == "assistant":
            message = message[:500] + "[assistant response truncated]"

        messages.append(
            {
                "role": "user" if message_idx % 2 == 0 else "assistant",
                "content": message,
            },
        )
        message_idx += 1

    messages.append({"role": "user", "content": "<now-facts>"})
    ollama_client = AsyncClient()

    stream = await ollama_client.chat(
        model="deepseek-r1:8b",
        messages=messages,
        stream=True,
        think=True,
        options=OllamaOptions(seed=random.randint(0, 2**31 - 1)),
    )

    async for chunk in stream:
        if "content" in chunk["message"]:
            yield chunk["message"]["content"]


async def infer_updated_facts(action: actionengine.Action):
    try:
        session_token, fact_node_id = await asyncio.gather(
            action["session_token"].consume(timeout=10.0),
            action["fact_node_id"].consume(timeout=10.0),
        )

        print(
            f"Running infer_updated_facts with session_token: {session_token}"
        )
        redis_client = get_redis_client()
        fact_store = redis_client.get(fact_node_id) or b""
        fact_store = fact_store.decode("utf-8")
        print(f"Loaded fact store for session {session_token}: {fact_store}")
        facts = fact_store.splitlines()

        print(f"Loaded {len(facts)} existing facts for session {session_token}")

        formatted_existing_facts = "[no facts]"
        if facts:
            formatted_existing_facts = "\n".join(
                f"<fact>{fact}</fact>" for fact in facts
            )

        instruction = SYSTEM_INSTRUCTIONS.format(
            existing_facts=formatted_existing_facts
        )

        rehydrate_session = (
            action.get_registry()
            .make_action("rehydrate_session", node_map=action.get_node_map())
            .run()
        )
        await asyncio.gather(
            rehydrate_session["session_token"].put_and_finalize(session_token),
        )
        previous_messages = []
        async for message in rehydrate_session["previous_messages"]:
            previous_messages.append(message)

        await rehydrate_session.wait_until_complete()

        reply_parts = []
        async for chunk in make_ollama_stream(instruction, previous_messages):
            reply_parts.append(chunk)

        new_facts = []
        reply = "".join(reply_parts)
        reply = reply.strip()
        for line in reply.splitlines():
            if not (line.startswith("<fact>") and line.endswith("</fact>")):
                continue
            fact = line[6:-7]
            if isinstance(fact, bytes):
                fact = fact.decode()
            new_facts.append(fact)
            await action["facts"].put(fact)

        redis_client.set(fact_node_id, "\n".join(new_facts).encode("utf-8"))

        await action["facts"].finalize()
    except Exception as e:
        traceback.print_exc()
