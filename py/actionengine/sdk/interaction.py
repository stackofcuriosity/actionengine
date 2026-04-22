# Copyright 2026 The Action Engine Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import base64
import os
from typing import Any

from actionengine import redis as ae_redis
from actionengine.actions import Action
from actionengine.data import Chunk, ChunkMetadata
from actionengine.logging import get_logger
from pydantic import BaseModel

_LOGGER = get_logger().getChild("sdk.interaction")


def _get_redis_client():
    if not hasattr(_get_redis_client, "_redis_client"):
        _get_redis_client._redis_client = ae_redis.Redis.connect(
            os.environ.get("REDIS_HOST", "localhost")
        )
    return _get_redis_client._redis_client


def _get_text_chunk(text: str):
    return Chunk(
        metadata=ChunkMetadata(mimetype="text/plain"),
        data=text.encode("utf-8"),
    )


def make_token_for_interaction_info(
    interaction_id: str,
    next_message_seq: int,
    next_thought_seq: int,
) -> str:
    redis_client = _get_redis_client()
    interaction_token = base64.urlsafe_b64encode(os.urandom(6)).decode("utf-8")
    redis_client.set(
        interaction_token,
        f"{interaction_id}:{next_message_seq}:{next_thought_seq}",
    )
    return interaction_token


async def resolve_token_to_id_and_seqs(
    token: str = "",
) -> tuple[str | None, int, int]:
    redis_client = _get_redis_client()

    interaction_id = base64.urlsafe_b64encode(os.urandom(6)).decode("utf-8")
    next_message_seq = 0
    next_thought_seq = 0

    if not token:
        _LOGGER.debug(
            "No interaction token provided, nothing to resolve. "
            "Generating a new interaction ID.",
        )
        return interaction_id, next_message_seq, next_thought_seq

    try:
        interaction_info = await asyncio.to_thread(redis_client.get, token)
    except Exception:
        interaction_info = None
        _LOGGER.exception(
            f"Error resolving interaction token {token} from Redis"
        )

    if not interaction_info:
        _LOGGER.debug(f"interaction token {token} not found in Redis.")
        return None, next_message_seq, next_thought_seq

    _LOGGER.debug(
        f"Resolved interaction with token: {token}, info: {interaction_info}"
    )

    interaction_info_parts = interaction_info.split(":")
    interaction_id = interaction_info_parts[0]

    next_message_seq = int(interaction_info_parts[1])
    next_thought_seq = int(interaction_info_parts[2])

    return interaction_id, next_message_seq, next_thought_seq


async def rehydrate_interaction(action: Action):
    _LOGGER.debug(f"Running rehydrate_interaction {action.get_id()}.")
    interaction_token = await action["interaction_token"].consume()
    if interaction_token:
        interaction_id, next_message_seq, next_thought_seq = (
            await resolve_token_to_id_and_seqs(interaction_token)
        )
    else:
        interaction_id = None
        next_message_seq = 0
        next_thought_seq = 0

    if interaction_id is None:
        await action["previous_messages"].finalize()
        await action["previous_thoughts"].finalize()
        return

    redis_client = _get_redis_client()

    # message offset is 2x the thought offset, because there is a user input
    # and a model output for each turn, but only one thought message per turn.
    message_offset = max(0, next_message_seq - 20)
    thought_offset = max(0, next_thought_seq - 10)

    message_store = ae_redis.ChunkStore(
        redis_client,
        f"{interaction_id}:messages",
        -1,  # no TTL
    )
    thought_store = ae_redis.ChunkStore(
        redis_client,
        f"{interaction_id}:thoughts",
        -1,  # no TTL
    )

    try:
        for idx in range(message_offset, next_message_seq):
            await action["previous_messages"].put(message_store.get(idx))

        for idx in range(thought_offset, next_thought_seq):
            await action["previous_thoughts"].put(thought_store.get(idx))
    finally:
        await action["previous_messages"].finalize()
        await action["previous_thoughts"].finalize()

    _LOGGER.debug(
        f"Rehydrated interaction {interaction_id} with {next_message_seq} messages "
        f"and {next_thought_seq} thoughts."
    )


async def save_turn(
    interaction_id: str,
    chat_input: str,
    output: str,
    thought: str,
    next_message_seq: int = 0,
    next_thought_seq: int = 0,
):
    _LOGGER.debug(
        f"Saving message turn for interaction {interaction_id}, "
        f"next_message_seq: {next_message_seq}, next_thought_seq: {next_thought_seq}"
    )
    redis_client = _get_redis_client()
    message_store = ae_redis.ChunkStore(
        redis_client,
        f"{interaction_id}:messages",
        -1,  # no TTL
    )

    await asyncio.to_thread(
        message_store.put, next_message_seq, _get_text_chunk(chat_input)
    )
    await asyncio.to_thread(
        message_store.put, next_message_seq + 1, _get_text_chunk(output)
    )
    next_message_seq += 2

    if thought:
        thought_store = ae_redis.ChunkStore(
            redis_client,
            f"{interaction_id}:thoughts",
            -1,  # no TTL
        )
        await asyncio.to_thread(
            thought_store.put, next_thought_seq, _get_text_chunk(thought)
        )
        next_thought_seq += 1

    interaction_token = await asyncio.to_thread(
        make_token_for_interaction_info,
        interaction_id,
        next_message_seq,
        next_thought_seq,
    )

    return interaction_token


class Message(BaseModel):
    role: str
    content: str | dict[str, Any]
