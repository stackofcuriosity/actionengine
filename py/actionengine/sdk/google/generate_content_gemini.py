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

import logging

from google import genai

from actionengine.actions import ActionSchema

_LOGGER = logging.getLogger(__name__)


def get_gemini_client(api_key: str):
    return genai.client.AsyncClient(
        genai.client.BaseApiClient(
            api_key=api_key,
        )
    )


GENERATE_CONTENT_GEMINI_SCHEMA = ActionSchema(
    name="generate_content_gemini",
    inputs=[
        ("api_key", "text/plain"),
        ("chat_input", "text/plain"),
        ("system_instructions", "text/plain"),
        ("interaction_token", "text/plain"),
        ("config", "__BaseModel__"),
        ("tools", "application/json"),
    ],
    outputs=[
        ("output", "text/plain"),
        ("thoughts", "text/plain"),
        ("interaction_token", "text/plain"),
    ],
)
