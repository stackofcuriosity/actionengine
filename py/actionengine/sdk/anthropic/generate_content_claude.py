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

from actionengine.actions import ActionSchema
from pydantic import BaseModel, Field


class CreateMessageConfig(BaseModel):
    max_tokens: int = Field(
        default=10240,
        description="Maximum number of tokens to generate",
    )
    model: str = Field(
        default="claude-sonnet-4-6",
        description="Model to use for generation",
    )


GENERATE_CONTENT_CLAUDE_SCHEMA = ActionSchema(
    name="generate_content",
    inputs=[
        ("api_key", "text/plain"),
        ("chat_input", "text/plain"),
        ("system_instructions", "text/plain"),
        ("interaction_token", "text/plain"),
        ("config", CreateMessageConfig),
        ("tools", "application/json"),
    ],
    outputs=[
        ("output", "text/plain"),
        ("thoughts", "text/plain"),
        ("new_interaction_token", "text/plain"),
    ],
)
