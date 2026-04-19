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

_ENABLED_PROVIDERS = {"claude", "gemini", "ollama"}


def set_enabled_providers(providers: set[str]) -> None:
    """Set the enabled providers for the generate_content action.

    Args:
        providers (set[str]): Set of provider names to enable.
    """
    global _ENABLED_PROVIDERS
    _ENABLED_PROVIDERS = providers


def is_provider_enabled(provider: str) -> bool:
    return provider in _ENABLED_PROVIDERS


def get_enabled_providers() -> set[str]:
    return _ENABLED_PROVIDERS.copy()


GENERATE_CONTENT_SCHEMA = ActionSchema(
    name="generate_content",
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
        ("new_interaction_token", "text/plain"),
    ],
)
