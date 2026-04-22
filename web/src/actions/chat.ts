/**
 * Copyright 2026 The Action Engine Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ChatMessage } from '@/components/dom/Chat'

type MessageSetter = (fn: (prev: ChatMessage[]) => ChatMessage[]) => void

export const REHYDRATE_INTERACTION_SCHEMA = {
  name: 'rehydrate_interaction',
  inputs: [{ name: 'interaction_token', type: 'text/plain' }],
  outputs: [
    { name: 'previous_messages', type: 'text/plain' },
    { name: 'previous_thoughts', type: 'text/plain' },
  ],
}

export const GENERATE_CONTENT_SCHEMA = {
  name: 'generate_content',
  inputs: [
    { name: 'api_key', type: 'text/plain' },
    { name: 'chat_input', type: 'text/plain' },
    { name: 'system_instructions', type: 'text/plain' },
    { name: 'interaction_token', type: 'text/plain' },
    { name: 'config', type: '__BaseModel__' },
    { name: 'tools', type: 'application/json' },
  ],
  outputs: [
    { name: 'output', type: 'text/plain' },
    { name: 'thoughts', type: 'text/plain' },
    { name: 'new_interaction_token', type: 'text/plain' },
  ],
}
