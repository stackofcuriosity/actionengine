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

'use client'

import { Chat, ChatMessage } from '@/components/dom/Chat'
import { decode } from '@msgpack/msgpack'

import {
  Action,
  ActionRegistry,
  AsyncNode,
  makeTextChunk,
} from '@helenapankov/actionengine'
import React, { useCallback, useContext, useEffect, useState } from 'react'
import { Leva, useControls } from 'leva'
import { useSearchParams } from 'next/navigation'
import { ActionEngineContext, makeAction } from '@/helpers/actionengine'
import {
  GENERATE_CONTENT_SCHEMA,
  REHYDRATE_SESSION_SCHEMA,
} from '@/actions/chat'
import { setChatMessagesFromAsyncNode } from '@/helpers/demoChats'

interface DeepResearchAction {
  id: string
  type: string
}

const iterateLogs = async (node: AsyncNode, setThoughts) => {
  console.log('Iterating logs @', node.getId())
  node.setReaderOptions({ ordered: true, removeChunks: true, timeout: -1 })
  const textDecoder = new TextDecoder('utf-8')
  let idx = 0
  for await (const chunk of node) {
    const text = textDecoder.decode(chunk.data)
    const id = `${node.getId()}-${idx.toString(36)}`
    setThoughts((prev) => [...prev, { text, sender: 'Agent', id }])
    ++idx
  }
}

const observeNestedActions = async (
  actionsNode: AsyncNode,
  cb?: (action: DeepResearchAction) => Promise<void>,
) => {
  actionsNode.setReaderOptions({
    ordered: true,
    removeChunks: true,
    timeout: -1,
  })
  for await (const chunk of actionsNode) {
    const [model, data] = decode(chunk.data) as [string, Uint8Array]
    const action = decode(data) as DeepResearchAction
    console.log('Observing nested action:', model, action)
    if (cb !== undefined) {
      cb(action).then()
    }
  }
}

const useAuxControls = () => {
  const searchParams = useSearchParams()
  const secret = searchParams.get('q')

  return useControls(
    '',
    () => {
      return {
        apiKey: {
          value: secret ? secret : '',
          label: 'API key',
        },
      }
    },
    [],
  )
}

const registerDeepResearchAction = (registry: ActionRegistry) => {
  registry.register(
    'deep_research',
    {
      name: 'deep_research',
      inputs: [
        { name: 'api_key', type: 'text/plain' },
        { name: 'topic', type: 'text/plain' },
      ],
      outputs: [
        { name: 'report', type: 'text/plain' },
        { name: 'actions', type: '__BaseModel__' },
        { name: 'user_log', type: 'text/plain' },
      ],
    },
    async (_: Action) => {},
  )
}

export default function Page() {
  const [actionEngine] = useContext(ActionEngineContext)

  const [controls] = useAuxControls()

  const [streamReady, setStreamReady] = useState(false)
  useEffect(() => {
    if (!actionEngine || !actionEngine.stream) {
      setStreamReady(false)
      return
    }
    const current = streamReady
    let cancelled = false
    actionEngine.stream.waitUntilReady().then(() => {
      if (cancelled) {
        return
      }
      setStreamReady(true)
    })
    return () => {
      cancelled = true
      setStreamReady(current)
    }
  }, [actionEngine])

  const apiKey = controls.apiKey
  const enableInput = !!apiKey && streamReady
  const disabledInputMessage = !streamReady
    ? 'Waiting for connection...'
    : 'Please enter your API key'

  useEffect(() => {
    const { actionRegistry } = actionEngine
    if (!actionRegistry) {
      return
    }
    actionRegistry.register('generate_content', GENERATE_CONTENT_SCHEMA)
    actionRegistry.register('rehydrate_session', REHYDRATE_SESSION_SCHEMA)
    registerDeepResearchAction(actionRegistry)
  }, [actionEngine])

  const [messages, setMessages] = useState([])
  const [thoughts, setThoughts] = useState([])

  const observeActionCallback = useCallback(
    async (action: DeepResearchAction) => {
      const logNode = actionEngine.nodeMap.getNode(`${action.id}#user_log`)
      iterateLogs(logNode, setThoughts).then()
    },
    [actionEngine],
  )

  const sendMessage = async (msg: ChatMessage) => {
    if (msg.text.startsWith('/get')) {
      const nodeId = msg.text.split(' ')[1]
      if (!nodeId) {
        setThoughts((prev) => [
          ...prev,
          { text: 'Usage: /get [nodeId]', sender: 'System', id: Date.now() },
        ])
        return
      }
      if (!actionEngine.nodeMap.hasNode(nodeId)) {
        setThoughts((prev) => [
          ...prev,
          {
            text: `Node ${nodeId} not found`,
            sender: 'System',
            id: Date.now(),
          },
        ])
        return
      }
      setThoughts((prev) => [
        ...prev,
        { text: `Fetching node ${nodeId}:`, sender: 'System', id: Date.now() },
      ])
      const node = actionEngine.nodeMap.getNode(nodeId)
      setChatMessagesFromAsyncNode(node, setThoughts).then()
      return
    }
    const action = makeAction('deep_research', actionEngine)
    action.call().then()

    setMessages((prev) => [...prev, msg])

    const apiKeyNode = action.getInput('api_key')
    await apiKeyNode.putAndFinalize(makeTextChunk(apiKey))

    const topicNode = action.getInput('topic')
    await topicNode.putAndFinalize(makeTextChunk(msg.text))

    setChatMessagesFromAsyncNode(action.getOutput('report'), setMessages).then()
    observeNestedActions(
      action.getOutput('actions'),
      observeActionCallback,
    ).then()
  }

  return (
    <>
      <div className='flex h-screen w-full flex-row space-x-4'>
        <div className='w-[360px] h-full bg-zinc-600'>
          <div className='w-full h-1/3'>
            <Leva oneLineLabels flat fill titleBar={{ drag: false }} />
          </div>
        </div>
        <div className='flex flex-1 flex-row space-x-4'>
          <div className='flex w-full flex-col items-center justify-center space-y-4 py-4'>
            <Chat
              name='Deep Research'
              messages={messages}
              sendMessage={sendMessage}
              disableInput={!enableInput}
              disabledInputMessage={disabledInputMessage}
              className='h-full max-w-full w-full'
            />
          </div>
          <div className='flex w-full flex-col items-center justify-center space-y-4 pr-4 py-4'>
            <Chat
              name='Debug messages'
              messages={thoughts}
              sendMessage={async (_) => {}}
              disableInput
              disabledInputMessage='This is a read-only chat for generated content.'
              className='h-full max-w-full w-full'
            />
          </div>
        </div>
      </div>
    </>
  )
}
