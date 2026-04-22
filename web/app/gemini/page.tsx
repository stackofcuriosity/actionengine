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

import { AsyncNode, makeTextChunk } from '@helenapankov/actionengine'
import React, { useCallback, useContext, useEffect, useState } from 'react'
import { Leva, useControls } from 'leva'
import { usePathname, useSearchParams } from 'next/navigation'
import { ActionEngineContext, makeAction } from '@/helpers/actionengine'
import {
  GENERATE_CONTENT_SCHEMA,
  REHYDRATE_INTERACTION_SCHEMA,
} from '@/actions/chat'
import {
  rehydrateMessages,
  rehydrateThoughts,
  setChatMessagesFromAsyncNode,
} from '@/helpers/demoChats'

const setInteractionTokenFromAction = async (
  node: AsyncNode,
  setInteractionToken,
) => {
  node.setReaderOptions({ ordered: true, removeChunks: true, timeout: -1 })
  for await (const chunk of node) {
    const interactionToken = new TextDecoder('utf-8').decode(chunk.data)
    setInteractionToken(interactionToken)
  }
}

const useAuxControls = () => {
  const searchParams = useSearchParams()
  const secret = searchParams.get('q')
  return useControls('', () => {
    return {
      apiKey: {
        value: secret ? secret : '',
        label: 'API key',
      },
    }
  })
}

export default function Page() {
  const [actionEngine] = useContext(ActionEngineContext)

  const searchParams = useSearchParams()

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
    actionRegistry.register(
      'rehydrate_interaction',
      REHYDRATE_INTERACTION_SCHEMA,
    )
  }, [actionEngine])

  const [messages, setMessages] = useState([])
  const [thoughts, setThoughts] = useState([])

  const [rehydrated, setRehydrated] = useState(false)
  const interactionToken = useSearchParams().get('interaction_token') || ''
  useEffect(() => {
    if (!actionEngine.stream) {
      return
    }
    if (rehydrated) {
      return
    }
    const rehydrate = async () => {
      console.log('Rehydrating interaction with token:', interactionToken)
      const action = makeAction('rehydrate_interaction', actionEngine)
      action.call().then()

      await action
        .getInput('interaction_token')
        .putAndFinalize(makeTextChunk(interactionToken || ''))

      rehydrateMessages(
        action.getOutput('previous_messages'),
        setMessages,
      ).then()

      rehydrateThoughts(
        action.getOutput('previous_thoughts'),
        setThoughts,
      ).then()
    }
    actionEngine.stream.waitUntilReady().then(() => {
      setRehydrated(true)
      if (interactionToken) {
        rehydrate().then()
      }
    })
  }, [actionEngine, rehydrated])

  const createQueryString = useCallback(
    (name: string, value: string) => {
      const params = new URLSearchParams(searchParams.toString())
      params.set(name, value)

      return params.toString()
    },
    [searchParams],
  )

  const pathname = usePathname()

  const [nextInteractionToken, setNextInteractionToken] =
    useState<string>(interactionToken)
  useEffect(() => {
    if (!nextInteractionToken) {
      return
    }
    window.history.replaceState(
      null,
      '',
      pathname +
        '?' +
        createQueryString('interaction_token', nextInteractionToken || ''),
    )
  }, [createQueryString, nextInteractionToken, pathname])

  const sendMessage = async (msg: ChatMessage) => {
    const action = makeAction('generate_content', actionEngine)
    await action.call()

    setMessages((prev) => [...prev, msg])

    await action.getInput('api_key').putAndFinalize(makeTextChunk(apiKey))
    await action.getInput('chat_input').putAndFinalize(makeTextChunk(msg.text))
    await action.getInput('system_instructions').finalize()
    await action
      .getInput('interaction_token')
      .putAndFinalize(makeTextChunk(interactionToken || ''))
    await action.getInput('config').finalize()
    await action.getInput('tools').finalize()

    setChatMessagesFromAsyncNode(action.getOutput('output'), setMessages).then()
    setChatMessagesFromAsyncNode(
      action.getOutput('thoughts'),
      setThoughts,
    ).then()
    setInteractionTokenFromAction(
      action.getOutput('new_interaction_token'),
      setNextInteractionToken,
    ).then()
  }

  return (
    <div className='flex h-screen w-full flex-row space-x-4'>
      <div className='w-[360px] h-full bg-zinc-600'>
        <Leva oneLineLabels fill titleBar={{ drag: false }} />
      </div>
      <div className='flex flex-1 flex-row space-x-4'>
        <div className='flex flex-col w-full items-center justify-center space-y-4 py-4'>
          <Chat
            name={`${apiKey === 'ollama' ? 'Ollama' : 'Gemini'} interaction ${nextInteractionToken}`}
            messages={messages}
            sendMessage={sendMessage}
            disableInput={!enableInput}
            disabledInputMessage={disabledInputMessage}
            className='h-full max-w-full w-full'
          />
        </div>
        <div className='flex flex-col w-full items-center justify-center space-y-4 pr-4 py-4'>
          <Chat
            name='Thoughts'
            messages={thoughts}
            sendMessage={async (_) => {}}
            disableInput
            disabledInputMessage='This is a read-only chat for generated content.'
            className='h-full max-w-full w-full'
          />
        </div>
      </div>
    </div>
  )
}
