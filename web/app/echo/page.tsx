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
import { useCallback, useContext, useEffect, useState } from 'react'
import { ECHO_SCHEMA } from '@/actions/echo'
import { ActionEngineContext, makeAction } from '@/helpers/actionengine'

const textDecoder = new TextDecoder()

const iterateResponse = async (
  node: AsyncNode,
  receiveMessage?: (msg: ChatMessage) => void,
) => {
  node.setReaderOptions({ ordered: true, removeChunks: true, timeout: -1 })
  const chunks: string[] = []
  for await (const chunk of node) {
    chunks.push(textDecoder.decode(chunk.data))
  }
  const message = chunks.join('')
  if (receiveMessage) {
    receiveMessage({
      text: message,
      sender: 'Echo Action',
      id: `${Date.now()}`,
    })
  }
}

export default function Page() {
  const [disableInput, setDisableInput] = useState(true)

  const [actionEngine] = useContext(ActionEngineContext)

  useEffect(() => {
    if (!actionEngine || !actionEngine.stream) {
      setDisableInput(true)
      return
    }
    const currentDisabled = disableInput
    let cancelled = false
    actionEngine.stream.waitUntilReady().then(() => {
      if (cancelled) {
        return
      }
      setDisableInput(false)
    })
    return () => {
      cancelled = true
      setDisableInput(currentDisabled)
    }
  }, [actionEngine])

  useEffect(() => {
    if (!actionEngine) {
      return
    }
    actionEngine.actionRegistry.register('echo', ECHO_SCHEMA)
  }, [actionEngine])

  const [messages, setMessages] = useState([])

  const receiveMessage = useCallback(
    (msg: ChatMessage) => {
      setMessages((prev) => [...prev, msg])
    },
    [setMessages],
  )

  const sendMessage = useCallback(
    async (msg: ChatMessage) => {
      const action = makeAction('echo', actionEngine)
      action.call().then()
      iterateResponse(action.getOutput('response'), receiveMessage).then()

      await action.getInput('text').putAndFinalize(makeTextChunk(msg.text))

      setMessages((prev) => [...prev, msg])
    },
    [setMessages, actionEngine],
  )

  return (
    <>
      <div className='mx-auto flex h-2/3 max-h-2/3 w-screen max-w-2xl flex-row justify-center'>
        <Chat
          messages={messages}
          sendMessage={sendMessage}
          disableInput={disableInput}
        />
      </div>
    </>
  )
}
