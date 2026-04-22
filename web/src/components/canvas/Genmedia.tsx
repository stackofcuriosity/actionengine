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

import {
  Action,
  encodeBaseModelMessage,
  makeBlobFromChunk,
} from '@helenapankov/actionengine'

import { OrbitControls, useTexture } from '@react-three/drei'
import { makeAction } from '@/helpers/actionengine'
import * as THREE from 'three'
import React, {
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from 'react'
import { useCursor } from '@react-three/drei'
import { ActionRegistry, makeTextChunk } from '@helenapankov/actionengine'
import { levaStore, useControls } from 'leva'
import { trait } from 'koota'
import { useWorld } from 'koota/react'
import { useTrait, useTraitEffect } from 'koota/react'
import {
  GlobalUIState,
  Material,
  Mesh,
  Position,
  Rotation,
  Scale,
} from '@/helpers/genmedia/traits'
import { GlobalCursor } from '@/components/canvas/Cursor'
import { getRoundedPlaneShape } from '@/helpers/genmedia/primitives'
import {
  DrawingCanvas,
  DrawingCanvasController,
} from '@/components/canvas/DrawingCanvas'
import { SpecialInputs } from 'leva/plugin'
import { ActionEngineContext } from '@/helpers/actionengine'
import { GENERATE_CONTENT_SCHEMA } from '../../actions/chat'

const IsSelected = trait()

const kDefaultTextureUrl =
  'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mOMdtrxHwAEqwJW7xjHFQAAAABJRU5ErkJggg=='

const TextureUrl = trait(() => {
  return {
    url: kDefaultTextureUrl,
  }
})

const Progress = trait(() => {
  return {
    progress: 0,
  }
})

const registerGenerateContentAction = (registry: ActionRegistry) => {
  registry.register(
    'generate_content',
    GENERATE_CONTENT_SCHEMA,
    async (_: Action) => {},
  )
}

const registerTextToImageAction = (registry: ActionRegistry) => {
  registry.register(
    'text_to_image',
    {
      name: 'text_to_image',
      inputs: [{ name: 'request', type: '__BaseModel__' }],
      outputs: [
        { name: 'progress', type: '__BaseModel__' },
        { name: 'image', type: 'image/png' },
      ],
    },
    async (_) => {},
  )
}

interface DiffusionRequest {
  prompt: string
  num_inference_steps: number
  height: number
  width: number
  seed: number
}

const makeDiffusionRequest = (params: Partial<DiffusionRequest>) => {
  return {
    prompt: 'a fox',
    num_inference_steps: kInferenceSteps,
    height: 512,
    width: 512,
    seed: Math.floor(Math.random() * 1000000),
    ...params,
  }
}

const kInferenceSteps = 25

const kCreatePromptPrompt = `surprise me with a prompt to generate a really nice image. keep it short. only use small letters and no punctuation. only output one prompt. do NOT think much. --nothink`

const useBlobControls = () => {
  const world = useWorld()
  if (!world) {
    throw new Error('World is not available')
  }
  const [actionEngine] = useContext(ActionEngineContext)

  return useControls(
    '',
    () => {
      const get = levaStore.get
      const set = levaStore.set
      return {
        prompt: { value: 'a fox', label: 'Prompt', rows: 5 },
        position: {
          value: { x: -1, y: 1 },
          label: 'Position',
          step: 0.02,
          disabled: false,
        },
        progress: {
          value: 0,
          min: 0,
          max: kInferenceSteps,
          step: 1,
          disabled: true,
          label: 'Progress',
          render: () => false,
        },
        createPrompt: {
          label: 'Create Prompt',
          onClick: async () => {
            set({ prompt: 'Generating...' }, false)
            const action = makeAction('generate_content', actionEngine)
            await action.call()

            await action
              .getInput('api_key')
              .putAndFinalize(makeTextChunk('ollama'))
            await action
              .getInput('chat_input')
              .putAndFinalize(makeTextChunk(kCreatePromptPrompt))
            await action.getInput('system_instructions').finalize()
            await action
              .getInput('interaction_token')
              .putAndFinalize(makeTextChunk(''))
            await action.getInput('config').finalize()
            await action.getInput('tools').finalize()

            const outputNode = action.getOutput('output')
            const iterateOutput = async () => {
              outputNode.setReaderOptions({
                ordered: true,
                removeChunks: true,
                nChunksToBuffer: -1,
                timeout: -1,
              })
              const textDecoder = new TextDecoder('utf-8')
              let first = true
              for await (const chunk of outputNode) {
                const text = textDecoder.decode(chunk.data)
                if (first && text) {
                  first = false
                  set({ prompt: text }, false)
                  continue
                }
                set({ prompt: get('prompt') + text }, false)
              }
            }
            iterateOutput().then()
          },
          type: SpecialInputs.BUTTON,
          settings: {
            disabled: false,
          },
        },
        generate: {
          label: 'Generate',
          onClick: async () => {
            const selectedEntities = world.query(IsSelected)
            if (selectedEntities.length === 0) {
              console.warn('No entity selected to update texture')
              return
            }

            selectedEntities.forEach((entity, index) => {
              if (!entity || typeof entity.set !== 'function') return
              entity.set(TextureUrl, { url: kDefaultTextureUrl })
              entity.set(Progress, { progress: 0 })
            })

            const fetchImage = async (entity: any, seed: number) => {
              const action = makeAction('text_to_image', actionEngine)
              await action.call()

              await action.getInput('request').putAndFinalize({
                metadata: { mimetype: '__BaseModel__' },
                data: encodeBaseModelMessage(
                  'actions.text_to_image.DiffusionRequest',
                  makeDiffusionRequest({ prompt: get('prompt'), seed }),
                ),
              })

              const imageNode = action.getOutput('image')
              imageNode.setReaderOptions({
                ordered: false,
                removeChunks: true,
                nChunksToBuffer: -1,
                timeout: -1,
              })

              const fetchProgress = async () => {
                const progressNode = action.getOutput('progress')
                progressNode.setReaderOptions({
                  ordered: false,
                  removeChunks: true,
                  nChunksToBuffer: -1,
                  timeout: -1,
                })

                let progress = 0

                try {
                  for await (const chunk of progressNode) {
                    progress += 1
                    if (progress >= kInferenceSteps) {
                      return
                    }
                    // set({ progress }, false)
                    entity.set(Progress, { progress })
                  }
                } catch (error) {
                  console.error('Error fetching progress:', error)
                }
              }

              fetchProgress().then()

              try {
                for await (let chunk of imageNode) {
                  console.log('Chunk received:', chunk)
                  const url = URL.createObjectURL(makeBlobFromChunk(chunk))
                  if (!entity.has(TextureUrl)) {
                    return
                  }
                  entity.set(TextureUrl, { url })
                  // set({ progress: kInferenceSteps }, false)
                  entity.set(Progress, { progress: kInferenceSteps })
                }
              } catch (error) {
                console.error('Error fetching image:', error)
              }
            }

            selectedEntities.forEach((entity, index) => {
              fetchImage(entity, Math.floor(Math.random() * 1000000)).catch(
                (error) => {
                  console.error('Error in generating image:', error)
                },
              )
            })
          },
          type: SpecialInputs.BUTTON,
          settings: {
            disabled: false,
          },
        },
      }
    },
    [actionEngine],
  )
}

export const GenmediaExample = () => {
  const world = useWorld()
  useEffect(() => {
    if (!world) {
      return
    }
    const entity = world.spawn(
      GlobalCursor,
      Position,
      Rotation,
      Mesh,
      Material,
      Scale,
    )
    entity.set(Position, { x: 0, y: 0, z: 0 })
    world.add(GlobalUIState)
    return () => {
      world.remove(GlobalUIState)
    }
  }, [world])

  const [actionEngine] = useContext(ActionEngineContext)
  const [controls, setControl, getControl] = useBlobControls()

  useEffect(() => {
    if (!actionEngine || !actionEngine.actionRegistry) {
      return
    }
    registerTextToImageAction(actionEngine.actionRegistry)
    registerGenerateContentAction(actionEngine.actionRegistry)
  }, [actionEngine])

  const meshRef = useRef<THREE.Mesh>(new THREE.Mesh())

  const [imageEntities, setImageEntities] = useState([])

  useEffect(() => {
    if (!world) {
      return
    }
    const entities = [
      world.spawn(Mesh, Position, Scale, IsSelected, TextureUrl, Progress),
      world.spawn(Mesh, Position, Scale, IsSelected, TextureUrl, Progress),
      world.spawn(Mesh, Position, Scale, IsSelected, TextureUrl, Progress),
      world.spawn(Mesh, Position, Scale, IsSelected, TextureUrl, Progress),
    ]

    entities.forEach((entity, index) => {
      const xOffset = (index % 2) - 0.5
      const yOffset = -(Math.floor(index / 2) - 0.5)
      const scale = 0.9
      entity.set(Position, {
        x: xOffset,
        y: yOffset,
        z: 0,
      })
      entity.set(Scale, { x: scale, y: scale, z: 1 })
      entity.set(TextureUrl, { url: kDefaultTextureUrl })
      entity.set(Progress, { progress: 0 })
    })

    setImageEntities(entities)
    return () => {
      entities.forEach((entity) => entity && entity.destroy && entity.destroy())
    }
  }, [world])

  useEffect(() => {
    if (imageEntities.length === 0 || !controls.position) {
      return
    }
    imageEntities.forEach((entity, index) => {
      if (!entity || typeof entity.set !== 'function') return
      const xOffset = (index % 2) - 0.5
      const yOffset = -(Math.floor(index / 2) - 0.5)
      entity.set(Position, {
        x: controls.position.x + xOffset,
        y: -controls.position.y + yOffset,
        z: 0,
      })
    })
  }, [controls.position, imageEntities])

  const [orbit, setOrbit] = useState(true)

  const [hovered, hover] = useState(false)
  useCursor(hovered, 'none', 'auto')

  const canvasRef = useRef<DrawingCanvasController>(null)
  useEffect(() => {
    if (!world || !canvasRef.current) return
    canvasRef.current.entity.set(Position, { x: -0.5, y: -0.5, z: 0 })

    return () => {
      if (canvasRef.current) {
        canvasRef.current.entity.destroy()
        canvasRef.current = null
      }
    }
  }, [world])

  const generatePromptCallback = useCallback(async () => {
    if (
      !actionEngine.stream ||
      !actionEngine.actionRegistry.definitions.has('generate_content')
    ) {
      return
    }
    setControl({ prompt: 'Generating...' })
    const action = makeAction('generate_content', actionEngine)
    await action.call()

    await action.getInput('api_key').putAndFinalize(makeTextChunk('ollama'))
    await action.getInput('interaction_token').putAndFinalize(makeTextChunk(''))

    await action.getInput('system_instructions').finalize()
    await action
      .getInput('chat_input')
      .putAndFinalize(
        makeTextChunk(
          'surprise me with a prompt to generate a really nice image. keep it short. only use small letters and no punctuation. only output one prompt. do NOT think much. This prompt MUST be a simple description. --nothink',
        ),
      )

    await action.getInput('config').finalize()
    await action.getInput('tools').finalize()

    const outputNode = action.getOutput('output')
    const iterateOutput = async () => {
      outputNode.setReaderOptions({
        ordered: true,
        removeChunks: true,
        timeout: -1,
      })
      const textDecoder = new TextDecoder('utf-8')
      let first = true
      for await (const chunk of outputNode) {
        const text = textDecoder.decode(chunk.data)
        if (first && text) {
          first = false
          setControl({ prompt: text })
          continue
        }
        setControl({ prompt: getControl('prompt') + text })
      }
    }
    iterateOutput().then()
  }, [actionEngine])

  useEffect(() => {
    if (
      !actionEngine ||
      !actionEngine.stream ||
      !actionEngine.actionRegistry.definitions.has('generate_content')
    ) {
      return
    }
    let cancelled = false
    actionEngine.stream.waitUntilReady().then(() => {
      if (cancelled) {
        return
      }
      console.log('Generating initial prompt', actionEngine)
      generatePromptCallback().then()
    })
    return () => {
      cancelled = true
    }
  }, [actionEngine])

  useTraitEffect(world, GlobalUIState, (uiState) => {
    if (!uiState) {
      return
    }
    setOrbit(!uiState.orbitingBlocked)
  })

  return (
    <>
      <group>
        {orbit && <OrbitControls />}
        {imageEntities.map((entity, index) => (
          <ImageMesh key={entity.id()} entity={entity} />
        ))}

        <DrawingCanvas ref={canvasRef} width={1024} height={1024} disabled />
      </group>
    </>
  )
}

const ImageMesh = ({ entity }: { entity: any }) => {
  const [textureUrl, setTextureUrl] = useState(
    entity.get(TextureUrl)?.url || kDefaultTextureUrl,
  )
  const [progress, setProgress] = useState(entity.get(Progress)?.progress || 0)

  useTraitEffect(entity, TextureUrl, (textureUrlTrait) => {
    if (textureUrlTrait) {
      setTextureUrl(textureUrlTrait.url)
    }
  })

  useTraitEffect(entity, Progress, (progressTrait) => {
    if (progressTrait) {
      setProgress(progressTrait.progress)
    }
  })

  const texture = useTexture(textureUrl || kDefaultTextureUrl)
  const position = entity.get(Position)
  const scale = entity.get(Scale)

  if (!position || !scale) {
    return null
  }

  return (
    <mesh
      position={[
        position.x - scale.x / 2,
        position.y - scale.y / 2,
        position.z,
      ]}
      scale={[scale.x, scale.y, scale.z]}
    >
      <shapeGeometry args={[getRoundedPlaneShape(1, 1, 0.05)]} />
      <meshBasicMaterial
        color='#ffffff'
        side={THREE.DoubleSide}
        // @ts-ignore
        map={texture}
        wireframe={progress > 0 && textureUrl === kDefaultTextureUrl}
      />
    </mesh>
  )
}
