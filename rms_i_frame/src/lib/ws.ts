"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import type {
  Role,
  ClientEvent,
  ServerEvent,
  ConversationItem,
  ServerJoinedUser,
  ServerJoinedAdminList,
  ServerSelected,
  ServerTextMessage,
  ServerFileMessage,
  ServerAudioMessage,
  ServerCallIncoming,
  ServerCallRinging,
  ServerCallAccepted,
  ServerCallOffer,
  ServerCallAnswer,
  ServerCallIce,
  ServerCallEnded,
  ServerCallError,
} from "./types"

const DEFAULT_WS = process.env.NEXT_PUBLIC_WS_URL || "ws://127.0.0.1:8013/ws"

type UseChatSocketOptions = {
  token: string
  role: Role
}

export function useChatSocket({ token }: UseChatSocketOptions) {
  const [status, setStatus] = useState<"idle" | "connecting" | "open" | "closed">("idle")
  const [chatId, setChatId] = useState<string | null>(null)
  const [needsSelection, setNeedsSelection] = useState(false)
  const [chatrooms, setChatrooms] = useState<ServerJoinedAdminList["chatrooms"]>([])
  const [messages, setMessages] = useState<ConversationItem[]>([])
  const [callEvent, setCallEvent] = useState<ServerCallIncoming | ServerCallRinging | ServerCallAccepted | ServerCallOffer | ServerCallAnswer | ServerCallIce | ServerCallEnded | ServerCallError | null>(null)
  const wsRef = useRef<WebSocket | null>(null)
  const retryRef = useRef(0)
  const keepAliveRef = useRef<NodeJS.Timeout | null>(null)

  const wsUrl = useMemo(() => {
    if (!token) return ""
    const sep = DEFAULT_WS.includes("?") ? "&" : "?"
    return `${DEFAULT_WS}${sep}token=${encodeURIComponent(token)}`
  }, [token])

  const send = useCallback((payload: ClientEvent) => {
    const s = wsRef.current
    if (s && s.readyState === s.OPEN) {
      console.log("[WS] Sending:", payload.type, payload)
      s.send(JSON.stringify(payload))
    } else {
      console.warn("[WS] Cannot send - WebSocket not open", { readyState: s?.readyState })
    }
  }, [])

  const connect = useCallback(() => {
    if (!token || !wsUrl) {
      console.warn("[WS] Cannot connect: missing token or URL", { token: !!token, wsUrl })
      return
    }
    console.log("[WS] Connecting to:", wsUrl.replace(/token=[^&]+/, "token=***"))
    setStatus("connecting")
    const ws = new WebSocket(wsUrl)
    wsRef.current = ws

    ws.onopen = () => {
      console.log("[WS] Connected successfully")
      setStatus("open")
      retryRef.current = 0
      keepAliveRef.current = setInterval(() => {
        send({ type: "ping" })
      }, 25000)
    }

    ws.onmessage = (e) => {
      try {
        const data: ServerEvent = JSON.parse(e.data)
        // console.log("test data",data);

        if (data.type === "joined") {
          console.log("[WS] Received joined event:", data)
          if ((data as any).role === "user") {
            const j = data as ServerJoinedUser
            console.log("[WS] User joined, chat_id:", j.chat_id)
            setChatId(j.chat_id)
            setNeedsSelection(false)
          } else {
            const j = data as ServerJoinedAdminList
            console.log("[WS] Admin/Master joined, needs_selection:", j.needs_selection, "chatrooms:", j.chatrooms.length)
            if (j.needs_selection) {
              setNeedsSelection(true)
              setChatrooms(
                [...j.chatrooms].sort(
                  (a, b) => new Date(b.updated_time).getTime() - new Date(a.updated_time).getTime(),
                ),
              )
            }
          }
        } else if (data.type === "selected") {
          const s = data as ServerSelected
          setChatId(s.chat_id)
          setNeedsSelection(false)
        } else if (data.type === "message") {
          const m = data as ServerTextMessage | ServerFileMessage | ServerAudioMessage
          setMessages((prev) => {
            const messageId = m.message_id
            const exists = prev.some((msg) => msg.message_id === messageId)
            if (exists) {
              return prev
            }
            
            if ("is_file" in m && m.is_file) {
              if (m.kind === "file") {
                return [
                  ...prev,
                  {
                    kind: "file",
                    from: m.from,
                    file_url: m.file_url,
                    file_name: m.file_name,
                    file_type: m.file_type,
                    created_at: m.created_time,
                    message_id: m.message_id,
                  },
                ]
              } else if (m.kind === "audio") {
                return [
                  ...prev,
                  {
                    kind: "audio",
                    from: m.from,
                    audio_url: m.audio_url,
                    audio_name: m.audio_name,
                    audio_type: m.audio_type,
                    created_at: m.created_time,
                    message_id: m.message_id,
                  },
                ]
              }
            } else {
              return [
                ...prev,
                {
                  kind: "text",
                  from: m.from,
                  text: (m as any).message,
                  created_at: m.created_time,
                  message_id: m.message_id,
                  meta: (m as any).meta,
                },
              ]
            }
            return prev
          })
        } else if (data.type === "call.incoming") {
          console.log("[WS] Received call.incoming event:", data)
          setCallEvent(data as ServerCallIncoming)
        } else if (data.type === "call.ringing") {
          console.log("[WS] Received call.ringing event:", data)
          setCallEvent(data as ServerCallRinging)
        } else if (data.type === "call.accepted") {
          console.log("[WS] Received call.accepted event:", data)
          setCallEvent(data as ServerCallAccepted)
        } else if (data.type === "call.offer") {
          console.log("[WS] Received call.offer event:", data)
          setCallEvent(data as ServerCallOffer)
        } else if (data.type === "call.answer") {
          console.log("[WS] Received call.answer event:", data)
          setCallEvent(data as ServerCallAnswer)
        } else if (data.type === "call.ice") {
          console.log("[WS] Received call.ice event:", data)
          setCallEvent(data as ServerCallIce)
        } else if (data.type === "call.ended") {
          console.log("[WS] Received call.ended event:", data)
          setCallEvent(data as ServerCallEnded)
        } else if (data.type === "call.error") {
          console.error("[WS] Received call.error event:", data)
          setCallEvent(data as ServerCallError)
        }
      } catch (err) {
        console.log("[v0] WS parse error:", (err as Error).message)
      }
    }


    // ws.onclose = () => {
    //   setStatus("closed")
    //   if (keepAliveRef.current) clearInterval(keepAliveRef.current)
    //   const delay = Math.min(1000 * 2 ** retryRef.current, 15000)
    //   retryRef.current += 1
    //   setTimeout(connect, delay)
    // }

    ws.onclose = (event) => {
      console.log("[WS] Connection closed", { code: event.code, reason: event.reason, wasClean: event.wasClean })
      setStatus("closed")
      if (keepAliveRef.current) clearInterval(keepAliveRef.current)

      if (retryRef.current < 5) {
        const delay = Math.min(1000 * 2 ** retryRef.current, 15000)
        retryRef.current += 1
        console.log(`[WS] Retrying connection in ${delay}ms (attempt ${retryRef.current}/5)`)
        setTimeout(connect, delay)
      } else {
        console.error("[WS] Max retry attempts reached. Please check server connection.")
      }
    }
    ws.onerror = (error) => {
      console.error("[WS] Connection error:", error)
      try {
        ws.close()
      } catch { }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token, wsUrl])

  useEffect(() => {
    connect()
    return () => {
      if (keepAliveRef.current) clearInterval(keepAliveRef.current)
      wsRef.current?.close()
    }
  }, [connect])

  const sendText = useCallback(
    (text: string) => {
      send({ type: "message", text })
    },
    [send],
  )

  const selectRoom = useCallback(
    (id: string) => {
      send({ type: "select_chatroom", chat_id: id })
    },
    [send],
  )

  const resetLiveMessages = useCallback(() => setMessages([]), [])
  const clearCallEvent = useCallback(() => setCallEvent(null), [])

  return {
    status,
    chatId,
    needsSelection,
    chatrooms,
    messages,
    sendText,
    selectRoom,
    resetLiveMessages,
    send,
    callEvent,
    clearCallEvent,
  }
}
