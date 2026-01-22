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
  ServerAdminSelected,
  ServerMasterSelected,
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
  HierarchyAdmin,
  HierarchyMaster,
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
  const [hierarchy, setHierarchy] = useState<{ type: "superadmin"; admins: HierarchyAdmin[] } | { type: "admin"; masters: HierarchyMaster[] } | null>(null)
  const [selectedAdminId, setSelectedAdminId] = useState<string | null>(null)
  const [selectedMasterId, setSelectedMasterId] = useState<string | null>(null)
  const [masters, setMasters] = useState<HierarchyMaster[]>([])
  const [initialPersonalChatrooms, setInitialPersonalChatrooms] = useState<ServerJoinedAdminList["chatrooms"]>([])
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
            console.log("[WS] Admin/Master joined, needs_selection:", j.needs_selection, "chatrooms:", j.chatrooms.length, "hierarchy:", j.hierarchy)
            if (j.needs_selection) {
              setNeedsSelection(true)
              const sortedChatrooms = [...j.chatrooms].sort(
                (a, b) => new Date(b.updated_time).getTime() - new Date(a.updated_time).getTime(),
              )
              setChatrooms(sortedChatrooms)
              const personalChats = sortedChatrooms.filter(c => c.room_type === "staff_bot")
              setInitialPersonalChatrooms(personalChats)
              if (j.hierarchy) {
                setHierarchy(j.hierarchy)
              }
            }
          }
        } else if (data.type === "selected") {
          const s = data as ServerSelected
          setChatId(s.chat_id)
          setNeedsSelection(false)
        } else if (data.type === "admin_selected") {
          const s = data as ServerAdminSelected
          console.log("[WS] Admin selected:", s.admin_id, "masters:", s.masters.length, "chatrooms:", s.chatrooms.length)
          setSelectedAdminId(s.admin_id)
          setMasters(s.masters)
          const sortedChatrooms = [...s.chatrooms].sort(
            (a, b) => new Date(b.updated_time).getTime() - new Date(a.updated_time).getTime(),
          )
          const personalChats = sortedChatrooms.filter(c => c.room_type === "staff_bot")
          if (personalChats.length === 0) {
            const adminPersonalChats = initialPersonalChatrooms.filter(c => {
              const adminIdFromChat = (c as any).admin_id || (c as any).user_id
              return adminIdFromChat === s.admin_id
            })
            setChatrooms([...sortedChatrooms, ...adminPersonalChats])
          } else {
            setChatrooms(sortedChatrooms)
          }
          setNeedsSelection(true)
        } else if (data.type === "master_selected") {
          const s = data as ServerMasterSelected
          console.log("[WS] Master selected:", s.master_id, "chatrooms:", s.chatrooms.length)
          setSelectedMasterId(s.master_id)
          setChatrooms(
            [...s.chatrooms].sort(
              (a, b) => new Date(b.updated_time).getTime() - new Date(a.updated_time).getTime(),
            ),
          )
          setNeedsSelection(true)
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

  const selectAdmin = useCallback(
    (adminId: string) => {
      send({ type: "select_admin", admin_id: adminId })
      setSelectedAdminId(adminId)
      setSelectedMasterId(null)
      setMasters([])
    },
    [send],
  )

  const selectMaster = useCallback(
    (masterId: string, adminId?: string) => {
      send({ type: "select_master", master_id: masterId, admin_id: adminId })
      setSelectedMasterId(masterId)
    },
    [send],
  )

  const resetLiveMessages = useCallback(() => setMessages([]), [])
  const clearCallEvent = useCallback(() => setCallEvent(null), [])
  const resetHierarchy = useCallback(() => {
    setSelectedAdminId(null)
    setSelectedMasterId(null)
    setMasters([])
  }, [])

  return {
    status,
    chatId,
    needsSelection,
    chatrooms,
    hierarchy,
    selectedAdminId,
    selectedMasterId,
    masters,
    initialPersonalChatrooms,
    messages,
    sendText,
    selectRoom,
    selectAdmin,
    selectMaster,
    resetHierarchy,
    resetLiveMessages,
    send,
    callEvent,
    clearCallEvent,
  }
}
