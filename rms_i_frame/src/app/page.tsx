
"use client"

import useSWR from "swr"
import { Suspense } from "react"
// import { TokenGate } from "@/components/token-gate"
import { useChatSocket } from "@/lib/ws"
import { getChatroom } from "@/lib/api"
import { useCallback, useEffect, useMemo, useState } from "react"
import type { ConversationItem } from "@/lib/types"
import { MessageList } from "@/components/chat/message-list"
import { Composer } from "@/components/chat/composer"
import { useSearchParams } from "next/navigation"
import { useWebRTC } from "@/hooks/use-webrtc"
import { CallUI } from "@/components/call/call-ui"
import { Phone } from "lucide-react"
import { useToast } from "@/hooks/use-toast"

export default function UserPage() {

  return (
    <Suspense fallback={null}>
      <UserPageInner />
    </Suspense>
  )
}
const STATIC_USER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2OTRjMWM5NmYxYTcwOWU3NDM0ZDBhNmIiLCJuYW1lIjoiUEVSU09OQUwiLCJwaG9uZSI6IjkwOTA5MDkwOTAiLCJ1c2VyTmFtZSI6IlBFUlNPTkFMIiwicm9sZSI6IjY0YjYzNzU1YzcxNDYxYzUwMmVhNDcxNyIsImRldmljZUlkIjoiMWU0NTA3MTAtODVhNi00ZjVjLWFmMTYtNTQxNTAwOTM0YjU3IiwiZGV2aWNlVHlwZSI6ImRlc2t0b3AiLCJzZXF1ZW5jZSI6MjAwNTEsImlhdCI6MTc2NzkzNTE2MSwiZXhwIjoxNzY4NTM5OTYxfQ.m0is2qMhwv9e6IpCgDZQxnB_HuQjMz29ZDb_7lrXQj0"

function UserPageInner() {
  const search = useSearchParams()
  const token =
    search.get("token") ||
    search.get("t") ||
    search.get("jwt") ||
    search.get("auth") ||
    STATIC_USER_TOKEN ||
    null

  if (!token) {
    return (
      <main className="h-[100dvh] w-full max-w-full mx-auto flex flex-col bg-[#e5ddd5] dark:bg-[#0b141a]">
        <header className="bg-[#008069] dark:bg-[#202c33] px-4 py-3 shadow-sm">
          <div className="text-sm font-medium text-[#ffffff] dark:text-[#e9edef]">
            Status: blocked (no token)
          </div>
        </header>
        <div className="p-4 text-sm text-[#667781] dark:text-[#8696a0] text-center flex-1 flex items-center justify-center">
          Something went wrong !!
        </div>
      </main>
    )
  }

  return <UserView token={token} />
}


function UserView({ token }: { token: string }) {
  const { status, chatId, messages, sendText, resetLiveMessages, send, callEvent, clearCallEvent } = useChatSocket({ token, role: "user" as any })
  const { toast } = useToast()
  const [isSearchOpen, setIsSearchOpen] = useState(false)
  const [searchQuery, setSearchQuery] = useState("")
  const [optimisticMessages, setOptimisticMessages] = useState<ConversationItem[]>([])

  const {
    callState,
    isInitiator,
    isRemoteAudioEnabled,
    showIncomingCall,
    micPermission,
    speakerPermission,
    startCall,
    acceptCall,
    endCall,
    handleCallIncoming,
    handleCallRinging,
    handleCallAccepted,
    handleCallOffer,
    handleCallAnswer,
    handleIceCandidate,
    toggleMute,
    toggleRemoteAudio,
    localAudioRef,
    remoteAudioRef,
  } = useWebRTC({
    chatId,
    role: "user",
    send,
  })

  useEffect(() => {
    if (!callEvent) return

    console.log("[User] Received call event:", callEvent.type, callEvent)

    if (callEvent.type === "call.incoming") {
      console.log("[User] Processing call.incoming")
      handleCallIncoming(callEvent.call_id)
      clearCallEvent()
    } else if (callEvent.type === "call.ringing") {
      console.log("[User] Processing call.ringing")
      handleCallRinging(callEvent.call_id)
      clearCallEvent()
    } else if (callEvent.type === "call.accepted") {
      console.log("[User] Processing call.accepted")
      handleCallAccepted()
      clearCallEvent()
    } else if (callEvent.type === "call.offer") {
      console.log("[User] Processing call.offer")
      handleCallOffer(callEvent.sdp)
      clearCallEvent()
    } else if (callEvent.type === "call.answer") {
      console.log("[User] Processing call.answer")
      handleCallAnswer(callEvent.sdp)
      clearCallEvent()
    } else if (callEvent.type === "call.ice") {
      console.log("[User] Processing call.ice")
      handleIceCandidate(callEvent.candidate)
      clearCallEvent()
    } else if (callEvent.type === "call.ended") {
      console.log("[User] Processing call.ended - call was ended")
      console.warn("[User] Call ended. This might indicate:")
      console.warn("  - Master didn't accept the call")
      console.warn("  - Master ended the call")
      console.warn("  - Connection issue")
      console.warn("  - Server timeout")
      endCall()
      clearCallEvent()
    } else if (callEvent.type === "call.error") {
      console.error("[User] Call error:", callEvent.error)
      if (callEvent.error === "target_offline") {
        toast({
          title: "Call Error",
          description: "Master is offline or not connected. Please try again later.",
          variant: "destructive",
        })
      } else {
        toast({
          title: "Call Error",
          description: callEvent.error || "An error occurred during the call.",
          variant: "destructive",
        })
      }
      endCall()
      clearCallEvent()
    }
  }, [callEvent, handleCallIncoming, handleCallRinging, handleCallAccepted, handleCallOffer, handleCallAnswer, handleIceCandidate, endCall, clearCallEvent])

  const { data, isLoading } = useSWR(chatId ? ["history", chatId, token] : null, ([, id, t]) => getChatroom(t, id), {
    revalidateOnFocus: false,
  })

  const historyItems: ConversationItem[] = useMemo(() => {
    if (!data?.conversation) return []
    return data.conversation.map((c: any) => {
      const from = c.from === "agent" || c.from === "superadmin" ? "admin" : c.from
      if (c.type === "file") {
        return {
          kind: "file",
          from,
          file_url: c.file_url,
          file_name: c.file_name || "File",
          file_type: c.file_type || "",
          created_at: c.created_at,
        } as ConversationItem
      }
      if (c.type === "audio") {
        return {
          kind: "audio",
          from,
          audio_url: c.audio_url,
          audio_name: "Audio",
          audio_type: "",
          created_at: c.created_at,
        } as ConversationItem
      }
      return { kind: "text", from, text: c.text, created_at: c.created_at } as ConversationItem
    })
  }, [data])

  useEffect(() => {
    resetLiveMessages()
  }, [chatId, resetLiveMessages])


  const normalizedLive = useMemo(() => {
    return messages.map((m) => ({
      ...m,
      from: m.from === "superadmin" ? "admin" : m.from,
      status: "delivered" as const,
    }))
  }, [messages])

  const combined = useMemo(() => [...historyItems, ...optimisticMessages, ...normalizedLive], [historyItems, optimisticMessages, normalizedLive])

  const addOptimisticMessage = useCallback((message: ConversationItem) => {
    setOptimisticMessages((prev) => [...prev, { ...message, status: "sending" }])
  }, [])

  useEffect(() => {
    if (normalizedLive.length > 0 && optimisticMessages.length > 0) {
      setOptimisticMessages((prev) => {
        const newMessages = prev.filter((optMsg) => {
          const recentServerMsg = normalizedLive.find((serverMsg) => {
            if (optMsg.kind === "text" && serverMsg.kind === "text") {
              return optMsg.text === serverMsg.text &&
                Math.abs(new Date(optMsg.created_at).getTime() - new Date(serverMsg.created_at).getTime()) < 5000
            }
            if (optMsg.kind === "file" && serverMsg.kind === "file") {
              return optMsg.file_name === serverMsg.file_name &&
                Math.abs(new Date(optMsg.created_at).getTime() - new Date(serverMsg.created_at).getTime()) < 5000
            }
            return false
          })
          return !recentServerMsg
        })
        return newMessages
      })
    }
  }, [normalizedLive, optimisticMessages])

  const getStatusText = () => {
    switch (status) {
      case "connecting":
        return "Connecting..."
      case "open":
        return "Online"
      case "closed":
        return "Offline"
      default:
        return "Connecting..."
    }
  }

  return (
    <main className="h-[100dvh] w-full max-w-full mx-auto flex flex-col bg-[#e5ddd5] dark:bg-[#0b141a]">
      <header
        className="bg-[#008069] dark:bg-[#202c33] px-4 py-3 flex items-center justify-between shadow-sm"
        role="banner"
      >
        <div className="flex items-center gap-3 flex-1 min-w-0">
          <div className="flex-shrink-0">
            <div className="w-10 h-10 rounded-full bg-gradient-to-br from-[#008069] to-[#006b58] dark:from-[#53bdeb] dark:to-[#008069] flex items-center justify-center overflow-hidden shadow-sm">
              <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="text-white"><path d="M12 6V2H8" /><path d="M15 11v2" /><path d="M2 12h2" /><path d="M20 12h2" /><path d="M20 16a2 2 0 0 1-2 2H8.828a2 2 0 0 0-1.414.586l-2.202 2.202A.71.71 0 0 1 4 20.286V8a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2z" /><path d="M9 11v2" /></svg>
            </div>
          </div>
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <h1 className="text-base font-semibold text-[#ffffff] dark:text-[#e9edef] truncate">
                Support Team
              </h1>
              <span
                className="flex-shrink-0"
                aria-label="Verified business account"
                title="Verified business account"
              >
                <svg
                  width="16"
                  height="16"
                  viewBox="0 0 16 16"
                  fill="none"
                  xmlns="http://www.w3.org/2000/svg"
                  className="text-[#53bdeb]"
                  aria-hidden="true"
                >
                  <path
                    d="M8 0C3.6 0 0 3.6 0 8s3.6 8 8 8 8-3.6 8-8-3.6-8-8-8zm4.3 6.1l-4.8 4.8c-.2.2-.5.2-.7 0L3.7 7.5c-.2-.2-.2-.5 0-.7.2-.2.5-.2.7 0L7 9.1l4.6-4.6c.2-.2.5-.2.7 0 .2.2.2.5 0 .6z"
                    fill="currentColor"
                  />
                </svg>
              </span>
            </div>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span
                className={`text-xs ${status === "open"
                    ? "text-[#ffffff] dark:text-[#8696a0]"
                    : "text-[#ffffff]/80 dark:text-[#8696a0]"
                  }`}
                aria-live="polite"
                aria-atomic="true"
              >
                {getStatusText()}
              </span>
              {status === "open" && (
                <span className="w-2 h-2 rounded-full bg-[#53bdeb] animate-pulse" aria-hidden="true" />
              )}
            </div>
          </div>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          {!isSearchOpen && (
            <button
              onClick={() => {
                console.log("[User] Call button clicked", { chatId, callState, status })
                startCall()
              }}
              disabled={!chatId || callState !== "idle"}
              className="p-2 text-[#ffffff] dark:text-[#8696a0] hover:bg-[#008069]/80 dark:hover:bg-[#313d45] rounded-full transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              aria-label="Call master"
              title="Call master"
            >
              <Phone className="w-5 h-5" aria-hidden="true" />
            </button>
          )}
          {isSearchOpen ? (
            <div className="flex items-center gap-2 bg-[#008069]/20 dark:bg-[#2a3942]/50 rounded-lg px-2 py-1 flex-1 max-w-[200px]">
              <svg
                width="16"
                height="16"
                viewBox="0 0 24 24"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                className="text-[#ffffff] dark:text-[#8696a0] flex-shrink-0"
                aria-hidden="true"
              >
                <path
                  d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search..."
                className="bg-transparent border-none outline-none text-[#ffffff] dark:text-[#e9edef] text-sm placeholder:text-[#ffffff]/60 dark:placeholder:text-[#8696a0] flex-1 min-w-0"
                autoFocus
                onKeyDown={(e) => {
                  if (e.key === "Escape") {
                    setIsSearchOpen(false)
                    setSearchQuery("")
                  }
                }}
              />
              <button
                onClick={() => {
                  setIsSearchOpen(false)
                  setSearchQuery("")
                }}
                className="text-[#ffffff] dark:text-[#8696a0] hover:opacity-80 p-1"
                aria-label="Close search"
              >
                <svg
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  xmlns="http://www.w3.org/2000/svg"
                  aria-hidden="true"
                >
                  <path
                    d="M18 6L6 18M6 6l12 12"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              </button>
            </div>
          ) : (
            <button
              onClick={() => setIsSearchOpen(true)}
              className="p-2 text-[#ffffff] dark:text-[#8696a0] hover:bg-[#008069]/80 dark:hover:bg-[#313d45] rounded-full transition-colors"
              aria-label="Search messages"
              title="Search"
            >
              <svg
                width="20"
                height="20"
                viewBox="0 0 24 24"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                aria-hidden="true"
              >
                <path
                  d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </button>
          )}
        </div>
      </header>

      {isLoading && (
        <div
          className="p-4 text-sm text-[#667781] dark:text-[#8696a0] text-center"
          role="status"
          aria-live="polite"
        >
          Loading history...
        </div>
      )}

      <MessageList items={combined} viewerRole="user" searchQuery={searchQuery} />

      <Composer
        token={token}
        mode="user"
        onSendText={async (t) => {
          if (!t) return
          const tempId = `temp-${Date.now()}`
          addOptimisticMessage({
            kind: "text",
            from: "user",
            text: t,
            created_at: new Date().toISOString(),
            message_id: tempId,
            status: "sending",
          })
          await Promise.resolve(sendText(t))
        }}
        onFileSent={(file, fileUrl) => {
          const tempId = `temp-file-${Date.now()}`
          addOptimisticMessage({
            kind: "file",
            from: "user",
            file_url: fileUrl || URL.createObjectURL(file),
            file_name: file.name,
            file_type: file.type,
            created_at: new Date().toISOString(),
            message_id: tempId,
            status: "sending",
          })
        }}
      />
      <CallUI
        callState={callState}
        isInitiator={isInitiator}
        isRemoteAudioEnabled={isRemoteAudioEnabled}
        showIncomingCall={showIncomingCall}
        onEndCall={endCall}
        onAcceptCall={acceptCall}
        onToggleMute={toggleMute}
        onToggleRemoteAudio={toggleRemoteAudio}
        localAudioRef={localAudioRef}
        remoteAudioRef={remoteAudioRef}
        displayName="Master"
        micPermission={micPermission}
        speakerPermission={speakerPermission}
      />
    </main>
  )
}

