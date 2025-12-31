
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

export default function UserPage() {

  return (
    <Suspense fallback={null}>
      <UserPageInner />
    </Suspense>
  )
}
function UserPageInner() {
  const search = useSearchParams()
  const token =
    search.get("token") ||
    search.get("t") ||
    search.get("jwt") ||
    search.get("auth") ||
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
  const { status, chatId, messages, sendText, resetLiveMessages } = useChatSocket({ token, role: "user" as any })
  const [isSearchOpen, setIsSearchOpen] = useState(false)
  const [searchQuery, setSearchQuery] = useState("")
  const [optimisticMessages, setOptimisticMessages] = useState<ConversationItem[]>([])

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
            <div className="w-10 h-10 rounded-full bg-[#ffffff] dark:bg-[#2a3942] flex items-center justify-center">
              <span className="text-lg font-semibold text-[#008069] dark:text-[#53bdeb]" aria-hidden="true">
                C
              </span>
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
                className={`text-xs ${
                  status === "open" 
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
    </main>
  )
}

