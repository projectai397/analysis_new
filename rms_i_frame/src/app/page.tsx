
"use client"

import useSWR from "swr"
import { Suspense } from "react"
// import { TokenGate } from "@/components/token-gate"
import { useChatSocket } from "@/lib/ws"
import { getChatroom } from "@/lib/api"
import { useEffect, useMemo } from "react"
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
      <main className="h-[100dvh] mx-auto flex flex-col">
        <header className="border-b p-3">
          <div className="text-sm font-medium">Status: blocked (no token)</div>
        </header>
        <div className="p-4 text-sm text-muted-foreground">
          Something went wrong !!
        </div>
      </main>
    )
  }

  return <UserView token={token} />
}


function UserView({ token }: { token: string }) {
  const { status, chatId, messages, sendText, resetLiveMessages } = useChatSocket({ token, role: "user" as any })

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
  }))
}, [messages])

  const combined = useMemo(() => [...historyItems, ...normalizedLive], [historyItems, normalizedLive])

  return (
    <main className="h-[100dvh]  mx-auto flex flex-col">
      <header className="border-b p-3">
        <div className="text-sm">
          Status: <span className="font-medium">{status}</span>
         <span className="hidden">  {chatId ? `(room: ${chatId})` : ""}</span>
        </div>
        {/* <div className="text-xs text-muted-foreground">Bot replies when no admin is present. Files/audio via REST.</div> */}
      </header>

      {isLoading && <div className="p-4 text-sm text-muted-foreground">Loading history...</div>}

      <MessageList items={combined} viewerRole="user" />

      <Composer
        token={token}
        mode="user"
        onSendText={async (t) => {
          if (!t) return
          // spec: user sends over WS; server echoes
          // eslint-disable-next-line @typescript-eslint/no-empty-function
          await Promise.resolve(sendText(t))
        }}
      />
    </main>
  )
}

