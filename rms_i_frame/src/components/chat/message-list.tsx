"use client"

import { useEffect, useRef } from "react"
import type { ConversationItem, Role } from "@/lib/types"

export function MessageList({ items, viewerRole }: { items: ConversationItem[]; viewerRole: Role }) {
   const baseurl = process.env.NEXT_PUBLIC_API_BASE_URL;
  const endRef = useRef<HTMLDivElement | null>(null)
  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth", block: "end" })
  }, [items])

  return (
    <div className="flex-1 overflow-y-auto px-3 py-4 space-y-4">
      {items.map((m, idx) => {
        const key = (m as any).message_id || `${m.kind}-${idx}-${m.created_at}`
        const isMine = viewerRole === "superadmin" ? m.from === "admin" : m.from === "user"
        const align = m.from === "user" ? "justify-end" : "justify-start"
        return (
          <div key={key} className={`flex ${align}`}>
            <div className="max-w-lg rounded-md border px-3 py-2 text-sm bg-background ">
              {m.kind === "text" && (
                <div className="space-y-1">
                  <p className="whitespace-pre-wrap text-pretty">{m.text}</p>
                  {m.meta?.domain === "out_of_scope" && (
                    <p className="text-xs text-muted-foreground">Topic not supported.</p>
                  )}
                </div>
              )}
              {m.kind === "file" && (
                <div className="space-y-1">
                  <a className="text-primary underline break-all" href={baseurl + m.file_url} target="_blank" rel="noreferrer">
                    {m.file_name || "File"}
                  </a>
                  <div className="text-xs text-muted-foreground">{m.file_type}</div>
                </div>
              )}
              {m.kind === "audio" && (
                <div className="space-y-1">
                  <audio controls src={m.audio_url} className="w-64">
                    Your browser does not support the audio element.
                  </audio>
                  <div className="text-xs text-muted-foreground">{m.audio_name || "Audio"}</div>
                </div>
              )}
              <div className="text-[11px] text-muted-foreground mt-1">{new Date(m.created_at).toLocaleString()}</div>
            </div>
          </div>
        )
      })}
      <div ref={endRef} />
    </div>
  )
}
