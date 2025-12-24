"use client"

import type React from "react"
import { useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"

export function TokenGate(props: { children: (token: string) => React.ReactNode; title?: string }) {
  const [token, setToken] = useState("")
  const [saved, setSaved] = useState<string | null>(null)

  useEffect(() => {
    const t = localStorage.getItem("jwt_token")
    if (t) setSaved(t)
  }, [])

  if (!saved) {
    return (
      <div className="max-w-md mx-auto p-6 flex flex-col gap-4">
        <h1 className="text-2xl font-semibold text-balance">{props.title || "Enter your JWT"}</h1>
        <Input placeholder="Paste JWT…" value={token} onChange={(e) => setToken(e.target.value)} />
        <Button
          onClick={() => {
            const t = token.trim()
            if (t) {
              // localStorage.setItem("jwt_token", t)
              setSaved(t)
            }
          }}
        >
          Continue
        </Button>
        <p className="text-sm text-muted-foreground">Stored locally for demo only.</p>
      </div>
    )
  }

  return <>{props.children(saved)}</>
}
