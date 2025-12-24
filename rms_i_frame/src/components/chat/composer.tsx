"use client"

import type React from "react"
import { useCallback, useEffect, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { uploadAudio, uploadFile } from "@/lib/api"

type Props = {
  token: string
  mode: "user" | "admin"
  onSendText: (text: string) => Promise<void> | void
  chatId?: string // required for admin uploads
}

export function Composer({ token, mode, onSendText, chatId }: Props) {
  const [text, setText] = useState("")
  const [sending, setSending] = useState(false)
  const [uploading, setUploading] = useState(false)
  const fileRef = useRef<HTMLInputElement | null>(null)

  const [recSupported, setRecSupported] = useState(false)
  const [recording, setRecording] = useState(false)
  const mediaRecRef = useRef<MediaRecorder | null>(null)
  const chunksRef = useRef<BlobPart[]>([])

  useEffect(() => setRecSupported(typeof window !== "undefined" && "MediaRecorder" in window), [])

  const canUpload = mode === "user" || (mode === "admin" && chatId)

  const send = useCallback(async () => {
    const t = text.trim()
    if (!t) return
    setSending(true)
    try {
      await onSendText(t)
      setText("")
    } finally {
      setSending(false)
    }
  }, [text, onSendText])

  const onKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault()
        void send()
      }
    },
    [send],
  )

  const onPickFile = () => fileRef.current?.click()

  const onFileChange = useCallback(
    async (e: React.ChangeEvent<HTMLInputElement>) => {
      if (!e.target.files || !e.target.files.length) return
      if (!canUpload) return
      setUploading(true)
      try {
        await uploadFile(token, e.target.files[0], chatId)
      } finally {
        setUploading(false)
        e.target.value = ""
      }
    },
    [token, chatId, canUpload],
  )

  const startRecording = useCallback(async () => {
    if (!recSupported || recording) return
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
    const mr = new MediaRecorder(stream, { mimeType: "audio/webm" })
    mediaRecRef.current = mr
    chunksRef.current = []
    mr.ondataavailable = (ev) => chunksRef.current.push(ev.data)
    mr.onstop = async () => {
      const blob = new Blob(chunksRef.current, { type: "audio/webm" })
      setUploading(true)
      try {
        await uploadAudio(token, blob, chatId)
      } finally {
        setUploading(false)
      }
    }
    mr.start()
    setRecording(true)
  }, [recSupported, recording, token, chatId])

  const stopRecording = useCallback(() => {
    mediaRecRef.current?.stop()
    mediaRecRef.current?.stream.getTracks().forEach((t) => t.stop())
    setRecording(false)
  }, [])

  return (
    <div className="border-t p-3 flex items-center gap-2">
      <Input
        placeholder="Type a message…"
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={onKeyDown}
        disabled={sending}
      />
      <Button onClick={() => void send()} disabled={sending}>
        Send
      </Button>

      <input ref={fileRef} type="file" className="hidden" onChange={onFileChange} />
      <Button variant="secondary" onClick={onPickFile} disabled={!canUpload || uploading}>
        {uploading ? "Uploading…" : "Upload"}
      </Button>

      {recSupported && (
        <Button
          variant="secondary"
          
          onClick={recording ? stopRecording : startRecording}
          disabled={!canUpload || uploading}
        >
          {recording ? "Stop" : "Record"}
        </Button>
      )}
    </div>
  )
}
