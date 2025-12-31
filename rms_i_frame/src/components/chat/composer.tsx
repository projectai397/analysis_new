"use client"

import type React from "react"
import { useCallback, useEffect, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { uploadAudio, uploadFile } from "@/lib/api"
import { Paperclip, Mic, Send, Square, X } from "lucide-react"

type Props = {
  token: string
  mode: "user" | "admin"
  onSendText: (text: string) => Promise<void> | void
  onFileSent?: (file: File, fileUrl?: string) => void
  chatId?: string
}

export function Composer({ token, mode, onSendText, onFileSent, chatId }: Props) {
  const [text, setText] = useState("")
  const [sending, setSending] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [filePreview, setFilePreview] = useState<string | null>(null)
  const fileRef = useRef<HTMLInputElement | null>(null)
  const textareaRef = useRef<HTMLTextAreaElement | null>(null)

  const [recSupported, setRecSupported] = useState(false)
  const [recording, setRecording] = useState(false)
  const mediaRecRef = useRef<MediaRecorder | null>(null)
  const chunksRef = useRef<BlobPart[]>([])

  useEffect(() => setRecSupported(typeof window !== "undefined" && "MediaRecorder" in window), [])

  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = "auto"
      textareaRef.current.style.height = `${Math.min(textareaRef.current.scrollHeight, 120)}px`
    }
  }, [text])

  const canUpload = mode === "user" || (mode === "admin" && chatId)

  const send = useCallback(async () => {
    const t = text.trim()
    if (!t) return
    setSending(true)
    try {
      await onSendText(t)
      setText("")
      if (textareaRef.current) {
        textareaRef.current.style.height = "auto"
      }
    } finally {
      setSending(false)
    }
  }, [text, onSendText])

  const onKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault()
        void send()
      }
    },
    [send],
  )

  const onPickFile = () => {
    fileRef.current?.click()
  }

  const isImageFile = (file: File) => {
    return file.type.startsWith('image/')
  }

  const onFileChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      if (!e.target.files || !e.target.files.length) return
      if (!canUpload) return
      
      const file = e.target.files[0]
      setSelectedFile(file)
      
      if (isImageFile(file)) {
        const reader = new FileReader()
        reader.onloadend = () => {
          setFilePreview(reader.result as string)
        }
        reader.readAsDataURL(file)
      } else {
        setFilePreview(null)
      }
      
      e.target.value = ""
    },
    [canUpload],
  )

  const sendFile = useCallback(async () => {
    if (!selectedFile) return
    setUploading(true)
    try {
      if (onFileSent && filePreview) {
        onFileSent(selectedFile, filePreview)
      }
      const result = await uploadFile(token, selectedFile, chatId)
      setSelectedFile(null)
      setFilePreview(null)
    } finally {
      setUploading(false)
      if (fileRef.current) {
        fileRef.current.value = ""
      }
    }
  }, [selectedFile, token, chatId, onFileSent, filePreview])

  const cancelFilePreview = useCallback(() => {
    setSelectedFile(null)
    setFilePreview(null)
    if (fileRef.current) {
      fileRef.current.value = ""
    }
  }, [])

  const startRecording = useCallback(async () => {
    if (!recSupported || recording) return
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ 
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
          sampleRate: 48000,
          channelCount: 1
        } 
      })
      
      const getBestMimeType = () => {
        const types = [
          'audio/webm;codecs=opus',
          'audio/webm',
          'audio/ogg;codecs=opus',
          'audio/mp4',
          'audio/wav'
        ]
        for (const type of types) {
          if (MediaRecorder.isTypeSupported(type)) {
            return type
          }
        }
        return 'audio/webm'
      }

      const mimeType = getBestMimeType()
      const options: MediaRecorderOptions = {
        mimeType,
        audioBitsPerSecond: 128000
      }

      if (MediaRecorder.isTypeSupported('audio/webm;codecs=opus')) {
        options.audioBitsPerSecond = 128000
      }

      const mr = new MediaRecorder(stream, options)
      mediaRecRef.current = mr
      chunksRef.current = []
      mr.ondataavailable = (ev) => {
        if (ev.data.size > 0) {
          chunksRef.current.push(ev.data)
        }
      }
      mr.onstop = async () => {
        const blob = new Blob(chunksRef.current, { type: mimeType })
        setUploading(true)
        try {
          await uploadAudio(token, blob, chatId)
        } finally {
          setUploading(false)
        }
        stream.getTracks().forEach((t) => t.stop())
      }
      mr.start(100)
      setRecording(true)
    } catch (error) {
      console.error("Error starting recording:", error)
    }
  }, [recSupported, recording, token, chatId])

  const stopRecording = useCallback(() => {
    if (mediaRecRef.current) {
      mediaRecRef.current.stop()
      setRecording(false)
    }
  }, [])

  const canSend = text.trim().length > 0 && !sending && !selectedFile

  return (
    <>
      {selectedFile && (
        <div className="bg-[#f0f2f5] dark:bg-[#202c33] border-t border-[#e4e6eb] dark:border-[#313d45] px-4 py-3">
          <div className="relative inline-block max-w-[200px]">
            {filePreview ? (
              <div className="relative">
                <img
                  src={filePreview}
                  alt={selectedFile.name}
                  className="max-w-full max-h-[200px] rounded-lg object-contain"
                />
                <button
                  onClick={cancelFilePreview}
                  className="absolute top-2 right-2 w-6 h-6 bg-black/50 hover:bg-black/70 rounded-full flex items-center justify-center transition-colors"
                  aria-label="Remove file"
                >
                  <X className="w-4 h-4 text-white" aria-hidden="true" />
                </button>
              </div>
            ) : (
              <div className="bg-[#ffffff] dark:bg-[#2a3942] rounded-lg p-4 border border-[#e4e6eb] dark:border-[#313d45]">
                <div className="flex items-center justify-between">
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-[#111b21] dark:text-[#e9edef] truncate">
                      {selectedFile.name}
                    </p>
                    <p className="text-xs text-[#667781] dark:text-[#8696a0]">
                      {(selectedFile.size / 1024).toFixed(1)} KB
                    </p>
                  </div>
                  <button
                    onClick={cancelFilePreview}
                    className="ml-2 text-[#667781] dark:text-[#8696a0] hover:text-[#111b21] dark:hover:text-[#e9edef]"
                    aria-label="Remove file"
                  >
                    <X className="w-5 h-5" aria-hidden="true" />
                  </button>
                </div>
              </div>
            )}
          </div>
          <div className="mt-2 flex items-center gap-2">
            <button
              onClick={cancelFilePreview}
              className="px-4 py-2 text-sm text-[#667781] dark:text-[#8696a0] hover:bg-[#e4e6eb] dark:hover:bg-[#313d45] rounded-lg transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={() => void sendFile()}
              disabled={uploading}
              className="px-4 py-2 text-sm bg-[#008069] hover:bg-[#006b58] text-white rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
            >
              {uploading ? (
                <>
                  <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  Sending...
                </>
              ) : (
                <>
                  <Send className="w-4 h-4" aria-hidden="true" />
                  Send
                </>
              )}
            </button>
          </div>
        </div>
      )}
      <div 
        className="bg-[#f0f2f5] dark:bg-[#202c33] border-t border-[#e4e6eb] dark:border-[#313d45] px-4 py-2 flex items-center gap-2 "
        role="region"
        aria-label="Message composer"
      >
      <div className="flex items-center gap-1">
        <input
          ref={fileRef}
          type="file"
          className="hidden"
          onChange={onFileChange}
          accept="*/*"
          aria-label="Upload file"
        />
        <Button
          variant="ghost"
          size="icon"
          onClick={onPickFile}
          disabled={!canUpload || uploading || !!selectedFile}
          className="text-[#54656f] dark:text-[#8696a0] hover:bg-[#e4e6eb] dark:hover:bg-[#313d45] rounded-full !size-7"
          aria-label="Attach file"
          title="Attach file"
        >
          <Paperclip className="w-4 h-4" aria-hidden="true" />
        </Button>
        {recSupported && (
          <Button
            variant="ghost"
            size="icon"
            onClick={recording ? stopRecording : startRecording}
            disabled={!canUpload || uploading || canSend || !!selectedFile}
            className={`!size-7 text-[#54656f] dark:text-[#8696a0] hover:bg-[#e4e6eb] dark:hover:bg-[#313d45] rounded-full ${
              recording ? "bg-red-100 dark:bg-red-900/30 text-red-600 dark:text-red-400" : ""
            }`}
            aria-label={recording ? "Stop recording" : "Record voice message"}
            title={recording ? "Stop recording" : "Record voice message"}
          >
            {recording ? (
              <Square className="w-5 h-5" aria-hidden="true" />
            ) : (
              <Mic className="w-5 h-5" aria-hidden="true" />
            )}
          </Button>
        )}
      </div>

      <div className="flex-1 relative">
        <textarea
          ref={textareaRef}
          placeholder="Type a message"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={onKeyDown}
          disabled={sending || uploading || !!selectedFile}
          rows={1}
          className={`w-full bg-[#ffffff] dark:bg-[#2a3942] text-[#111b21] dark:text-[#e9edef] rounded-lg px-4 py-2.5 resize-none border-none outline-none focus:ring-2 focus:ring-[#008069] focus:ring-offset-2 text-sm leading-5 overflow-y-auto placeholder:text-[#667781] dark:placeholder:text-[#8696a0] ${
            canSend ? "pr-12" : "pr-4"
          }`}
          aria-label="Type a message"
          aria-multiline="true"
        />
        {canSend && (
          <button
            onClick={() => void send()}
            disabled={sending}
            className="absolute right-2 top-1/2 -translate-y-1/2 w-8 h-8 bg-[#008069] dark:bg-[#008069] rounded-full flex items-center justify-center hover:bg-[#006b58] dark:hover:bg-[#006b58] transition-colors disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-[#008069] focus:ring-offset-2 shadow-sm z-10"
            aria-label="Send message"
            title="Send message"
          >
            <Send className="w-4 h-4 text-white" aria-hidden="true" />
          </button>
        )}
      </div>

      {uploading && (
        <div className="sr-only" role="status" aria-live="polite">
          Uploading file
        </div>
      )}
      {recording && (
        <div className="sr-only" role="status" aria-live="polite">
          Recording audio message
        </div>
      )}
      </div>
    </>
  )
}
