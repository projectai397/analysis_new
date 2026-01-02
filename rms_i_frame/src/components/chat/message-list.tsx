"use client"

import { useEffect, useRef, useState } from "react"
import type { ConversationItem, Role } from "@/lib/types"
import { CheckCheck, Check, Clock, ChevronDown } from "lucide-react"

export function MessageList({ items, viewerRole, searchQuery }: { items: ConversationItem[]; viewerRole: Role; searchQuery?: string }) {
  const baseurl = process.env.NEXT_PUBLIC_API_BASE_URL;
  const endRef = useRef<HTMLDivElement | null>(null)
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [highlightedIds, setHighlightedIds] = useState<Set<string>>(new Set())
  const [currentHighlightIndex, setCurrentHighlightIndex] = useState<number>(0)
  const highlightedIdsArrayRef = useRef<string[]>([])
  const messageRefs = useRef<Map<string, HTMLDivElement>>(new Map())
  const [showScrollToBottom, setShowScrollToBottom] = useState(false)
  
  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth", block: "end" })
    setShowScrollToBottom(false)
  }, [items])

  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    const checkScrollPosition = () => {
      const { scrollTop, scrollHeight, clientHeight } = container
      const isNearBottom = scrollHeight - scrollTop - clientHeight < 100
      setShowScrollToBottom(!isNearBottom)
    }

    container.addEventListener("scroll", checkScrollPosition)
    checkScrollPosition()

    return () => {
      container.removeEventListener("scroll", checkScrollPosition)
    }
  }, [])

  const scrollToBottom = () => {
    endRef.current?.scrollIntoView({ behavior: "smooth", block: "end" })
  }

  const prevSearchQueryRef = useRef<string>("")

  useEffect(() => {
    const searchQueryTrimmed = searchQuery?.trim() || ""
    
    if (!searchQueryTrimmed) {
      if (highlightedIds.size > 0) {
        setHighlightedIds(new Set())
      }
      highlightedIdsArrayRef.current = []
      setCurrentHighlightIndex(-1)
      prevSearchQueryRef.current = ""
      return
    }

    if (prevSearchQueryRef.current === searchQueryTrimmed) {
      return
    }

    prevSearchQueryRef.current = searchQueryTrimmed

    const query = searchQueryTrimmed.toLowerCase()
    const matchingIds = new Set<string>()
    
    items.forEach((item, idx) => {
      const key = (item as any).message_id || `${item.kind}-${idx}-${item.created_at}`
      let matches = false
      
      if (item.kind === "text" && item.text) {
        matches = item.text.toLowerCase().includes(query)
      } else if (item.kind === "file" && item.file_name) {
        matches = item.file_name.toLowerCase().includes(query)
      } else if (item.kind === "audio" && item.audio_name) {
        matches = item.audio_name.toLowerCase().includes(query)
      }
      
      if (matches) {
        matchingIds.add(key)
      }
    })

    if (matchingIds.size > 0) {
      const matchingIdsArray = Array.from(matchingIds)
      highlightedIdsArrayRef.current = matchingIdsArray
      setHighlightedIds(matchingIds)
      setCurrentHighlightIndex(-1)
    } else {
      setHighlightedIds(new Set())
      highlightedIdsArrayRef.current = []
      setCurrentHighlightIndex(-1)
    }
  }, [searchQuery, items])

  const [isClient, setIsClient] = useState(false)
  
  useEffect(() => {
    setIsClient(true)
  }, [])

  useEffect(() => {
    if (!searchQuery || !searchQuery.trim() || highlightedIdsArrayRef.current.length === 0) {
      return
    }

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Enter" && !e.shiftKey && !e.ctrlKey && !e.metaKey) {
        const target = e.target as HTMLElement
        const isComposer = target.closest('textarea') && target.closest('textarea')?.getAttribute('aria-label') === 'Type a message'
        
        if (isComposer) {
          return
        }

        e.preventDefault()
        e.stopPropagation()
        
        let nextIndex: number
        if (currentHighlightIndex === -1) {
          nextIndex = highlightedIdsArrayRef.current.length - 1
        } else {
          nextIndex = currentHighlightIndex - 1
          if (nextIndex < 0) {
            nextIndex = highlightedIdsArrayRef.current.length - 1
          }
        }
        setCurrentHighlightIndex(nextIndex)
        
        const nextMatchKey = highlightedIdsArrayRef.current[nextIndex]
        setTimeout(() => {
          const nextMatchElement = messageRefs.current.get(nextMatchKey)
          if (nextMatchElement) {
            nextMatchElement.scrollIntoView({ behavior: "smooth", block: "center" })
          }
        }, 50)
      }
    }

    window.addEventListener("keydown", handleKeyDown, true)
    return () => {
      window.removeEventListener("keydown", handleKeyDown, true)
    }
  }, [searchQuery, currentHighlightIndex])

  const formatTime = (dateString: string) => {
    if (!isClient) {
      const date = new Date(dateString)
      return date.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", hour12: true })
    }
    const date = new Date(dateString)
    const now = new Date()
    const isToday = date.toDateString() === now.toDateString()
    
    if (isToday) {
      return date.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", hour12: true })
    }
    return date.toLocaleDateString("en-US", { month: "short", day: "numeric" }) + ", " + 
           date.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", hour12: true })
  }

  const normalizeText = (text: string): string => {
    if (!text) return text
    
    const trimmed = text.trim()
    if (!trimmed) return text
    
    const hasSpacedChars = /^(\S\s)+\S?$/.test(trimmed)
    
    if (hasSpacedChars) {
      const noSpaces = trimmed.replace(/\s+/g, '')
      let normalized = noSpaces.replace(/([a-z])([A-Z])/g, '$1 $2')
      normalized = normalized.replace(/([.!?,;:])([A-Za-z])/g, '$1 $2')
      return normalized
    }
    
    return text.replace(/\s+/g, ' ').trim()
  }

  const parseEmojis = (text: string): string => {
    if (!text) return text
    
    return text
      .replace(/\\ud([0-9a-fA-F]{3})\\ud([0-9a-fA-F]{3})/g, (match, high, low) => {
        const highSurrogate = parseInt('d' + high, 16)
        const lowSurrogate = parseInt('d' + low, 16)
        if (highSurrogate >= 0xD800 && highSurrogate <= 0xDBFF && lowSurrogate >= 0xDC00 && lowSurrogate <= 0xDFFF) {
          const codePoint = (highSurrogate - 0xD800) * 0x400 + (lowSurrogate - 0xDC00) + 0x10000
          return String.fromCodePoint(codePoint)
        }
        return match
      })
      .replace(/\\u([0-9a-fA-F]{4})/g, (match, code) => {
        const charCode = parseInt(code, 16)
        if (charCode >= 0xD800 && charCode <= 0xDFFF) {
          return match
        }
        return String.fromCharCode(charCode)
      })
  }

  const shouldShowDateSeparator = (current: ConversationItem, previous: ConversationItem | undefined) => {
    if (!previous) return true
    const currentDate = new Date(current.created_at).toDateString()
    const previousDate = new Date(previous.created_at).toDateString()
    return currentDate !== previousDate
  }

  const isImageFile = (fileType: string | undefined, fileName: string | undefined) => {
    if (!fileType && !fileName) return false
    const imageTypes = ['image/', 'image/jpeg', 'image/jpg', 'image/png', 'image/gif', 'image/webp', 'image/svg']
    const imageExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg']
    
    if (fileType) {
      return imageTypes.some(type => fileType.toLowerCase().includes(type))
    }
    if (fileName) {
      const ext = fileName.toLowerCase().substring(fileName.lastIndexOf('.'))
      return imageExtensions.includes(ext)
    }
    return false
  }

  return (
    <div className="flex-1 relative flex flex-col min-h-0">
      <div 
        ref={containerRef}
        className="flex-1 overflow-y-auto px-4 py-2 space-y-1 chat-box-wrapper min-h-0"
        role="log"
        aria-label="Chat messages"
        aria-live="polite"
        aria-atomic="false"
      >
      {items.map((m, idx) => {
        const key = (m as any).message_id || `${m.kind}-${idx}-${m.created_at}`
        const isMine = viewerRole === "superadmin" ? (m.from === "admin" || m.from === "bot") : m.from === "user"
        const isBot = m.from === "bot"
        const previousItem = idx > 0 ? items[idx - 1] : undefined
        const showDateSeparator = shouldShowDateSeparator(m, previousItem)
        
        const isHighlighted = highlightedIds.has(key)
        const escapeRegex = (str: string) => {
          return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        }
        const highlightText = (text: string) => {
          if (!searchQuery || !searchQuery.trim()) return text
          const query = searchQuery.trim()
          const escapedQuery = escapeRegex(query)
          const parts = text.split(new RegExp(`(${escapedQuery})`, "gi"))
          return parts.map((part, i) => 
            part.toLowerCase() === query.toLowerCase() ? (
              <mark key={i} className="bg-yellow-300 dark:bg-yellow-600/50 px-0.5 rounded">
                {part}
              </mark>
            ) : part
          )
        }

        const highlightHTML = (html: string, query: string) => {
          if (!query || !query.trim()) return html
          const escapedQuery = escapeRegex(query.trim())
          const parts = html.split(/(<[^>]*>)/)
          return parts.map((part) => {
            if (part.startsWith('<') && part.endsWith('>')) {
              return part
            }
            return part.replace(new RegExp(`(${escapedQuery})`, "gi"), (match) => 
              `<mark class="bg-yellow-300 dark:bg-yellow-600/50 px-0.5 rounded">${match}</mark>`
            )
          }).join('')
        }

        return (
          <div key={key} ref={(el) => {
            if (el) {
              messageRefs.current.set(key, el)
            } else {
              messageRefs.current.delete(key)
            }
          }}>
            {showDateSeparator && isClient && (
              <div className="flex justify-center my-4" role="separator" aria-label={`Messages from ${new Date(m.created_at).toLocaleDateString()}`}>
                <div className="bg-[#ffffff] dark:bg-[#202c33] px-3 py-1 rounded-full text-xs text-[#667781] dark:text-[#8696a0] shadow-sm">
                  {new Date(m.created_at).toLocaleDateString("en-US", { weekday: "long", year: "numeric", month: "long", day: "numeric" })}
                </div>
              </div>
            )}
            <div className={`flex ${isMine ? "justify-end" : "justify-start"} mb-1`}>
              <div
                className={`max-w-[75%] sm:max-w-[70%] md:max-w-[65%] lg:max-w-[60%] rounded-lg px-2 py-1.5 text-sm shadow-sm transition-colors duration-300 overflow-hidden ${
                  isMine
                    ? "bg-[#dcf8c6] dark:bg-[#005c4b] text-[#111b21] dark:text-[#e9edef] rounded-tr-none"
                    : "bg-[#ffffff] dark:bg-[#202c33] text-[#111b21] dark:text-[#e9edef] rounded-tl-none"
                }`}
                role="article"
                aria-label={isMine ? "Your message" : "Received message"}
              >
                {m.kind === "text" && (
                  <div className="space-y-1">
                    {isBot && viewerRole === "superadmin" && (
                      <p className="text-xs font-medium text-[#667781] dark:text-[#8696a0] mb-1" role="note">
                        Bot reply
                      </p>
                    )}
                    {(() => {
                      const emojiParsedText = parseEmojis(m.text)
                      const normalizedText = normalizeText(emojiParsedText)
                      const containsHTML = /<[^>]+>/.test(normalizedText)
                      
                      if (containsHTML) {
                        let htmlContent = normalizedText
                        if (searchQuery && searchQuery.trim()) {
                          htmlContent = highlightHTML(htmlContent, searchQuery)
                        }
                        return (
                          <div 
                            className="whitespace-pre-wrap text-pretty break-words leading-relaxed"
                            dangerouslySetInnerHTML={{ __html: htmlContent }}
                          />
                        )
                      } else {
                        return (
                          <p className="whitespace-pre-wrap text-pretty break-words leading-relaxed">
                            {searchQuery && searchQuery.trim() ? highlightText(normalizedText) : normalizedText}
                          </p>
                        )
                      }
                    })()}
                    {m.meta?.domain === "out_of_scope" && (
                      <p className="text-xs text-[#667781] dark:text-[#8696a0] mt-1" role="note">
                        Topic not supported.
                      </p>
                    )}
                  </div>
                )}
                {m.kind === "file" && (
                  <div className="space-y-2">
                    {isImageFile(m.file_type, m.file_name) && (
                      <div className="mb-1">
                        <a
                          href={baseurl + m.file_url}
                          target="_blank"
                          rel="noreferrer"
                          className="block focus:outline-none focus:ring-2 focus:ring-[#008069] focus:ring-offset-2 rounded overflow-hidden"
                          aria-label={`View image: ${m.file_name || "Image"}`}
                        >
                          <img
                            src={baseurl + m.file_url}
                            alt={m.file_name || "Image preview"}
                            className="w-12 h-12 object-cover rounded cursor-pointer hover:opacity-90 transition-opacity"
                            loading="lazy"
                          />
                        </a>
                      </div>
                    )}
                    <a
                      className="text-[#008069] dark:text-[#53bdeb] underline break-all hover:opacity-80 focus:outline-none focus:ring-2 focus:ring-[#008069] focus:ring-offset-2 rounded"
                      href={baseurl + m.file_url}
                      target="_blank"
                      rel="noreferrer"
                      aria-label={`Download file: ${m.file_name || "File"}`}
                    >
                      {m.file_name || "File"}
                    </a>
                    {m.file_type && (
                      <div className="text-xs text-[#667781] dark:text-[#8696a0]">{m.file_type}</div>
                    )}
                  </div>
                )}
                {m.kind === "audio" && (
                  <div className="space-y-2 min-w-[200px]">
                    <audio
                      controls
                      src={m.audio_url.startsWith("http") ? m.audio_url : baseurl + m.audio_url}
                      className="w-full max-w-[250px] h-8"
                      aria-label={`Audio message: ${m.audio_name || "Audio"}`}
                    >
                      Your browser does not support the audio element.
                    </audio>
                    {m.audio_name && m.audio_name !== "Audio" && (
                      <div className="text-xs text-[#667781] dark:text-[#8696a0]">{m.audio_name}</div>
                    )}
                  </div>
                )}
                <div className={`flex items-center gap-1 mt-1 ${isMine ? "justify-end" : "justify-start"}`}>
                  <span className="text-[10px] text-[#667781] dark:text-[#8696a0] leading-none" aria-label={`Sent at ${formatTime(m.created_at)}`} suppressHydrationWarning>
                    {formatTime(m.created_at)}
                  </span>
                  {isMine && (
                    <span className="ml-1" aria-label={m.status === "sending" ? "Sending" : m.status === "sent" ? "Sent" : "Delivered"}>
                      {m.status === "sending" ? (
                        <Clock className="w-3.5 h-3.5 text-[#667781] dark:text-[#8696a0] animate-pulse" aria-hidden="true" />
                      ) : m.status === "sent" ? (
                        <Check className="w-3.5 h-3.5 text-[#667781] dark:text-[#8696a0]" aria-hidden="true" />
                      ) : (
                        <CheckCheck className="w-3.5 h-3.5 text-[#667781] dark:text-[#8696a0]" aria-hidden="true" />
                      )}
                    </span>
                  )}
                </div>
              </div>
            </div>
          </div>
        )
      })}
      <div ref={endRef} aria-hidden="true" />
      </div>
      {showScrollToBottom && (
        <button
          onClick={scrollToBottom}
          className="absolute bottom-4 right-4 w-10 h-10 bg-[#008069] dark:bg-[#008069] rounded-full flex items-center justify-center shadow-lg hover:bg-[#006b58] dark:hover:bg-[#006b58] transition-all duration-200 hover:scale-110 focus:outline-none focus:ring-2 focus:ring-[#008069] focus:ring-offset-2 z-10"
          aria-label="Scroll to bottom"
          title="Scroll to bottom"
        >
          <ChevronDown className="w-5 h-5 text-white" aria-hidden="true" />
        </button>
      )}
    </div>
  )
}
