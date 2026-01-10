"use client"

import { Phone } from "lucide-react"
import { Button } from "@/components/ui/button"

type CallToasterProps = {
  show: boolean
  title: string
  status: string
  iconType: "calling" | "incoming" | "connected"
  showAccept?: boolean
  onAccept?: () => void
  onEnd?: () => void
}

export function CallToaster({
  show,
  title,
  status,
  iconType,
  showAccept = false,
  onAccept,
  onEnd,
}: CallToasterProps) {
  console.log("[CallToaster] Render", { show, title, status, iconType, showAccept })
  
  if (!show) {
    console.log("[CallToaster] Not showing (show=false)")
    return null
  }

  const iconBgClass = {
    calling: "bg-[#22c55e]",
    incoming: "bg-[#3b82f6]",
    connected: "bg-[#22c55e]",
  }[iconType]

  console.log("[CallToaster] Rendering toaster", { iconBgClass })

  return (
    <div 
      className="fixed bottom-24 right-5 z-[9999] transition-all duration-300 ease-out" 
      style={{ pointerEvents: 'auto', position: 'fixed' }}
      data-testid="call-toaster"
    >
      <div className="bg-white dark:bg-[#202c33] rounded-xl shadow-2xl p-4 min-w-[280px] max-w-[320px] border-2 border-[#e5e7eb] dark:border-[#313d45]">
        <div className="flex items-center gap-3 mb-3">
          <div className={`w-10 h-10 rounded-full ${iconBgClass} flex items-center justify-center flex-shrink-0`}>
            <Phone className="w-5 h-5 text-white" />
          </div>
          <div className="flex-1 min-w-0">
            <div className="text-sm font-semibold text-[#1f2937] dark:text-[#e9edef] mb-1">
              {title}
            </div>
            <div className="text-xs text-[#6b7280] dark:text-[#8696a0]">
              {status}
            </div>
          </div>
        </div>
        <div className="flex gap-2 pt-3 border-t border-[#e5e7eb] dark:border-[#313d45]">
          {showAccept && onAccept ? (
            <Button
              onClick={onAccept}
              className="flex-1 bg-[#22c55e] hover:bg-[#16a34a] text-white"
              size="sm"
            >
              Accept
            </Button>
          ) : (
            <Button
              onClick={onEnd}
              className="flex-1 bg-[#dc2626] hover:bg-[#b91c1c] text-white"
              size="sm"
            >
              End Call
            </Button>
          )}
        </div>
      </div>
    </div>
  )
}
