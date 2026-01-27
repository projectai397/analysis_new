"use client"

import { Phone, PhoneIncoming, PhoneCall } from "lucide-react"
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
  if (!show) {
    return null
  }

  const iconConfig = {
    calling: {
      bg: "bg-gradient-to-br from-[#008069] to-[#006b58]",
      icon: PhoneCall,
      border: "border-[#008069]/20"
    },
    incoming: {
      bg: "bg-gradient-to-br from-[#3b82f6] to-[#2563eb]",
      icon: PhoneIncoming,
      border: "border-[#3b82f6]/20"
    },
    connected: {
      bg: "bg-gradient-to-br from-[#22c55e] to-[#16a34a]",
      icon: Phone,
      border: "border-[#22c55e]/20"
    },
  }[iconType]

  const Icon = iconConfig.icon

  return (
    <div 
      className="fixed bottom-24 right-5 z-[9999] transition-all duration-300 ease-out animate-in slide-in-from-bottom-5" 
      style={{ pointerEvents: 'auto', position: 'fixed' }}
      data-testid="call-toaster"
    >
      <div className={`bg-gradient-to-br from-white to-gray-50 dark:from-[#111b21] dark:to-[#202c33] rounded-2xl shadow-2xl p-5 min-w-[300px] max-w-[340px] border-2 ${iconConfig.border} backdrop-blur-sm`}>
        <div className="flex items-center gap-4 mb-4">
          <div className={`w-14 h-14 rounded-full ${iconConfig.bg} flex items-center justify-center flex-shrink-0 shadow-lg ring-4 ring-white/20 dark:ring-[#111b21]/20`}>
            <Icon className="w-7 h-7 text-white" />
          </div>
          <div className="flex-1 min-w-0">
            <div className="text-base font-bold text-[#111b21] dark:text-[#e9edef] mb-1">
              {title}
            </div>
            <div className="text-sm text-[#667781] dark:text-[#8696a0]">
              {status}
            </div>
          </div>
        </div>
        <div className="flex gap-3 pt-4 border-t border-[#e4e6eb] dark:border-[#313d45]">
          {showAccept && onAccept ? (
            <Button
              onClick={onAccept}
              className="flex-1 bg-gradient-to-r from-[#008069] to-[#006b58] hover:from-[#006b58] hover:to-[#005a4a] text-white font-semibold shadow-lg transition-all"
              size="sm"
            >
              Accept
            </Button>
          ) : (
            <Button
              onClick={onEnd}
              className="flex-1 bg-gradient-to-r from-[#dc2626] to-[#b91c1c] hover:from-[#b91c1c] hover:to-[#991b1b] text-white font-semibold shadow-lg transition-all"
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
