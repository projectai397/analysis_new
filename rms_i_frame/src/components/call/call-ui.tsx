"use client"

import { Phone, PhoneOff, Volume2, VolumeX, Mic, MicOff } from "lucide-react"
import { Button } from "@/components/ui/button"
import type { CallState } from "@/hooks/use-webrtc"
import { CallToaster } from "./call-toaster"
import { useEffect } from "react"

type CallUIProps = {
  callState: CallState
  isInitiator: boolean
  isRemoteAudioEnabled: boolean
  showIncomingCall: boolean
  onEndCall: () => void
  onAcceptCall?: () => void
  onToggleMute: () => void
  onToggleRemoteAudio: () => void
  localAudioRef: React.RefObject<HTMLAudioElement | null>
  remoteAudioRef: React.RefObject<HTMLAudioElement | null>
  displayName?: string
}

export function CallUI({
  callState,
  isInitiator,
  isRemoteAudioEnabled,
  showIncomingCall,
  onEndCall,
  onAcceptCall,
  onToggleMute,
  onToggleRemoteAudio,
  localAudioRef,
  remoteAudioRef,
  displayName = "Master",
}: CallUIProps) {
  const isMuted = localAudioRef.current?.srcObject
    ? !(localAudioRef.current.srcObject as MediaStream).getAudioTracks()[0]?.enabled
    : false

  const showToaster = callState === "ringing" || callState === "connecting" || callState === "connected"
  const toasterTitle = isInitiator 
    ? (callState === "connected" ? "Connected" : "Calling...")
    : (callState === "connected" ? "Connected" : "Incoming call")
  const toasterStatus = isInitiator
    ? `Connecting to ${displayName}`
    : `${displayName} is calling`
  const toasterIconType = isInitiator 
    ? (callState === "connected" ? "connected" : "calling")
    : (callState === "connected" ? "connected" : "incoming")

  useEffect(() => {
    if (showToaster) {
      console.log("[CallUI] Toaster should be visible", { 
        callState, 
        showToaster, 
        toasterTitle, 
        toasterStatus,
        showIncomingCall 
      })
    }
  }, [showToaster, callState, toasterTitle, toasterStatus, showIncomingCall])

  console.log("[CallUI] Render state:", { 
    callState, 
    isInitiator, 
    showIncomingCall, 
    showToaster, 
    toasterTitle, 
    toasterStatus 
  })

  return (
    <>
      <audio ref={localAudioRef} autoPlay muted />
      <audio ref={remoteAudioRef} autoPlay />
      
      <CallToaster
        show={showToaster}
        title={toasterTitle}
        status={toasterStatus}
        iconType={toasterIconType}
        showAccept={showIncomingCall && !isInitiator}
        onAccept={onAcceptCall}
        onEnd={onEndCall}
      />
      {callState === "connected" && (
        <div className="fixed inset-0 bg-black/80 z-50 flex items-center justify-center">
          <div className="bg-[#111b21] dark:bg-[#202c33] rounded-2xl p-8 max-w-md w-full mx-4">
            <div className="text-center mb-8">
              <div className="w-32 h-32 mx-auto mb-6 rounded-full bg-gradient-to-br from-[#008069] to-[#006b58] flex items-center justify-center">
                <Phone className="w-12 h-12 text-white" />
              </div>
              <h2 className="text-2xl font-semibold text-white mb-2">{displayName}</h2>
              <p className="text-[#8696a0]">Call in progress</p>
            </div>

            <div className="flex items-center justify-center gap-4">
              <Button
                variant="ghost"
                size="icon"
                onClick={onToggleMute}
                className="w-14 h-14 rounded-full bg-[#2a3942] hover:bg-[#313d45] text-white"
                aria-label={isMuted ? "Unmute" : "Mute"}
              >
                {isMuted ? (
                  <MicOff className="w-6 h-6" />
                ) : (
                  <Mic className="w-6 h-6" />
                )}
              </Button>
              <Button
                variant="ghost"
                size="icon"
                onClick={onToggleRemoteAudio}
                className="w-14 h-14 rounded-full bg-[#2a3942] hover:bg-[#313d45] text-white"
                aria-label={isRemoteAudioEnabled ? "Mute remote" : "Unmute remote"}
              >
                {isRemoteAudioEnabled ? (
                  <Volume2 className="w-6 h-6" />
                ) : (
                  <VolumeX className="w-6 h-6" />
                )}
              </Button>
              <Button
                variant="ghost"
                size="icon"
                onClick={onEndCall}
                className="w-14 h-14 rounded-full bg-red-600 hover:bg-red-700 text-white"
                aria-label="End call"
              >
                <PhoneOff className="w-6 h-6" />
              </Button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
