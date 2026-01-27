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
  micPermission?: "granted" | "denied" | "prompt" | null
  speakerPermission?: "granted" | "denied" | "prompt" | null
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
  micPermission,
  speakerPermission,
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

  useEffect(() => {
    const audio = remoteAudioRef.current
    if (callState === "connected" && audio) {
      if (audio.srcObject) {
        audio.muted = !isRemoteAudioEnabled
        audio.volume = isRemoteAudioEnabled ? 1.0 : 0
        
        const playPromise = audio.play()
        if (playPromise !== undefined) {
          playPromise
            .then(() => {
              console.log("[CallUI] Remote audio play() succeeded", {
                muted: audio.muted,
                volume: audio.volume,
                readyState: audio.readyState
              })
            })
            .catch(err => {
              console.error("[CallUI] Error playing remote audio:", err)
              setTimeout(() => {
                if (audio && audio.srcObject) {
                  audio.play().catch(e => {
                    console.error("[CallUI] Retry play() failed:", e)
                  })
                }
              }, 100)
            })
        }
        
        console.log("[CallUI] Remote audio configured", {
          muted: audio.muted,
          volume: audio.volume,
          hasStream: !!audio.srcObject,
          readyState: audio.readyState
        })
      } else {
        console.warn("[CallUI] Remote audio has no srcObject when connected")
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [callState, isRemoteAudioEnabled])

  useEffect(() => {
    const localAudio = localAudioRef.current
    const remoteAudio = remoteAudioRef.current
    
    if (localAudio) {
      localAudio.setAttribute("autoplay", "true")
      localAudio.setAttribute("muted", "true")
      localAudio.setAttribute("playsinline", "true")
    }
    
    if (remoteAudio) {
      remoteAudio.setAttribute("autoplay", "true")
      remoteAudio.setAttribute("playsinline", "true")
      remoteAudio.muted = false
      remoteAudio.volume = 1.0
    }
  }, [])

  return (
    <>
      <audio ref={localAudioRef} autoPlay muted playsInline />
      <audio ref={remoteAudioRef} autoPlay playsInline />
      
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

            <div className="flex flex-col items-center gap-4">
              {(micPermission === "denied" || speakerPermission === "denied") && (
                <div className="bg-yellow-500/20 border border-yellow-500/50 rounded-lg px-4 py-2 text-yellow-200 text-sm">
                  {micPermission === "denied" && "Microphone permission required"}
                  {micPermission === "denied" && speakerPermission === "denied" && " • "}
                  {speakerPermission === "denied" && "Speaker permission required"}
                </div>
              )}
              <div className="flex items-center justify-center gap-4">
                <div className="relative">
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={onToggleMute}
                    className={`w-14 h-14 rounded-full bg-[#2a3942] hover:bg-[#313d45] text-white ${micPermission === "denied" ? "opacity-50 cursor-not-allowed" : ""}`}
                    aria-label={isMuted ? "Unmute" : "Mute"}
                    disabled={micPermission === "denied"}
                  >
                    {isMuted ? (
                      <MicOff className="w-6 h-6" />
                    ) : (
                      <Mic className="w-6 h-6" />
                    )}
                  </Button>
                  {micPermission === "granted" && (
                    <div className="absolute -top-1 -right-1 w-3 h-3 bg-green-500 rounded-full border-2 border-[#111b21]"></div>
                  )}
                </div>
                <div className="relative">
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={onToggleRemoteAudio}
                    className={`w-14 h-14 rounded-full bg-[#2a3942] hover:bg-[#313d45] text-white ${speakerPermission === "denied" ? "opacity-50 cursor-not-allowed" : ""}`}
                    aria-label={isRemoteAudioEnabled ? "Mute remote" : "Unmute remote"}
                    disabled={speakerPermission === "denied"}
                  >
                    {isRemoteAudioEnabled ? (
                      <Volume2 className="w-6 h-6" />
                    ) : (
                      <VolumeX className="w-6 h-6" />
                    )}
                  </Button>
                  {speakerPermission === "granted" && (
                    <div className="absolute -top-1 -right-1 w-3 h-3 bg-green-500 rounded-full border-2 border-[#111b21]"></div>
                  )}
                </div>
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
        </div>
      )}
    </>
  )
}
