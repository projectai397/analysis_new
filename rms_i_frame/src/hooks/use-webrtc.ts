"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import type { ClientEvent } from "@/lib/types"

export type CallState = "idle" | "ringing" | "connecting" | "connected" | "ended"

type UseWebRTCOptions = {
  chatId: string | null
  role: "user" | "superadmin" | "master"
  send: (payload: ClientEvent) => void
}

export function useWebRTC({ chatId, role, send }: UseWebRTCOptions) {
  const [callState, setCallState] = useState<CallState>("idle")
  const [isInitiator, setIsInitiator] = useState(false)
  const [isRemoteAudioEnabled, setIsRemoteAudioEnabled] = useState(true)
  const [callId, setCallId] = useState<string | null>(null)
  const [showIncomingCall, setShowIncomingCall] = useState(false)

  useEffect(() => {
    callIdRef.current = callId
  }, [callId])

  useEffect(() => {
    callStateRef.current = callState
  }, [callState])
  
  const peerConnectionRef = useRef<RTCPeerConnection | null>(null)
  const localStreamRef = useRef<MediaStream | null>(null)
  const remoteStreamRef = useRef<MediaStream | null>(null)
  const localAudioRef = useRef<HTMLAudioElement | null>(null)
  const remoteAudioRef = useRef<HTMLAudioElement | null>(null)
  const pendingIceRef = useRef<RTCIceCandidateInit[]>([])
  const callIdRef = useRef<string | null>(null)
  const callStateRef = useRef<CallState>("idle")
  const isEndingCallRef = useRef<boolean>(false)
  const endCallRef = useRef<(() => void) | null>(null)

  const createPeerConnection = useCallback(() => {
    const configuration: RTCConfiguration = {
      iceServers: [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" },
      ],
    }

    const pc = new RTCPeerConnection(configuration)
    
    pc.onicecandidate = (event) => {
      if (event.candidate && callIdRef.current) {
        send({
          type: "call.ice",
          call_id: callIdRef.current,
          candidate: event.candidate.toJSON(),
        })
      }
    }

    pc.ontrack = (event) => {
      if (event.streams[0]) {
        remoteStreamRef.current = event.streams[0]
        if (remoteAudioRef.current) {
          remoteAudioRef.current.srcObject = event.streams[0]
        }
      }
    }

    pc.onconnectionstatechange = () => {
      console.log("[WebRTC] Peer connection state changed:", pc.connectionState, { callId: callIdRef.current, callState: callStateRef.current })
      if (pc.connectionState === "connected") {
        setCallState("connected")
      } else if (pc.connectionState === "disconnected" || pc.connectionState === "failed") {
        console.warn("[WebRTC] Connection failed/disconnected, ending call", { callId: callIdRef.current, callState: callStateRef.current })
        if (callIdRef.current && (callStateRef.current === "connecting" || callStateRef.current === "connected")) {
          if (endCallRef.current) {
            endCallRef.current()
          }
        } else {
          console.log("[WebRTC] Ignoring connection state change - call not in active state")
        }
      }
    }

    return pc
  }, [send])

  const startCall = useCallback(async () => {
    console.log("[WebRTC] startCall called", { chatId, callState, role })
    if (!chatId) {
      console.warn("[WebRTC] Cannot start call: no chatId")
      alert("Cannot start call: No chatroom selected")
      return
    }
    if (callState !== "idle") {
      console.warn("[WebRTC] Cannot start call: callState is not idle", { callState })
      return
    }

    console.log("[WebRTC] Starting call, sending call.start")
    setIsInitiator(true)
    setCallState("ringing")
    try {
      send({ type: "call.start" })
      console.log("[WebRTC] call.start sent, callState set to ringing")
    } catch (error) {
      console.error("[WebRTC] Error sending call.start:", error)
      setCallState("idle")
      setIsInitiator(false)
    }
  }, [chatId, callState, send, role])

  const handleCallIncoming = useCallback((incomingCallId: string) => {
    console.log("[WebRTC] handleCallIncoming called", { incomingCallId, role, currentCallId: callId, currentCallState: callState })
    if (callState !== "idle" && callId !== incomingCallId) {
      console.warn("[WebRTC] Received call.incoming but call is already active", { currentCallId: callId, incomingCallId, callState })
      return
    }
    setCallId(incomingCallId)
    setShowIncomingCall(true)
    setCallState("ringing")
    setIsInitiator(false)
    console.log("[WebRTC] Incoming call state updated", { callId: incomingCallId, showIncomingCall: true, callState: "ringing" })
  }, [role, callId, callState])

  const acceptCall = useCallback(async () => {
    console.log("[WebRTC] acceptCall called", { callId, role })
    if (!callId) {
      console.warn("[WebRTC] Cannot accept call: no callId")
      return
    }

    console.log("[WebRTC] Accepting call, sending call.accept")
    setShowIncomingCall(false)
    setCallState("connecting")
    
    send({
      type: "call.accept",
      call_id: callId,
    })
    console.log("[WebRTC] call.accept sent")
  }, [callId, send, role])

  const handleCallAccepted = useCallback(async () => {
    if (!callId) return

    try {
      const stream = await navigator.mediaDevices.getUserMedia({ 
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
        } 
      })
      
      localStreamRef.current = stream
      if (localAudioRef.current) {
        localAudioRef.current.srcObject = stream
      }

      const pc = createPeerConnection()
      peerConnectionRef.current = pc

      stream.getTracks().forEach((track) => {
        pc.addTrack(track, stream)
      })

      const offer = await pc.createOffer()
      await pc.setLocalDescription(offer)

      send({
        type: "call.offer",
        call_id: callId,
        sdp: offer,
      })

      setCallState("connecting")
    } catch (error) {
      console.error("Error creating peer after call accepted:", error)
      if (endCallRef.current) {
        endCallRef.current()
      }
    }
  }, [callId, createPeerConnection, send])

  const handleCallOffer = useCallback(async (offer: RTCSessionDescriptionInit) => {
    if (!callId) return

    try {
      const stream = await navigator.mediaDevices.getUserMedia({ 
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
        } 
      })
      
      localStreamRef.current = stream
      if (localAudioRef.current) {
        localAudioRef.current.srcObject = stream
      }

      const pc = createPeerConnection()
      peerConnectionRef.current = pc

      stream.getTracks().forEach((track) => {
        pc.addTrack(track, stream)
      })

      await pc.setRemoteDescription(new RTCSessionDescription(offer))

      while (pendingIceRef.current.length > 0) {
        const candidate = pendingIceRef.current.shift()
        if (candidate) {
          try {
            await pc.addIceCandidate(new RTCIceCandidate(candidate))
          } catch (err) {
            console.warn("addIceCandidate (flush) failed:", err)
          }
        }
      }

      const answer = await pc.createAnswer()
      await pc.setLocalDescription(answer)

      send({
        type: "call.answer",
        call_id: callId,
        sdp: answer,
      })

      setCallState("connecting")
    } catch (error) {
      console.error("Error handling call offer:", error)
      if (endCallRef.current) {
        endCallRef.current()
      }
    }
  }, [callId, createPeerConnection, send])

  const handleCallAnswer = useCallback(async (answer: RTCSessionDescriptionInit) => {
    const pc = peerConnectionRef.current
    if (!pc || !answer) return

    try {
      await pc.setRemoteDescription(new RTCSessionDescription(answer))
      setCallState("connecting")
    } catch (error) {
      console.error("Error handling call answer:", error)
      if (endCallRef.current) {
        endCallRef.current()
      }
    }
  }, [])

  const handleIceCandidate = useCallback(async (candidate: RTCIceCandidateInit) => {
    const pc = peerConnectionRef.current
    if (!candidate) return

    if (!pc || !pc.remoteDescription) {
      pendingIceRef.current.push(candidate)
      return
    }

    try {
      await pc.addIceCandidate(new RTCIceCandidate(candidate))
    } catch (error) {
      console.error("Error adding ICE candidate:", error)
    }
  }, [])

  const handleCallRinging = useCallback((incomingCallId: string) => {
    console.log("[WebRTC] handleCallRinging called", { incomingCallId, role })
    setCallId((prevCallId) => {
      if (prevCallId && prevCallId !== incomingCallId) {
        console.warn("[WebRTC] Call ID mismatch in handleCallRinging", { prevCallId, incomingCallId })
      }
      return incomingCallId
    })
    setCallState((prevState) => {
      if (prevState === "idle") {
        console.log("[WebRTC] Setting call state to ringing (was idle) - this is an incoming call")
        setIsInitiator(false)
        setShowIncomingCall(true)
        return "ringing"
      } else {
        console.log("[WebRTC] Preserving call state and initiator status", { prevState })
        return prevState
      }
    })
    console.log("[WebRTC] Call ringing state updated", { callId: incomingCallId })
  }, [role])

  const endCall = useCallback(() => {
    if (isEndingCallRef.current) {
      console.log("[WebRTC] endCall already in progress, ignoring duplicate call")
      return
    }
    
    console.log("[WebRTC] endCall called", { callId, callState, role })
    isEndingCallRef.current = true
    
    if (callId) {
      console.log("[WebRTC] Sending call.end")
      send({
        type: "call.end",
        call_id: callId,
      })
    }

    if (localStreamRef.current) {
      localStreamRef.current.getTracks().forEach((track) => track.stop())
      localStreamRef.current = null
    }

    if (remoteStreamRef.current) {
      remoteStreamRef.current.getTracks().forEach((track) => track.stop())
      remoteStreamRef.current = null
    }

    if (peerConnectionRef.current) {
      peerConnectionRef.current.close()
      peerConnectionRef.current = null
    }

    if (localAudioRef.current) {
      localAudioRef.current.srcObject = null
    }

    if (remoteAudioRef.current) {
      remoteAudioRef.current.srcObject = null
    }

    pendingIceRef.current = []
    setCallState("idle")
    setIsInitiator(false)
    setCallId(null)
    setShowIncomingCall(false)
    isEndingCallRef.current = false
    console.log("[WebRTC] Call ended, state reset to idle")
  }, [callId, send, callState, role])

  useEffect(() => {
    endCallRef.current = endCall
  }, [endCall])

  const toggleMute = useCallback(() => {
    if (localStreamRef.current) {
      localStreamRef.current.getAudioTracks().forEach((track) => {
        track.enabled = !track.enabled
      })
    }
  }, [])

  const toggleRemoteAudio = useCallback(() => {
    setIsRemoteAudioEnabled((prev) => {
      if (remoteAudioRef.current) {
        remoteAudioRef.current.muted = !prev
      }
      return !prev
    })
  }, [])

  useEffect(() => {
    return () => {
      console.log("[WebRTC] Component unmounting, cleaning up call")
      const currentCallId = callIdRef.current
      const currentCallState = callStateRef.current
      if (currentCallId || currentCallState !== "idle") {
        if (currentCallId) {
          send({
            type: "call.end",
            call_id: currentCallId,
          })
        }
        if (localStreamRef.current) {
          localStreamRef.current.getTracks().forEach((track) => track.stop())
        }
        if (remoteStreamRef.current) {
          remoteStreamRef.current.getTracks().forEach((track) => track.stop())
        }
        if (peerConnectionRef.current) {
          peerConnectionRef.current.close()
        }
      }
    }
  }, [send])

  return {
    callState,
    isInitiator,
    isRemoteAudioEnabled,
    callId,
    showIncomingCall,
    startCall,
    acceptCall,
    endCall,
    handleCallIncoming,
    handleCallRinging,
    handleCallAccepted,
    handleCallOffer,
    handleCallAnswer,
    handleIceCandidate,
    toggleMute,
    toggleRemoteAudio,
    localAudioRef,
    remoteAudioRef,
  }
}
