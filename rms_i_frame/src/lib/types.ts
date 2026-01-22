export type Role = "user" | "superadmin"

export type ServerJoinedUser = {
  type: "joined"
  chat_id: string
  role: "user"
}

export type HierarchyAdmin = {
  id: string
  name: string
  userName: string
  phone?: string
}

export type HierarchyMaster = {
  id: string
  name: string
  userName: string
  phone?: string
}

export type ServerJoinedAdminList = {
  type: "joined"
  role: "superadmin" | "admin"
  needs_selection: true
  hierarchy?: {
    type: "superadmin"
    admins: HierarchyAdmin[]
  } | {
    type: "admin"
    masters: HierarchyMaster[]
  }
  chatrooms: {
    role: string
    chat_id: string
    user_id: string
    is_user_active: boolean
    is_superadmin_active: boolean
    updated_time: string
    room_type?: string
    user?: {
      name: string
      userName: string
      phone?: string
    }
  }[]
}

export type ServerSelected = {
  type: "selected"
  chat_id: string
  role: "superadmin"
}

export type ServerAdminSelected = {
  type: "admin_selected"
  admin_id: string
  chatrooms: {
    role: string
    chat_id: string
    user_id: string
    is_user_active: boolean
    is_superadmin_active: boolean
    updated_time: string
    room_type?: string
    user?: {
      name: string
      userName: string
      phone?: string
    }
  }[]
  masters: HierarchyMaster[]
  pagination?: {
    total_count: number
    total_pages: number
    current_page: number
    limit: number
  }
}

export type ServerMasterSelected = {
  type: "master_selected"
  master_id: string
  chatrooms: {
    role: string
    chat_id: string
    user_id: string
    is_user_active: boolean
    is_superadmin_active: boolean
    updated_time: string
    room_type?: string
    user?: {
      name: string
      userName: string
      phone?: string
    }
  }[]
  pagination?: {
    total_count: number
    total_pages: number
    current_page: number
    limit: number
  }
}

export type ServerPong = { type: "pong" }

export type ServerTextMessage = {
  type: "message"
  from: "user" | "admin" | "bot" | "superadmin"
  message: string
  message_id: string
  chat_id: string
  created_time: string
  meta?: { domain?: "out_of_scope" | string; reason?: string }
}

export type ServerFileMessage = {
  type: "message"
  from: "user" | "admin" | "superadmin"
  is_file: true
  kind: "file"
  file_url: string
  file_name: string
  file_type: string
  message_id: string
  chat_id: string
  created_time: string
}

export type ServerAudioMessage = {
  type: "message"
  from: "user" | "admin" | "superadmin"
  is_file: true
  kind: "audio"
  audio_url: string
  audio_name: string
  audio_type: string
  file_url?: string
  file_name?: string
  file_type?: string
  message_id: string
  chat_id: string
  created_time: string
}

export type ServerError = {
  type: "error"
  error: "invalid_json" | "no_chat_selected" | string
}

export type ServerCallIncoming = {
  type: "call.incoming"
  call_id: string
  chat_id: string
  from_user_id: string
  from_role: string
  to_role: string
}

export type ServerCallRinging = {
  type: "call.ringing"
  call_id: string
  chat_id: string
}

export type ServerCallAccepted = {
  type: "call.accepted"
  call_id: string
  chat_id: string
}

export type ServerCallOffer = {
  type: "call.offer"
  call_id: string
  chat_id: string
  sdp: RTCSessionDescriptionInit
}

export type ServerCallAnswer = {
  type: "call.answer"
  call_id: string
  chat_id: string
  sdp: RTCSessionDescriptionInit
}

export type ServerCallIce = {
  type: "call.ice"
  call_id: string
  chat_id: string
  candidate: RTCIceCandidateInit
}

export type ServerCallEnded = {
  type: "call.ended"
  call_id: string
  chat_id: string
}

export type ServerCallError = {
  type: "call.error"
  error: string
}

export type ServerEvent =
  | ServerJoinedUser
  | ServerJoinedAdminList
  | ServerSelected
  | ServerAdminSelected
  | ServerMasterSelected
  | ServerPong
  | ServerTextMessage
  | ServerFileMessage
  | ServerAudioMessage
  | ServerError
  | ServerCallIncoming
  | ServerCallRinging
  | ServerCallAccepted
  | ServerCallOffer
  | ServerCallAnswer
  | ServerCallIce
  | ServerCallEnded
  | ServerCallError

export type ClientPing = { type: "ping" }
export type ClientSelectRoom = { type: "select_chatroom"; chat_id: string }
export type ClientSelectAdmin = { type: "select_admin"; admin_id: string }
export type ClientSelectMaster = { type: "select_master"; master_id: string; admin_id?: string }
export type ClientSendText = { type: "message"; text: string }
export type ClientCallStart = { type: "call.start" }
export type ClientCallAccept = { type: "call.accept"; call_id: string }
export type ClientCallReject = { type: "call.reject"; call_id: string }
export type ClientCallOffer = { type: "call.offer"; call_id: string; sdp: RTCSessionDescriptionInit }
export type ClientCallAnswer = { type: "call.answer"; call_id: string; sdp: RTCSessionDescriptionInit }
export type ClientCallIce = { type: "call.ice"; call_id: string; candidate: RTCIceCandidateInit }
export type ClientCallEnd = { type: "call.end"; call_id: string }
export type ClientEvent = ClientPing | ClientSelectRoom | ClientSelectAdmin | ClientSelectMaster | ClientSendText | ClientCallStart | ClientCallAccept | ClientCallReject | ClientCallOffer | ClientCallAnswer | ClientCallIce | ClientCallEnd

export type ConversationItem =
  | {
      kind: "text"
      from: "user" | "admin" | "bot" | "superadmin"
      text: string
      created_at: string
      message_id?: string
      meta?: { domain?: string; reason?: string }
      status?: "sending" | "sent" | "delivered"
    }
  | {
      kind: "file"
      from: any
      file_url: string
      file_name: string
      file_type: string
      created_at: string
      message_id?: string
      status?: "sending" | "sent" | "delivered"
    }
  | {
      kind: "audio"
      from: "user" | "admin" | "superadmin"
      audio_url: string
      audio_name: string
      audio_type: string
      created_at: string
      message_id?: string
      status?: "sending" | "sent" | "delivered"
    }

export type ChatroomDetail = {
  chatroom: {
    id: string
    user_id: string
    super_admin_id?: string
    status: "open" | "closed" | string
    is_user_active: boolean
    is_superadmin_active: boolean
    created_time: string
    updated_time: string
  }
  conversation: (
    | { from: "user" | "bot" | "admin"; text: string; created_at: string }
    | {
        type: "file"
        from: "user" | "admin"
        file_url: string
        file_name?: string
        file_type?: string
        created_at: string
      }
    | { type: "audio"; from: "user" | "admin"; audio_url: string; created_at: string }
  )[]
}
