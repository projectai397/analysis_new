export type Role = "user" | "superadmin"

export type ServerJoinedUser = {
  type: "joined"
  chat_id: string
  role: "user"
}

export type ServerJoinedAdminList = {
  type: "joined"
  role: "superadmin"
  needs_selection: true
  chatrooms: {
    chat_id: string
    user_id: string
    is_user_active: boolean
    is_superadmin_active: boolean
    updated_time: string
  }[]
}

export type ServerSelected = {
  type: "selected"
  chat_id: string
  role: "superadmin"
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

export type ServerEvent =
  | ServerJoinedUser
  | ServerJoinedAdminList
  | ServerSelected
  | ServerPong
  | ServerTextMessage
  | ServerFileMessage
  | ServerAudioMessage
  | ServerError

export type ClientPing = { type: "ping" }
export type ClientSelectRoom = { type: "select_chatroom"; chat_id: string }
export type ClientSendText = { type: "message"; text: string }
export type ClientEvent = ClientPing | ClientSelectRoom | ClientSendText

export type ConversationItem =
  | {
      kind: "text"
      from: "user" | "admin" | "bot" | "superadmin"
      text: string
      created_at: string
      message_id?: string
      meta?: { domain?: string; reason?: string }
    }
  | {
      kind: "file"
      from: any
      file_url: string
      file_name: string
      file_type: string
      created_at: string
      message_id?: string
    }
  | {
      kind: "audio"
      from: "user" | "admin" | "superadmin"
      audio_url: string
      audio_name: string
      audio_type: string
      created_at: string
      message_id?: string
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
