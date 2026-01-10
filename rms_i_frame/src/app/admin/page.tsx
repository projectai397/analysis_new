"use client";

import useSWR from "swr";
import { useChatSocket } from "@/lib/ws";
import { getChatroom } from "@/lib/api";
import { useEffect, useMemo, useState, Suspense } from "react";
import type { ConversationItem, ChatroomDetail } from "@/types/chatbot_type";
import { MessageList } from "@/components/chat/message-list";
import { Composer } from "@/components/chat/composer";
import { useSearchParams } from "next/navigation";
import { useWebRTC } from "@/hooks/use-webrtc";
import { CallUI } from "@/components/call/call-ui";
import { Phone } from "lucide-react";

function decodeJWT(token: string): Record<string, unknown> | null {
    try {
        const parts = token.split(".");
        if (parts.length !== 3) {
            return null;
        }
        const payload = parts[1];
        const base64 = payload.replace(/-/g, "+").replace(/_/g, "/");
        const padded = base64.padEnd(
            base64.length + ((4 - (base64.length % 4)) % 4),
            "="
        );
        const jsonPayload = atob(padded);
        return JSON.parse(jsonPayload);
    } catch {
        return null;
    }
}

function formatChatTime(dateString: string): string {
    const date = new Date(dateString);
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    const messageDate = new Date(date.getFullYear(), date.getMonth(), date.getDate());

    if (messageDate.getTime() === today.getTime()) {
        return date.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", hour12: true });
    } else if (messageDate.getTime() === yesterday.getTime()) {
        return "Yesterday";
    } else {
        const diffTime = now.getTime() - date.getTime();
        const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
        if (diffDays < 7) {
            return date.toLocaleDateString("en-US", { weekday: "short" });
        } else {
            return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
        }
    }
}

function getInitials(name: string): string {
    if (!name) return "?";
    const parts = name.trim().split(/\s+/);
    if (parts.length >= 2) {
        return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
    }
    return name[0].toUpperCase();
}

export default function AdminPage() {
    return (
        <Suspense fallback={null}>
            <AdminPageInner />
        </Suspense>
    );
}

const STATIC_MASTER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2OTQzYjViNzgwZGEyMDVlZmUzYWYzNGIiLCJuYW1lIjoiUFJPRklUQjJDIiwicGhvbmUiOiIxMTExMTMzMzMzIiwidXNlck5hbWUiOiJQUk9GSVRCMkMiLCJyb2xlIjoibWFzdGVyIiwicm9sZV9pZCI6IjY0YjYzNzU1YzcxNDYxYzUwMmVhNDcxNSIsInByZWZlcmVuY2UiOm51bGwsImRldmljZVRva2VuIjpudWxsLCJkZXZpY2VJZCI6ImI3ODQyNGE4LTQ2YjAtNDJjMy04N2ExLWY0YWMwYzJjMjE3ZiIsImRldmljZVR5cGUiOiJtb2JpbGUiLCJzZXF1ZW5jZSI6MjAwMDUsImlhdCI6MTc2NzkzNTU4NCwiZXhwIjoxNzY4NTQwMzg0fQ.WqYnjqiFKIHY4IF3AY4N0CC2QjMNoW1iKiGjxa78a0A"

function AdminPageInner() {
    const searchParams = useSearchParams();
    const token = searchParams.get("token") || STATIC_MASTER_TOKEN;

    if (!token) {
        return (
            <main className="h-[100dvh] grid place-items-center bg-[#e5ddd5] dark:bg-[#0b141a]">
                <div className="text-sm text-[#667781] dark:text-[#8696a0]">
                    No token provided in URL
                </div>
            </main>
        );
    }

    const session = decodeJWT(token);
    if (!session) {
        return (
            <main className="h-[100dvh] grid place-items-center bg-[#e5ddd5] dark:bg-[#0b141a]">
                <div className="text-sm text-[#667781] dark:text-[#8696a0]">
                    Invalid token format
                </div>
            </main>
        );
    }

    return <AdminView token={token} />;
}

function AdminView({ token }: { token: string }) {
    const {
        status,
        chatId,
        chatrooms,
        selectRoom,
        messages,
        resetLiveMessages,
        sendText,
        send,
        callEvent,
        clearCallEvent,
    } = useChatSocket({
        token,
        role: "superadmin" as any,
    });

    const { data } = useSWR<ChatroomDetail>(
        chatId ? ["chatroom", chatId, token] : null,
        (key: [string, string, string]) => getChatroom(key[2], key[1]),
        { revalidateOnFocus: true }
    );

    const [showChatView, setShowChatView] = useState(false);
    const [isSearchOpen, setIsSearchOpen] = useState(false);
    const [searchQuery, setSearchQuery] = useState("");

    const {
        callState,
        isInitiator,
        isRemoteAudioEnabled,
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
    } = useWebRTC({
        chatId,
        role: "master",
        send,
    });

    useEffect(() => {
        if (!callEvent) return;

        console.log("[Master] Received call event:", callEvent.type, callEvent);

        if (callEvent.type === "call.incoming") {
            console.log("[Master] Processing call.incoming - showing toaster");
            handleCallIncoming(callEvent.call_id);
            clearCallEvent();
        } else if (callEvent.type === "call.ringing") {
            console.log("[Master] Processing call.ringing");
            handleCallRinging(callEvent.call_id);
            clearCallEvent();
        } else if (callEvent.type === "call.accepted") {
            console.log("[Master] Processing call.accepted");
            handleCallAccepted();
            clearCallEvent();
        } else if (callEvent.type === "call.offer") {
            console.log("[Master] Processing call.offer");
            handleCallOffer(callEvent.sdp);
            clearCallEvent();
        } else if (callEvent.type === "call.answer") {
            console.log("[Master] Processing call.answer");
            handleCallAnswer(callEvent.sdp);
            clearCallEvent();
        } else if (callEvent.type === "call.ice") {
            console.log("[Master] Processing call.ice");
            handleIceCandidate(callEvent.candidate);
            clearCallEvent();
        } else if (callEvent.type === "call.ended") {
            console.log("[Master] Processing call.ended - call was ended");
            console.warn("[Master] Call ended. This might indicate:");
            console.warn("  - User ended the call");
            console.warn("  - Connection issue");
            console.warn("  - Server timeout");
            endCall();
            clearCallEvent();
        } else if (callEvent.type === "call.error") {
            console.error("[Master] Call error:", callEvent.error);
            if (callEvent.error === "target_offline") {
                alert("Target is offline or not connected.");
            } else {
                alert(`Call error: ${callEvent.error}`);
            }
            endCall();
            clearCallEvent();
        }
    }, [callEvent, handleCallIncoming, handleCallRinging, handleCallAccepted, handleCallOffer, handleCallAnswer, handleIceCandidate, endCall, clearCallEvent]);

    useEffect(() => {
        resetLiveMessages();
        setShowChatView(false);
    }, [chatId, resetLiveMessages]);

    useEffect(() => {
        if (chatId) {
            setShowChatView(true);
        }
    }, [chatId]);

    useEffect(() => {
        console.log("[Master] Chatrooms/Status effect:", { 
            chatroomsCount: chatrooms.length, 
            chatId, 
            status,
            firstChatroomId: chatrooms[0]?.chat_id 
        });
        if (chatrooms.length > 0 && !chatId && status === "open") {
            const firstChatroom = chatrooms[0];
            console.log("[Master] Auto-selecting first chatroom:", firstChatroom.chat_id);
            selectRoom(firstChatroom.chat_id);
            setShowChatView(true);
        }
    }, [chatrooms, chatId, status, selectRoom]);

    const historyItems: ConversationItem[] = useMemo(() => {
        if (!data?.conversation) return [];

        return data.conversation.map((c: any) => {
            const from =
                c.from === "agent" || c.from === "superadmin" ? "admin" : c.from;
            if (c.type === "file") {
                return {
                    kind: "file",
                    from,
                    file_url: c.file_url,
                    file_name: c.file_name || "File",
                    file_type: c.file_type || "",
                    created_at: c.created_at,
                } as ConversationItem;
            }
            if (c.type === "audio") {
                return {
                    kind: "audio",
                    from,
                    audio_url: c.audio_url,
                    audio_name: "Audio",
                    audio_type: "",
                    created_at: c.created_at,
                } as ConversationItem;
            }
            return {
                kind: "text",
                from,
                text: c.text,
                created_at: c.created_at,
            } as ConversationItem;
        });
    }, [data]);

    const normalizedLive = useMemo(() => {
        return messages.map((m) => ({
            ...m,
            from: m.from === "superadmin" ? "admin" : m.from,
        }));
    }, [messages]);

    const combined = useMemo(
        () => [...historyItems, ...normalizedLive],
        [historyItems, normalizedLive]
    );
    console.log(combined, "combined");
    const selectedChatroom = chatrooms.find((r) => r.chat_id === chatId);
    const displayName = selectedChatroom?.user?.name || selectedChatroom?.user?.userName || "Unknown";

    const handleChatSelect = (chatId: string) => {
        selectRoom(chatId);
        setShowChatView(true);
    };

    const handleBackToList = () => {
        setShowChatView(false);
    };

    return (
        <main className="h-[100dvh] w-full max-w-full mx-auto flex flex-col bg-[#e5ddd5] dark:bg-[#0b141a]">
            <div className="flex flex-1 min-h-0 overflow-hidden">
                <aside
                    className={`bg-white dark:bg-[#111b21] w-full md:w-96 flex flex-col border-r border-[#e4e6eb] dark:border-[#313d45] ${showChatView ? "hidden md:flex" : "flex"
                        }`}
                >
                    <header
                        className="bg-[#008069] dark:bg-[#202c33] px-4 py-3 flex items-center shadow-sm flex-shrink-0"
                        role="banner"
                    >
                        <h1 className="text-lg font-semibold text-[#ffffff] dark:text-[#e9edef]">
                            Chats
                        </h1>
                    </header>

                    <div className="flex-1 overflow-y-auto">
                        {!chatrooms.length ? (
                            <div className="p-4 text-center text-sm text-[#667781] dark:text-[#8696a0]">
                                No chats available
                            </div>
                        ) : (
                            <div className="flex flex-col">
                                {chatrooms.map((r) => {
                                    const isSelected = chatId === r.chat_id;
                                    const name = r.user?.name || r.user?.userName || "Unknown";
                                    const initials = getInitials(name);

                                    return (
                                        <button
                                            key={r.chat_id}
                                            onClick={() => handleChatSelect(r.chat_id)}
                                            className={`w-full flex items-center gap-3 px-4 py-3 hover:bg-[#f5f6f6] dark:hover:bg-[#202c33] transition-colors border-b border-[#e4e6eb] dark:border-[#313d45] ${isSelected ? "bg-[#f0f2f5] dark:bg-[#202c33]" : ""
                                                }`}
                                        >
                                            <div className="relative flex-shrink-0">
                                                <div className="w-12 h-12 rounded-full bg-gradient-to-br from-[#008069] to-[#006b58] dark:from-[#53bdeb] dark:to-[#008069] flex items-center justify-center text-white font-medium text-lg">
                                                    {initials}
                                                </div>
                                                {r.is_user_active && (
                                                    <div className="absolute bottom-0 right-0 w-3 h-3 bg-[#53bdeb] border-2 border-white dark:border-[#111b21] rounded-full"></div>
                                                )}
                                            </div>
                                            <div className="flex-1 min-w-0">
                                                <div className="flex items-center justify-between mb-1">
                                                    <h3 className="text-base font-medium text-[#111b21] dark:text-[#e9edef] truncate">
                                                        {name}
                                                    </h3>
                                                    <span className="text-xs text-[#667781] dark:text-[#8696a0] flex-shrink-0 ml-2">
                                                        {formatChatTime(r.updated_time)}
                                                    </span>
                                                </div>
                                                <div className="flex items-center justify-between">
                                                    <p className="text-sm text-[#667781] dark:text-[#8696a0] truncate">
                                                        {r.user?.userName ? `@${r.user.userName}` : "No username"}
                                                    </p>
                                                    {!r.is_user_active && (
                                                        <span className="text-xs text-[#667781] dark:text-[#8696a0] flex-shrink-0 ml-2">
                                                            offline
                                                        </span>
                                                    )}
                                                </div>
                                            </div>
                                        </button>
                                    );
                                })}
                            </div>
                        )}
                    </div>
                </aside>

                <section
                    className={`bg-[#e5ddd5] dark:bg-[#0b141a] flex-1 flex flex-col ${showChatView ? "flex" : "hidden md:flex"
                        }`}
                >
                    {!chatId ? (
                        <div className="flex-1 flex items-center justify-center">
                            <div className="text-center px-4">
                                <div className="mb-4">
                                    <svg
                                        width="160"
                                        height="160"
                                        viewBox="0 0 160 160"
                                        fill="none"
                                        xmlns="http://www.w3.org/2000/svg"
                                        className="mx-auto opacity-50"
                                    >
                                        <circle cx="80" cy="80" r="60" stroke="currentColor" strokeWidth="2" className="text-[#667781] dark:text-[#8696a0]" />
                                        <path
                                            d="M60 80 L75 95 L100 70"
                                            stroke="currentColor"
                                            strokeWidth="3"
                                            strokeLinecap="round"
                                            strokeLinejoin="round"
                                            className="text-[#667781] dark:text-[#8696a0]"
                                        />
                                    </svg>
                                </div>
                                <p className="text-lg font-medium text-[#111b21] dark:text-[#e9edef] mb-2">
                                    Admin on Web
                                </p>
                                <p className="text-sm text-[#667781] dark:text-[#8696a0]">
                                    Select a chat to start conversation
                                </p>
                            </div>
                        </div>
                    ) : (
                        <>
                            <header
                                className="bg-[#008069] dark:bg-[#202c33] px-4 py-3 flex items-center justify-between shadow-sm flex-shrink-0"
                                role="banner"
                            >
                                <div className="flex items-center gap-3 flex-1 min-w-0">
                                    <button
                                        onClick={handleBackToList}
                                        className="md:hidden p-2 text-[#ffffff] dark:text-[#8696a0] hover:bg-[#008069]/80 dark:hover:bg-[#313d45] rounded-full transition-colors flex-shrink-0"
                                        aria-label="Back to chats"
                                    >
                                        <svg
                                            width="20"
                                            height="20"
                                            viewBox="0 0 24 24"
                                            fill="none"
                                            xmlns="http://www.w3.org/2000/svg"
                                            aria-hidden="true"
                                        >
                                            <path
                                                d="M15 18l-6-6 6-6"
                                                stroke="currentColor"
                                                strokeWidth="2"
                                                strokeLinecap="round"
                                                strokeLinejoin="round"
                                            />
                                        </svg>
                                    </button>
                                    <div className="relative flex-shrink-0">
                                        <div className="w-10 h-10 rounded-full bg-gradient-to-br from-[#008069] to-[#006b58] dark:from-[#53bdeb] dark:to-[#008069] flex items-center justify-center text-white font-medium">
                                            {getInitials(displayName)}
                                        </div>
                                        {selectedChatroom?.is_user_active && (
                                            <div className="absolute bottom-0 right-0 w-3 h-3 bg-[#53bdeb] border-2 border-[#008069] dark:border-[#202c33] rounded-full"></div>
                                        )}
                                    </div>
                                    <div className="flex-1 min-w-0">
                                        <div className="flex items-center gap-2">
                                            <h2 className="text-base font-semibold text-[#ffffff] dark:text-[#e9edef] truncate">
                                                {displayName}
                                            </h2>
                                        </div>
                                        <div className="flex items-center gap-1.5 mt-0.5">
                                            <span
                                                className={`text-xs ${selectedChatroom?.is_user_active
                                                    ? "text-[#ffffff] dark:text-[#8696a0]"
                                                    : "text-[#ffffff]/80 dark:text-[#8696a0]"
                                                    }`}
                                            >
                                                {selectedChatroom?.is_user_active ? "online" : "offline"}
                                            </span>
                                            {selectedChatroom?.is_user_active && (
                                                <span className="w-2 h-2 rounded-full bg-[#53bdeb] animate-pulse" aria-hidden="true" />
                                            )}
                                        </div>
                                    </div>
                                </div>
                                <div className="flex items-center gap-2 flex-shrink-0">
                                    {!isSearchOpen && (
                                        <button
                                            onClick={() => startCall()}
                                            disabled={!chatId || callState !== "idle"}
                                            className="p-2 text-[#ffffff] dark:text-[#8696a0] hover:bg-[#008069]/80 dark:hover:bg-[#313d45] rounded-full transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                                            aria-label="Call user"
                                            title="Call user"
                                        >
                                            <Phone className="w-5 h-5" aria-hidden="true" />
                                        </button>
                                    )}
                                    {isSearchOpen ? (
                                        <div className="flex items-center gap-2 bg-[#008069]/20 dark:bg-[#2a3942]/50 rounded-lg px-2 py-1 flex-1 max-w-[200px]">
                                            <svg
                                                width="16"
                                                height="16"
                                                viewBox="0 0 24 24"
                                                fill="none"
                                                xmlns="http://www.w3.org/2000/svg"
                                                className="text-[#ffffff] dark:text-[#8696a0] flex-shrink-0"
                                                aria-hidden="true"
                                            >
                                                <path
                                                    d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                                                    stroke="currentColor"
                                                    strokeWidth="2"
                                                    strokeLinecap="round"
                                                    strokeLinejoin="round"
                                                />
                                            </svg>
                                            <input
                                                type="text"
                                                value={searchQuery}
                                                onChange={(e) => setSearchQuery(e.target.value)}
                                                placeholder="Search..."
                                                className="bg-transparent border-none outline-none text-[#ffffff] dark:text-[#e9edef] text-sm placeholder:text-[#ffffff]/60 dark:placeholder:text-[#8696a0] flex-1 min-w-0"
                                                autoFocus
                                                onKeyDown={(e) => {
                                                    if (e.key === "Escape") {
                                                        setIsSearchOpen(false);
                                                        setSearchQuery("");
                                                    }
                                                }}
                                            />
                                            <button
                                                onClick={() => {
                                                    setIsSearchOpen(false);
                                                    setSearchQuery("");
                                                }}
                                                className="text-[#ffffff] dark:text-[#8696a0] hover:opacity-80 p-1"
                                                aria-label="Close search"
                                            >
                                                <svg
                                                    width="16"
                                                    height="16"
                                                    viewBox="0 0 24 24"
                                                    fill="none"
                                                    xmlns="http://www.w3.org/2000/svg"
                                                    aria-hidden="true"
                                                >
                                                    <path
                                                        d="M18 6L6 18M6 6l12 12"
                                                        stroke="currentColor"
                                                        strokeWidth="2"
                                                        strokeLinecap="round"
                                                        strokeLinejoin="round"
                                                    />
                                                </svg>
                                            </button>
                                        </div>
                                    ) : (
                                        <button
                                            onClick={() => setIsSearchOpen(true)}
                                            className="p-2 text-[#ffffff] dark:text-[#8696a0] hover:bg-[#008069]/80 dark:hover:bg-[#313d45] rounded-full transition-colors"
                                            aria-label="Search messages"
                                            title="Search"
                                        >
                                            <svg
                                                width="20"
                                                height="20"
                                                viewBox="0 0 24 24"
                                                fill="none"
                                                xmlns="http://www.w3.org/2000/svg"
                                                aria-hidden="true"
                                            >
                                                <path
                                                    d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                                                    stroke="currentColor"
                                                    strokeWidth="2"
                                                    strokeLinecap="round"
                                                    strokeLinejoin="round"
                                                />
                                            </svg>
                                        </button>
                                    )}
                                </div>
                            </header>

                            <MessageList items={combined} viewerRole="superadmin" searchQuery={searchQuery} />

                            <div className="flex-shrink-0">
                                <Composer
                                    token={token}
                                    mode="admin"
                                    chatId={chatId}
                                    onSendText={async (t) => {
                                        sendText(t);
                                    }}
                                />
                            </div>
                        </>
                    )}
                </section>
            </div>
            <CallUI
                callState={callState}
                isInitiator={isInitiator}
                isRemoteAudioEnabled={isRemoteAudioEnabled}
                showIncomingCall={showIncomingCall}
                onEndCall={endCall}
                onAcceptCall={acceptCall}
                onToggleMute={toggleMute}
                onToggleRemoteAudio={toggleRemoteAudio}
                localAudioRef={localAudioRef}
                remoteAudioRef={remoteAudioRef}
                displayName={displayName}
            />
        </main>
    );
}
