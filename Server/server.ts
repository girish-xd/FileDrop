import express from "express";
import { WebSocketServer, WebSocket } from "ws";
import http from "http";
import cors from "cors";

export interface ExtWebSocket extends WebSocket {
    roomId?: string | undefined;
    messageCount?: number | undefined;
    lastReset?: number | undefined;
    isTerminated?: boolean | undefined;
}

export interface SignalingMessage {
    type: "join" | "offer" | "ice-candidate" | "answer" | "peer-joined" | "peer-disconnected" | "error";
    roomId: string;
    payload?: any;
}

export interface BroadcastMessage {
    type: "offer" | "answer" | "ice-candidate" | "peer-joined" | "peer-disconnected" | "error";
    payload?: any;
}

export const PORT = process.env.PORT || 1908;
export const MAX_PAYLOAD = 64 * 1024; // 64KB max message size for signaling
export const MAX_ROOMS = parseInt(process.env.MAX_ROOMS || "1000", 10);
export const ROOM_ID_REGEX = /^[a-zA-Z0-9_-]{4,32}$/;

// Configurable origin allowlist for WebSocket connections.
// Example: ALLOWED_ORIGINS=http://localhost:3000,https://filedrop.example.com
export const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS
    ? process.env.ALLOWED_ORIGINS.split(",").map((o) => o.trim().toLowerCase())
    : null;

export interface ServerOptions {
    port?: number | undefined;
    allowedOrigins?: string[] | null | undefined;
    maxPayload?: number | undefined;
    maxRooms?: number | undefined;
    rateLimitMax?: number | undefined;
    rateLimitWindowMs?: number | undefined;
    server?: http.Server | undefined;
}

export function createSignalingServer(options: ServerOptions = {}) {
    const app = express();
    app.use(cors());

    // Health-check route
    app.get("/", (_req, res) => {
        res.send("Signalling server is alive...");
    });

    const httpServer = options.server || http.createServer(app);
    const rooms = new Map<string, Set<ExtWebSocket>>();
    const allowedOrigins = options.allowedOrigins !== undefined ? options.allowedOrigins : ALLOWED_ORIGINS;
    const maxPayload = options.maxPayload ?? MAX_PAYLOAD;
    const maxRooms = options.maxRooms ?? MAX_ROOMS;
    const rateLimitMax = options.rateLimitMax ?? 100;
    const rateLimitWindowMs = options.rateLimitWindowMs ?? 5000;

    const wss = new WebSocketServer({
        server: httpServer,
        maxPayload,
        verifyClient: (info, callback) => {
            if (!allowedOrigins) {
                // Development/default mode: allow connections without origin restriction
                return callback(true);
            }
            const origin = info.origin?.toLowerCase();
            if (!origin || !allowedOrigins.includes(origin)) {
                console.warn(`[Security] Rejected WebSocket connection from unauthorized origin: ${info.origin}`);
                return callback(false, 403, "Origin not allowed");
            }
            return callback(true);
        },
    });

    function checkRateLimit(ws: ExtWebSocket): boolean {
        const now = Date.now();
        if (!ws.lastReset || now - ws.lastReset > rateLimitWindowMs) {
            ws.lastReset = now;
            ws.messageCount = 1;
            return true;
        }
        ws.messageCount = (ws.messageCount || 0) + 1;
        return ws.messageCount <= rateLimitMax;
    }

    function broadcastToOther(ws: ExtWebSocket, roomId: string, message: BroadcastMessage) {
        const room = rooms.get(roomId);
        if (!room) return;

        const messageString = JSON.stringify(message);
        for (const client of room) {
            if (client !== ws && client.readyState === WebSocket.OPEN) {
                try {
                    client.send(messageString);
                } catch (err) {
                    console.error(`Error sending message to peer in room ${roomId}:`, err);
                }
            }
        }
    }

    function leaveRoom(ws: ExtWebSocket) {
        if (!ws.roomId) return;
        const currentRoomId = ws.roomId;
        ws.roomId = undefined;

        const room = rooms.get(currentRoomId);
        if (room) {
            room.delete(ws);
            console.log(`User left room ${currentRoomId}. Remaining Peers: ${room.size}`);

            // Notify peer about disconnection
            broadcastToOther(ws, currentRoomId, { type: "peer-disconnected" });

            if (room.size === 0) {
                rooms.delete(currentRoomId);
                console.log(`Room ${currentRoomId} deleted`);
            }
        }
    }

    function sendError(ws: ExtWebSocket, errorMessage: string) {
        if (ws.readyState === WebSocket.OPEN) {
            try {
                ws.send(JSON.stringify({
                    type: "error",
                    payload: errorMessage,
                }));
            } catch (err) {
                console.error("Failed to send error frame to socket:", err);
            }
        }
    }

    wss.on("connection", (ws: ExtWebSocket) => {
        ws.on("message", (rawMessage: Buffer | string) => {
            if (ws.isTerminated || ws.readyState !== WebSocket.OPEN) {
                return;
            }

            if (!checkRateLimit(ws)) {
                ws.isTerminated = true;
                console.warn(`[Security] Rate limit exceeded by socket. Terminating socket.`);
                sendError(ws, "Rate limit exceeded. Connection terminated.");
                leaveRoom(ws);
                ws.terminate();
                return;
            }

            // 1. Safe parsing of message
            let parsed: any;
            try {
                const messageAsString = typeof rawMessage === "string" ? rawMessage : rawMessage.toString("utf8");
                parsed = JSON.parse(messageAsString);
            } catch {
                sendError(ws, "Malformed message: invalid JSON");
                return;
            }

            // 2. Validate message structure
            if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
                sendError(ws, "Malformed message: expected JSON object");
                return;
            }

            const { type, roomId, payload } = parsed;

            if (typeof type !== "string" || !["join", "offer", "answer", "ice-candidate"].includes(type)) {
                sendError(ws, `Unsupported message type: ${String(type)}`);
                return;
            }

            if (typeof roomId !== "string" || !ROOM_ID_REGEX.test(roomId)) {
                sendError(ws, "Invalid roomId: must be 4-32 alphanumeric characters");
                return;
            }

            // 3. Handle message types with strict authorization
            if (type === "offer" || type === "answer" || type === "ice-candidate") {
                // Authorization check: sender must be in the specified room
                if (ws.roomId !== roomId || !rooms.get(roomId)?.has(ws)) {
                    sendError(ws, `Unauthorized: socket is not a member of room ${roomId}`);
                    return;
                }

                // Payload structure validation
                if (!payload || typeof payload !== "object") {
                    sendError(ws, `Malformed payload for ${type}`);
                    return;
                }

                broadcastToOther(ws, roomId, { type, payload });
            } else if (type === "join") {
                // If socket is already in this room, treat as no-op
                if (ws.roomId === roomId) {
                    return;
                }

                // If socket was in another room, clean up previous membership first
                if (ws.roomId && ws.roomId !== roomId) {
                    leaveRoom(ws);
                }

                // Enforce server capacity
                if (!rooms.has(roomId)) {
                    if (rooms.size >= maxRooms) {
                        sendError(ws, "Server room capacity reached");
                        return;
                    }
                    rooms.set(roomId, new Set<ExtWebSocket>());
                }

                const room = rooms.get(roomId);
                if (room) {
                    if (room.size >= 2) {
                        sendError(ws, "Room is full");
                        return;
                    }

                    room.add(ws);
                    ws.roomId = roomId;

                    if (room.size === 2) {
                        broadcastToOther(ws, roomId, { type: "peer-joined" });
                    }
                }
            }
        });

        ws.on("close", () => {
            leaveRoom(ws);
        });

        ws.on("error", (err) => {
            console.error("WebSocket client error:", err);
            leaveRoom(ws);
        });
    });

    return {
        app,
        server: httpServer,
        wss,
        rooms,
        broadcastToOther,
        leaveRoom,
    };
}

// Default singleton instance for standard server lifecycle
export const defaultInstance = createSignalingServer();
export const app = defaultInstance.app;
export const server = defaultInstance.server;
export const wss = defaultInstance.wss;
export const rooms = defaultInstance.rooms;
export const broadcastToOther = defaultInstance.broadcastToOther;
export const leaveRoom = defaultInstance.leaveRoom;

export const isTestEnvironment =
    process.env.NODE_ENV === "test" ||
    process.env.npm_lifecycle_event === "test" ||
    process.argv.some((arg) => arg.includes("test"));

// Start listening if run as main script outside test environment
if (!isTestEnvironment) {
    server.listen(Number(PORT), "0.0.0.0", () => {
        console.log("Signalling server running on PORT: ", PORT);
    });
}
