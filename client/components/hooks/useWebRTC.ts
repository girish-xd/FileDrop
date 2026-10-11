import { useEffect, useRef, useState, useCallback } from "react";

const wsURL = process.env.NEXT_PUBLIC_WEBSOCKET_URL;
const iceServers = [
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun2.l.google.com:19302" },
  { urls: "stun:stun4.l.google.com:19302" },
];

export type ConnectionState = "connecting" | "waiting" | "connected" | "error";

export function useWebRTC(
  roomId: string,
  dataChannelRef: React.RefObject<RTCDataChannel | null>,
  onMessage: (e: MessageEvent) => void,
  onDisconnect: () => void,
) {
  const [status, setStatus] = useState<ConnectionState>(() => (!wsURL ? "error" : "connecting"));
  const [errorMessage, setErrorMessage] = useState<string>(() =>
    !wsURL ? "Signaling server URL (NEXT_PUBLIC_WEBSOCKET_URL) is not configured." : ""
  );

  const ws = useRef<WebSocket | null>(null);
  const peerConn = useRef<RTCPeerConnection | null>(null);
  const iceCandidateQueue = useRef<RTCIceCandidateInit[]>([]);

  // STALE CLOSURE FIX: Always keep the latest message handler in a ref
  const onMessageRef = useRef(onMessage);
  const onDisconnectRef = useRef(onDisconnect);

  useEffect(() => {
    onMessageRef.current = onMessage;
  }, [onMessage]);

  useEffect(() => {
    onDisconnectRef.current = onDisconnect;
  }, [onDisconnect]);

  const addCandidates = useCallback(() => {
    if (!peerConn.current) {
      console.log("No peerConnection Found");
      return;
    }

    iceCandidateQueue.current.forEach((candidate) => {
      peerConn.current!.addIceCandidate(new RTCIceCandidate(candidate)).catch(console.error);
    });

    iceCandidateQueue.current = [];
  }, []);

  const setupDataChannel = useCallback(() => {
    if (!dataChannelRef.current) return;

    dataChannelRef.current.binaryType = "arraybuffer";

    dataChannelRef.current.onopen = () => setStatus("connected");
    dataChannelRef.current.onclose = () => {
      setStatus("waiting");
      onDisconnectRef.current?.();
    };

    // Attach File Transfer logic directly into the open pipe
    dataChannelRef.current.onmessage = (e) => onMessageRef.current(e);
  }, [dataChannelRef]);

  const initializeWebRTC = useCallback(() => {
    if (peerConn.current) {
      peerConn.current.close();
      peerConn.current = null;
    }

    const pc = new RTCPeerConnection({ iceServers });
    peerConn.current = pc;

    pc.onconnectionstatechange = () => {
      console.log("WebRTC Status: ", pc.connectionState);

      if (pc.connectionState === "disconnected" || pc.connectionState === "failed") {
        console.warn("WebRTC connection physically dropped!", pc.connectionState);
        onDisconnectRef.current();
        setStatus("waiting");
      } else if (pc.connectionState === "connected") {
        setStatus("connected");
      }
    };

    pc.onicecandidate = (e) => {
      if (e.candidate && ws.current?.readyState === WebSocket.OPEN) {
        ws.current.send(
          JSON.stringify({
            type: "ice-candidate",
            roomId,
            payload: e.candidate,
          })
        );
      }
    };
  }, [roomId]);

  const createOffer = useCallback(async () => {
    if (!peerConn.current) {
      console.log("No RTCwebConnection initialized...");
      return;
    }

    try {
      console.log("Creating offer and data channel");
      dataChannelRef.current = peerConn.current.createDataChannel("Lane");
      setupDataChannel();

      const offer = await peerConn.current.createOffer();
      await peerConn.current.setLocalDescription(offer);

      if (ws.current?.readyState === WebSocket.OPEN) {
        ws.current.send(
          JSON.stringify({
            type: "offer",
            roomId,
            payload: peerConn.current.localDescription,
          })
        );
      }
    } catch (err) {
      console.error("Error creating offer", err);
    }
  }, [dataChannelRef, roomId, setupDataChannel]);

  const createAnswer = useCallback(async (remoteDesc: RTCSessionDescriptionInit) => {
    if (!peerConn.current) {
      console.log("No RTCwebConnection Found...");
      return;
    }

    try {
      console.log("Creating an answer and setting up data channel!");

      await peerConn.current.setRemoteDescription(remoteDesc);

      peerConn.current.ondatachannel = (e) => {
        dataChannelRef.current = e.channel;
        setupDataChannel();
      };

      const answer = await peerConn.current.createAnswer();
      await peerConn.current.setLocalDescription(answer);

      if (ws.current?.readyState === WebSocket.OPEN) {
        ws.current.send(
          JSON.stringify({
            type: "answer",
            roomId,
            payload: peerConn.current.localDescription,
          })
        );
      }
    } catch (error) {
      console.error("Cant create answer...", error);
    }
  }, [dataChannelRef, roomId, setupDataChannel]);

  useEffect(() => {
    if (!wsURL || ws.current) return;

    const websocket = new WebSocket(wsURL);
    ws.current = websocket;

    websocket.onmessage = async (e: MessageEvent) => {
      let newMsg: { type?: string; payload?: unknown } | null = null;
      try {
        newMsg = JSON.parse(e.data);
      } catch (parseError) {
        console.warn("Failed to parse signaling message:", parseError);
        return;
      }

      if (!newMsg || typeof newMsg !== "object") return;
      const { type, payload } = newMsg;

      if (type === "error") {
        setErrorMessage(typeof payload === "string" ? payload : "This room is currently full.");
        setStatus("error");
      } else if (type === "peer-joined") {
        initializeWebRTC();
        await createOffer();
      } else if (type === "offer") {
        initializeWebRTC();
        await createAnswer(payload as RTCSessionDescriptionInit);
        addCandidates();
      } else if (type === "answer") {
        if (!peerConn.current) {
          console.log("No RTC Connection found to set Remote Desc...");
        } else {
          await peerConn.current.setRemoteDescription(payload as RTCSessionDescriptionInit);
          addCandidates();
        }
      } else if (type === "ice-candidate") {
        console.log("Adding ICE candidate");
        if (!peerConn.current) {
          console.log("No peer Connection to add ice candidate!");
        } else if (peerConn.current.remoteDescription && peerConn.current.remoteDescription.type) {
          await peerConn.current.addIceCandidate(payload as RTCIceCandidateInit);
        } else {
          iceCandidateQueue.current.push(payload as RTCIceCandidateInit);
        }
      } else if (type === "peer-disconnected") {
        console.log("Peer disconnected from room");
        onDisconnectRef.current();

        if (peerConn.current) {
          peerConn.current.close();
          peerConn.current = null;
        }
        if (dataChannelRef.current) {
          dataChannelRef.current.close();
          dataChannelRef.current = null;
        }
        setStatus("waiting");
      }
    };

    websocket.onopen = () => {
      websocket.send(JSON.stringify({ type: "join", roomId }));
      setStatus("waiting");
    };

    websocket.onerror = (evt) => {
      console.error("Signaling WebSocket error:", evt);
    };

    return () => {
      websocket.close();
      peerConn.current?.close();
      dataChannelRef.current?.close();

      ws.current = null;
      peerConn.current = null;
      dataChannelRef.current = null;
    };
  }, [addCandidates, createAnswer, createOffer, dataChannelRef, initializeWebRTC, roomId]);

  return { status, errorMessage };
}
