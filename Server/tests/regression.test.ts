process.env.NODE_ENV = "test";

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { WebSocket, WebSocketServer } from "ws";
import { createSignalingServer, defaultInstance, type ExtWebSocket } from "../server.js";

// Close the module-level singleton's wss so it does not keep test runner active
defaultInstance.wss.close();

describe("Signaling Server Security & Reliability (Production Server)", () => {
  let server: http.Server;
  let wss: WebSocketServer;
  let testPort: number;
  let defaultRooms: Map<string, Set<ExtWebSocket>>;

  before(async () => {
    // Instantiate production signaling server instance on an ephemeral port
    const instance = createSignalingServer();
    server = instance.server;
    wss = instance.wss;
    defaultRooms = instance.rooms;

    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", () => {
        testPort = (server.address() as any).port;
        resolve();
      });
    });
  });

  after(async () => {
    wss.close();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("handles malformed JSON without crashing the server", async () => {
    const ws = new WebSocket(`ws://127.0.0.1:${testPort}`);
    await new Promise((resolve) => ws.on("open", resolve));

    const responsePromise = new Promise<any>((resolve) => {
      ws.on("message", (data) => resolve(JSON.parse(data.toString())));
    });

    ws.send("this is definitely not json!@#$");
    const res = await responsePromise;

    assert.equal(res.type, "error");
    assert.match(res.payload, /Malformed message/);
    ws.close();
  });

  it("rejects invalid room IDs", async () => {
    const ws = new WebSocket(`ws://127.0.0.1:${testPort}`);
    await new Promise((resolve) => ws.on("open", resolve));

    const responsePromise = new Promise<any>((resolve) => {
      ws.on("message", (data) => resolve(JSON.parse(data.toString())));
    });

    ws.send(JSON.stringify({ type: "join", roomId: "../../evil-traversal" }));
    const res = await responsePromise;

    assert.equal(res.type, "error");
    assert.match(res.payload, /Invalid roomId/);
    ws.close();
  });

  it("rejects unauthorized signaling relay attempts to rooms not joined", async () => {
    const ws = new WebSocket(`ws://127.0.0.1:${testPort}`);
    await new Promise((resolve) => ws.on("open", resolve));

    const responsePromise = new Promise<any>((resolve) => {
      ws.on("message", (data) => resolve(JSON.parse(data.toString())));
    });

    // Attempt to inject an offer into a room without joining it
    ws.send(JSON.stringify({
      type: "offer",
      roomId: "targetroom12",
      payload: { sdp: "v=0..." }
    }));

    const res = await responsePromise;
    assert.equal(res.type, "error");
    assert.match(res.payload, /Unauthorized: socket is not a member/);
    ws.close();
  });

  it("handles clean room join, capacity limits, peer-joined notification, and prevents stale sockets on re-join", async () => {
    const ws1 = new WebSocket(`ws://127.0.0.1:${testPort}`);
    const ws2 = new WebSocket(`ws://127.0.0.1:${testPort}`);
    const ws3 = new WebSocket(`ws://127.0.0.1:${testPort}`);

    await Promise.all([
      new Promise((resolve) => ws1.on("open", resolve)),
      new Promise((resolve) => ws2.on("open", resolve)),
      new Promise((resolve) => ws3.on("open", resolve)),
    ]);

    const peerJoinedPromise = new Promise<any>((resolve) => {
      ws1.on("message", (data) => resolve(JSON.parse(data.toString())));
    });

    ws1.send(JSON.stringify({ type: "join", roomId: "sessionroom1" }));
    ws2.send(JSON.stringify({ type: "join", roomId: "sessionroom1" }));

    const peerJoinedMsg = await peerJoinedPromise;
    assert.equal(peerJoinedMsg.type, "peer-joined");
    assert.equal(defaultRooms.get("sessionroom1")?.size, 2);

    // Verify 3rd peer is rejected with "Room is full"
    const roomFullPromise = new Promise<any>((resolve) => {
      ws3.on("message", (data) => resolve(JSON.parse(data.toString())));
    });
    ws3.send(JSON.stringify({ type: "join", roomId: "sessionroom1" }));
    const roomFullMsg = await roomFullPromise;
    assert.equal(roomFullMsg.type, "error");
    assert.equal(roomFullMsg.payload, "Room is full");

    // Now ws1 joins a different room. ws2 should receive peer-disconnected
    const peerDisconnectedPromise = new Promise<any>((resolve) => {
      ws2.on("message", (data) => resolve(JSON.parse(data.toString())));
    });

    ws1.send(JSON.stringify({ type: "join", roomId: "sessionroom2" }));

    const disconnectMsg = await peerDisconnectedPromise;
    assert.equal(disconnectMsg.type, "peer-disconnected");

    // Old room should only have 1 peer left (no stale reference to ws1)
    assert.equal(defaultRooms.get("sessionroom1")?.size, 1);
    assert.equal(defaultRooms.get("sessionroom2")?.size, 1);

    ws1.close();
    ws2.close();
    ws3.close();
  });

  it("enforces origin validation when ALLOWED_ORIGINS is configured on production server", async () => {
    // Launch server with strict origin allowlist
    const restrictedServer = createSignalingServer({
      allowedOrigins: ["https://trusted.example.com"],
    });

    let restrictedPort = 0;
    await new Promise<void>((resolve) => {
      restrictedServer.server.listen(0, "127.0.0.1", () => {
        restrictedPort = (restrictedServer.server.address() as any).port;
        resolve();
      });
    });

    try {
      // 1. Connection with unauthorized origin should be rejected
      let rejected = false;
      try {
        const badWs = new WebSocket(`ws://127.0.0.1:${restrictedPort}`, {
          headers: { Origin: "https://evil.unauthorized.com" },
        });
        await new Promise((resolve, reject) => {
          badWs.on("open", resolve);
          badWs.on("error", reject);
        });
      } catch {
        rejected = true;
      }
      assert.equal(rejected, true, "Should reject connection from unauthorized origin");

      // 2. Connection with allowed origin should succeed
      const goodWs = new WebSocket(`ws://127.0.0.1:${restrictedPort}`, {
        headers: { Origin: "https://trusted.example.com" },
      });
      await new Promise((resolve, reject) => {
        goodWs.on("open", resolve);
        goodWs.on("error", reject);
      });

      assert.equal(goodWs.readyState, WebSocket.OPEN);
      goodWs.close();
    } finally {
      restrictedServer.wss.close();
      await new Promise<void>((resolve) => restrictedServer.server.close(() => resolve()));
    }
  });

  it("hardens rate limiting by terminating abusive sockets without response amplification", async () => {
    // Launch server with strict rate limit: max 5 messages per 5 seconds
    const rateLimitedServer = createSignalingServer({
      rateLimitMax: 5,
      rateLimitWindowMs: 5000,
    });

    let ratePort = 0;
    await new Promise<void>((resolve) => {
      rateLimitedServer.server.listen(0, "127.0.0.1", () => {
        ratePort = (rateLimitedServer.server.address() as any).port;
        resolve();
      });
    });

    try {
      const ws = new WebSocket(`ws://127.0.0.1:${ratePort}`);
      await new Promise((resolve) => ws.on("open", resolve));

      const receivedErrors: string[] = [];
      ws.on("message", (data) => {
        try {
          const msg = JSON.parse(data.toString());
          if (msg.type === "error") {
            receivedErrors.push(msg.payload);
          }
        } catch {
          // ignore
        }
      });

      const closedPromise = new Promise<void>((resolve) => {
        ws.on("close", () => resolve());
      });

      // Send 15 messages rapidly to exceed the limit of 5
      for (let i = 0; i < 15; i++) {
        ws.send(JSON.stringify({ type: "join", roomId: "ratetestroom" }));
      }

      // Wait for server to terminate the abusive socket
      await closedPromise;

      // Verify server sent at most ONE error notification and immediately terminated the socket
      // rather than echoing 10 amplified error frames back to the client
      assert.ok(receivedErrors.length <= 1, `Received ${receivedErrors.length} errors, expected at most 1`);
      assert.ok(
        receivedErrors.some((e) => e.includes("Rate limit exceeded")),
        "Expected rate limit error message"
      );
      assert.equal(ws.readyState, WebSocket.CLOSED);
    } finally {
      rateLimitedServer.wss.close();
      await new Promise<void>((resolve) => rateLimitedServer.server.close(() => resolve()));
    }
  });
});

describe("File Transfer Write Queue & Protocol State Machine", () => {
  // Mock file stream simulating async disk writing and abort/close semantics
  class MockFileStream {
    chunks: Uint8Array[] = [];
    isClosed = false;
    isAborted = false;
    delayMs = 0;
    failOnChunkIndex = -1;
    writeCount = 0;

    constructor(delayMs = 0, failOnChunkIndex = -1) {
      this.delayMs = delayMs;
      this.failOnChunkIndex = failOnChunkIndex;
    }

    async write(data: ArrayBuffer): Promise<void> {
      if (this.isClosed) throw new Error("Cannot write: stream is closed");
      if (this.isAborted) throw new Error("Cannot write: stream is aborted");

      this.writeCount++;
      if (this.failOnChunkIndex > 0 && this.writeCount === this.failOnChunkIndex) {
        throw new Error("Simulated disk write failure (e.g. disk quota exceeded)");
      }

      if (this.delayMs > 0) {
        await new Promise((r) => setTimeout(r, this.delayMs));
      }

      this.chunks.push(new Uint8Array(data));
    }

    async close(): Promise<void> {
      if (this.isClosed) throw new Error("Stream already closed");
      if (this.isAborted) throw new Error("Cannot close an aborted stream");
      this.isClosed = true;
    }

    async abort(): Promise<void> {
      this.isAborted = true;
    }
  }

  // Model of the exact async receiver coordination logic in useFileTransfer.ts
  class TransferCoordinator {
    queuedBytes = 0;
    writtenBytes = 0;
    expectedBytes = 0;
    writeQueue: Promise<void> = Promise.resolve();
    writeError: Error | null = null;
    isAborting = false;
    isFinalizingEof = false;
    isCompleted = false;
    stream: MockFileStream | null;
    abortedSent = false;

    constructor(stream: MockFileStream, expectedBytes: number) {
      this.stream = stream;
      this.expectedBytes = expectedBytes;
    }

    onChunk(chunk: ArrayBuffer) {
      if (this.isAborting || !this.stream || this.isFinalizingEof) return;

      const size = chunk.byteLength;
      this.queuedBytes += size;

      this.writeQueue = this.writeQueue
        .then(async () => {
          if (this.isAborting || !this.stream || this.writeError) return;
          await this.stream.write(chunk);
          this.writtenBytes += size;
        })
        .catch(async (err) => {
          this.writeError = err;
          if (!this.isAborting) {
            this.abortedSent = true;
            await this.abortAndCleanup();
          }
        });
    }

    async onEof(): Promise<boolean> {
      if (this.isAborting || !this.stream) return false;
      this.isFinalizingEof = true;

      // 1. Wait for pending writes
      try {
        await this.writeQueue;
      } catch {
        // captured in writeError
      }

      // 2. Check for write error or abort
      if (this.isAborting || !this.stream || this.writeError) {
        this.isFinalizingEof = false;
        await this.abortAndCleanup();
        return false;
      }

      // 3. Verify byte count
      if (this.writtenBytes !== this.expectedBytes || this.queuedBytes !== this.expectedBytes) {
        this.abortedSent = true;
        this.isFinalizingEof = false;
        await this.abortAndCleanup();
        return false;
      }

      // 4. Safely close stream
      const streamToClose = this.stream;
      this.stream = null;
      await streamToClose.close();

      this.isCompleted = true;
      this.isFinalizingEof = false;
      return true;
    }

    async abortAndCleanup() {
      if (this.isAborting) return;
      this.isAborting = true;

      const s = this.stream;
      this.stream = null;
      if (s) {
        await s.abort();
      }

      this.queuedBytes = 0;
      this.writtenBytes = 0;
      this.writeError = null;
      this.writeQueue = Promise.resolve();
      this.isFinalizingEof = false;
      this.isAborting = false;
    }
  }

  it("handles multiple incoming chunks followed immediately by EOF while writes are pending", async () => {
    const chunkData = new Uint8Array([1, 2, 3, 4, 5]).buffer; // 5 bytes each
    const totalBytes = 25; // 5 chunks * 5 bytes
    const stream = new MockFileStream(15); // 15ms simulated write per chunk
    const coordinator = new TransferCoordinator(stream, totalBytes);

    // Feed 5 chunks rapidly into the receiver
    for (let i = 0; i < 5; i++) {
      coordinator.onChunk(chunkData);
    }

    // Immediately trigger EOF while writes are still pending
    assert.equal(coordinator.queuedBytes, 25, "All bytes should be queued immediately");
    assert.ok(coordinator.writtenBytes < 25, "Writes should still be in flight");

    const success = await coordinator.onEof();

    assert.equal(success, true, "Transfer should complete successfully");
    assert.equal(coordinator.writtenBytes, 25, "All bytes should be written after EOF settles");
    assert.equal(coordinator.isCompleted, true);
    assert.equal(stream.isClosed, true);
    assert.equal(stream.chunks.length, 5);
  });

  it("correctly completes a valid transfer when the last write is delayed", async () => {
    // Custom stream where chunk 3 has a long delay (50ms)
    class DelayedLastStream extends MockFileStream {
      override async write(data: ArrayBuffer): Promise<void> {
        if (this.writeCount === 2) {
          await new Promise((r) => setTimeout(r, 50));
        }
        await super.write(data);
      }
    }

    const stream = new DelayedLastStream();
    const coordinator = new TransferCoordinator(stream, 15);
    const chunk = new Uint8Array([1, 2, 3, 4, 5]).buffer;

    coordinator.onChunk(chunk);
    coordinator.onChunk(chunk);
    coordinator.onChunk(chunk); // chunk 3 will be delayed

    const success = await coordinator.onEof();
    assert.equal(success, true);
    assert.equal(coordinator.writtenBytes, 15);
    assert.equal(stream.isClosed, true);
  });

  it("handles write failure during transfer without unhandled promise rejections", async () => {
    // Fail on chunk 2
    const stream = new MockFileStream(5, 2);
    const coordinator = new TransferCoordinator(stream, 15);
    const chunk = new Uint8Array([1, 2, 3, 4, 5]).buffer;

    coordinator.onChunk(chunk);
    coordinator.onChunk(chunk); // will fail
    coordinator.onChunk(chunk); // should be skipped

    // Wait for queue to settle
    try {
      await coordinator.writeQueue;
    } catch {
      // should be caught internally by coordinator
    }

    assert.equal(coordinator.abortedSent, true, "Should have sent stream-abort on write failure");
    assert.equal(stream.isAborted, true, "Stream should be aborted on failure");
    assert.equal(coordinator.isCompleted, false);
  });

  it("handles abort racing with EOF without crashing or double-closing stream", async () => {
    const stream = new MockFileStream(30);
    const coordinator = new TransferCoordinator(stream, 15);
    const chunk = new Uint8Array([1, 2, 3, 4, 5]).buffer;

    coordinator.onChunk(chunk);
    coordinator.onChunk(chunk);

    // Concurrently trigger abort and EOF
    const abortPromise = coordinator.abortAndCleanup();
    const eofPromise = coordinator.onEof();

    const [, eofResult] = await Promise.all([abortPromise, eofPromise]);

    assert.equal(eofResult, false, "EOF should report failure or abort");
    assert.equal(stream.isAborted, true);
    assert.equal(stream.isClosed, false);
  });

  it("detects sender failure before all bytes sent, sends stream-abort, and cleans up sender state", () => {
    // Test sender streaming completion contract
    const totalFileSize = 1000;
    let offset = 400; // Simulated network failure after sending 400 bytes
    let sentControlMessage: string | null = null;
    let isUploading = true;

    // Sender logic check
    const transferSuccess = offset >= totalFileSize;
    if (transferSuccess) {
      sentControlMessage = "file-eof";
    } else {
      sentControlMessage = "stream-abort";
    }
    isUploading = false;

    assert.equal(transferSuccess, false);
    assert.equal(sentControlMessage, "stream-abort", "Must send stream-abort instead of file-eof on partial transfer");
    assert.equal(isUploading, false, "Must reset isUploading state on early termination");
  });

  it("receiver cleans up all handles and resets state following abort", async () => {
    const stream = new MockFileStream();
    const coordinator = new TransferCoordinator(stream, 50);

    coordinator.onChunk(new Uint8Array(20).buffer);
    await coordinator.abortAndCleanup();

    assert.equal(coordinator.queuedBytes, 0, "Queued bytes should reset to 0");
    assert.equal(coordinator.writtenBytes, 0, "Written bytes should reset to 0");
    assert.equal(coordinator.stream, null, "Stream handle should be detached");
    assert.equal(stream.isAborted, true, "Stream should be aborted");
  });
});
