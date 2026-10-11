import { useRef, useState, useCallback } from "react";
import { sanitizeFilename } from "./sanitizeFilename";

export { sanitizeFilename };

export function useFileTransfer(dataChannel: React.RefObject<RTCDataChannel | null>) {
  const [incomingRequest, setIncomingRequest] = useState<{ name: string; size: number; type: string } | null>(null);

  // Progress state
  const [uploadProgress, setUploadProgress] = useState<number>(0);
  const [isUploading, setIsUploading] = useState<boolean>(false);

  const [downloadProgress, setDownloadProgress] = useState<number>(0);
  const [isDownloading, setIsDownloading] = useState<boolean>(false);

  // Distinguish queued bytes (on wire) vs successfully written bytes (flushed to disk/stream)
  const queuedBytes = useRef<number>(0);
  const writtenBytes = useRef<number>(0);
  const activeDownload = useRef<{ name: string; size: number } | null>(null);
  const uploadingFile = useRef<File | null>(null);
  const fileStream = useRef<FileSystemWritableFileStream | null>(null);
  const streamSaverWriter = useRef<WritableStreamDefaultWriter<Uint8Array> | null>(null);

  // Concurrency & lifecycle guards
  const writeQueue = useRef<Promise<void>>(Promise.resolve());
  const writeError = useRef<Error | null>(null);
  const isAborting = useRef<boolean>(false);
  const isFinalizingEof = useRef<boolean>(false);
  const cancelBackpressure = useRef<(() => void) | null>(null);

  const abortAndCleanup = useCallback(async () => {
    if (isAborting.current) return;
    isAborting.current = true;
    console.warn("Aborting transfer and cleaning up streams...");

    if (cancelBackpressure.current) {
      cancelBackpressure.current();
      cancelBackpressure.current = null;
    }

    // Safely capture and detach stream handles to prevent concurrent double-abort or closing
    const stream = fileStream.current;
    fileStream.current = null;
    const writer = streamSaverWriter.current;
    streamSaverWriter.current = null;

    if (stream) {
      try {
        await stream.abort();
      } catch {
        // ignore stream abort failure
      }
    }

    if (writer) {
      try {
        await writer.abort();
      } catch {
        // ignore stream abort failure
      }
    }

    uploadingFile.current = null;
    activeDownload.current = null;
    queuedBytes.current = 0;
    writtenBytes.current = 0;
    writeError.current = null;
    writeQueue.current = Promise.resolve();
    isFinalizingEof.current = false;
    isAborting.current = false;

    setIncomingRequest(null);
    setIsDownloading(false);
    setDownloadProgress(0);
    setIsUploading(false);
    setUploadProgress(0);
  }, []);

  const startStreaming = useCallback(async (file: File) => {
    const CHUNK_SIZE = 65536; // 64KB
    const BUFFER_THRESHOLD = 1048576; // 1MB
    let offset = 0;
    let lastPaint = 0;
    let streamFailed = false;

    if (!dataChannel.current || dataChannel.current.readyState !== "open") {
      console.warn("Cannot stream: DataChannel not open");
      return;
    }

    dataChannel.current.bufferedAmountLowThreshold = BUFFER_THRESHOLD;

    setIsUploading(true);
    setUploadProgress(0);

    // Explicit handling for zero-byte files
    if (file.size === 0) {
      setUploadProgress(100);
      if (dataChannel.current?.readyState === "open") {
        try {
          dataChannel.current.send(JSON.stringify({ type: "file-eof" }));
          console.log("Finished sending 0-byte file");
        } catch (err) {
          console.error("Failed to send 0-byte file-eof:", err);
        }
      }
      uploadingFile.current = null;
      setIsUploading(false);
      setUploadProgress(0);
      return;
    }

    while (offset < file.size) {
      if (!dataChannel.current || dataChannel.current.readyState !== "open" || !uploadingFile.current) {
        console.log("Transfer cancelled or channel closed. Halting loop");
        streamFailed = true;
        break;
      }

      // Cancellation-aware backpressure wait
      if (dataChannel.current.bufferedAmount > BUFFER_THRESHOLD) {
        try {
          await new Promise<void>((resolve, reject) => {
            let settled = false;
            const channel = dataChannel.current;

            const cleanup = () => {
              settled = true;
              cancelBackpressure.current = null;
              if (channel) {
                channel.onbufferedamountlow = null;
                channel.removeEventListener("close", onClose);
                channel.removeEventListener("error", onError);
              }
              clearTimeout(timeoutId);
            };

            const onLow = () => {
              if (!settled) {
                cleanup();
                resolve();
              }
            };

            const onClose = () => {
              if (!settled) {
                cleanup();
                reject(new Error("DataChannel closed during backpressure wait"));
              }
            };

            const onError = () => {
              if (!settled) {
                cleanup();
                reject(new Error("DataChannel error during backpressure wait"));
              }
            };

            cancelBackpressure.current = () => {
              if (!settled) {
                cleanup();
                reject(new Error("Transfer aborted"));
              }
            };

            if (channel) {
              channel.onbufferedamountlow = onLow;
              channel.addEventListener("close", onClose);
              channel.addEventListener("error", onError);
            }

            // 15-second safety timeout
            const timeoutId = setTimeout(() => {
              if (!settled) {
                cleanup();
                reject(new Error("Backpressure timeout"));
              }
            }, 15000);
          });
        } catch (bpError) {
          console.warn("Backpressure wait cancelled or failed:", bpError);
          streamFailed = true;
          break;
        }
      }

      try {
        const slice = file.slice(offset, offset + CHUNK_SIZE);
        const buffer = await slice.arrayBuffer();

        if (!dataChannel.current || dataChannel.current.readyState !== "open" || !uploadingFile.current) {
          streamFailed = true;
          break;
        }

        dataChannel.current.send(buffer);
        offset += CHUNK_SIZE;

        const currentPercent = Math.min(100, Math.round((offset / file.size) * 100));
        if (currentPercent > lastPaint) {
          setUploadProgress(currentPercent);
          lastPaint = currentPercent;
        }
      } catch (err) {
        console.error("Failed to read or send chunk:", err);
        streamFailed = true;
        break;
      }
    }

    if (cancelBackpressure.current) {
      cancelBackpressure.current();
      cancelBackpressure.current = null;
    }

    const transferSuccess = !streamFailed && offset >= file.size;

    if (transferSuccess) {
      if (dataChannel.current?.readyState === "open") {
        try {
          dataChannel.current.send(JSON.stringify({ type: "file-eof" }));
          console.log("Finished sending all chunks");
        } catch (eofErr) {
          console.error("Failed to send file-eof:", eofErr);
        }
      }
    } else {
      console.warn(`Transfer ended incompletely (${offset}/${file.size} bytes). Notifying receiver.`);
      if (dataChannel.current?.readyState === "open") {
        try {
          dataChannel.current.send(JSON.stringify({ type: "stream-abort" }));
        } catch (abortErr) {
          console.error("Failed to send stream-abort to receiver:", abortErr);
        }
      }
    }

    uploadingFile.current = null;
    setIsUploading(false);
    setUploadProgress(0);
  }, [dataChannel]);

  const handleDataMessage = useCallback(async (e: MessageEvent) => {
    if (typeof e.data === "string") {
      let msg: { type?: string; name?: string; size?: number; fileType?: string } | null = null;
      try {
        msg = JSON.parse(e.data);
      } catch (parseErr) {
        console.warn("Failed to parse incoming DataChannel string as JSON:", parseErr);
        return;
      }

      if (!msg || typeof msg !== "object") return;

      if (msg.type === "file-meta") {
        if (typeof msg.name !== "string" || typeof msg.size !== "number" || !Number.isFinite(msg.size) || msg.size < 0) {
          console.warn("Received malformed file-meta message:", msg);
          if (dataChannel.current?.readyState === "open") {
            try {
              dataChannel.current.send(JSON.stringify({ type: "stream-abort" }));
            } catch {
              // ignore send error
            }
          }
          return;
        }

        const safeName = sanitizeFilename(msg.name);
        setIncomingRequest({
          name: safeName,
          size: Math.floor(msg.size),
          type: typeof msg.fileType === "string" ? msg.fileType : "application/octet-stream",
        });
      } else if (msg.type === "file-eof") {
        if (isAborting.current || !activeDownload.current) {
          return;
        }

        isFinalizingEof.current = true;

        // 1. Wait for all pending writes in the queue to settle
        try {
          await writeQueue.current;
        } catch {
          // write errors are captured in writeError.current
        }

        // 2. Check if an error occurred during writes or if transfer was aborted during wait
        if (isAborting.current || !activeDownload.current || writeError.current) {
          console.warn("Transfer failed or was aborted before EOF could be finalized:", writeError.current);
          isFinalizingEof.current = false;
          await abortAndCleanup();
          return;
        }

        const expectedBytes = activeDownload.current.size;
        const actualWritten = writtenBytes.current;
        const actualQueued = queuedBytes.current;

        // 3. Verify received & written byte count before claiming success
        if (actualWritten !== expectedBytes || actualQueued !== expectedBytes) {
          console.error(
            `File transfer byte count mismatch! Expected ${expectedBytes} bytes, queued ${actualQueued} bytes, written ${actualWritten} bytes.`
          );
          if (dataChannel.current?.readyState === "open") {
            try {
              dataChannel.current.send(JSON.stringify({ type: "stream-abort" }));
            } catch {
              // ignore send error
            }
          }
          isFinalizingEof.current = false;
          await abortAndCleanup();
          return;
        }

        // 4. Safely close stream (detach before closing to prevent concurrent close/abort)
        const streamToClose = fileStream.current;
        fileStream.current = null;
        const writerToClose = streamSaverWriter.current;
        streamSaverWriter.current = null;

        try {
          if (streamToClose) {
            await streamToClose.close();
          } else if (writerToClose) {
            await writerToClose.close();
          }
          console.log(`File downloaded successfully. Verified ${actualWritten} bytes.`);
        } catch (closeErr) {
          console.error("Failed to close file stream on EOF:", closeErr);
          isFinalizingEof.current = false;
          await abortAndCleanup();
          return;
        }

        // 5. Reset download state cleanly
        isFinalizingEof.current = false;
        setIsDownloading(false);
        setDownloadProgress(0);
        activeDownload.current = null;
        queuedBytes.current = 0;
        writtenBytes.current = 0;
        writeError.current = null;
      } else if (msg.type === "stream-abort") {
        console.log("Peer rejected/cancelled the transfer!");
        await abortAndCleanup();
      } else if (msg.type === "stream-ready") {
        if (uploadingFile.current) {
          startStreaming(uploadingFile.current);
        }
      }
    } else if (e.data instanceof ArrayBuffer) {
      if (isAborting.current || !activeDownload.current || isFinalizingEof.current) {
        return;
      }

      const chunk = e.data;
      const chunkSize = chunk.byteLength;

      // Count received chunk exactly once upon arrival into the queue
      queuedBytes.current += chunkSize;

      // Serialize chunk write through promise chain
      writeQueue.current = writeQueue.current
        .then(async () => {
          if (isAborting.current || !activeDownload.current || writeError.current) {
            return;
          }

          if (fileStream.current) {
            await fileStream.current.write(chunk);
          } else if (streamSaverWriter.current) {
            await streamSaverWriter.current.write(new Uint8Array(chunk));
          } else {
            throw new Error("No active file stream available to write chunk");
          }

          writtenBytes.current += chunkSize;
          if (activeDownload.current) {
            const totalSize = activeDownload.current.size;
            const currentPercent = totalSize === 0
              ? 100
              : Math.min(100, Math.round((writtenBytes.current / totalSize) * 100));

            setDownloadProgress((prev) => (currentPercent > prev ? currentPercent : prev));
          }
        })
        .catch(async (err) => {
          const error = err instanceof Error ? err : new Error(String(err));
          console.error("Stream write failed in queue:", error);
          writeError.current = error;

          if (!isAborting.current) {
            if (dataChannel.current?.readyState === "open") {
              try {
                dataChannel.current.send(JSON.stringify({ type: "stream-abort" }));
              } catch {
                // ignore send error
              }
            }
            await abortAndCleanup();
          }
        });
    }
  }, [abortAndCleanup, dataChannel, startStreaming]);

  const handleFileSelect = (file: File) => {
    if (!dataChannel.current || dataChannel.current.readyState !== "open") return;

    console.log("File captured! Ready to process:", file.name, file.size);
    uploadingFile.current = file;

    dataChannel.current.send(
      JSON.stringify({
        type: "file-meta",
        name: sanitizeFilename(file.name),
        size: file.size,
        fileType: file.type || "application/octet-stream",
      })
    );
  };

  const declineFile = () => {
    if (dataChannel.current && dataChannel.current.readyState === "open") {
      dataChannel.current.send(JSON.stringify({ type: "stream-abort" }));
    }
    setIncomingRequest(null);
  };

  const acceptFile = async () => {
    if (!incomingRequest || !dataChannel.current) return;

    if ("showSaveFilePicker" in window) {
      try {
        const pickerWindow = window as Window & {
          showSaveFilePicker?: (options?: { suggestedName?: string }) => Promise<FileSystemFileHandle>;
        };

        if (!pickerWindow.showSaveFilePicker) {
          throw new Error("showSaveFilePicker not supported");
        }

        const handle = await pickerWindow.showSaveFilePicker({
          suggestedName: incomingRequest.name,
        });

        fileStream.current = await handle.createWritable();

        // Download progress setup
        queuedBytes.current = 0;
        writtenBytes.current = 0;
        writeError.current = null;
        writeQueue.current = Promise.resolve();
        isAborting.current = false;
        isFinalizingEof.current = false;

        setDownloadProgress(incomingRequest.size === 0 ? 100 : 0);
        setIsDownloading(true);
        activeDownload.current = {
          name: incomingRequest.name,
          size: incomingRequest.size,
        };

        dataChannel.current.send(JSON.stringify({ type: "stream-ready" }));
        setIncomingRequest(null);
      } catch (err) {
        console.log("User cancelled save dialog or picker failed:", err);
        if (dataChannel.current?.readyState === "open") {
          dataChannel.current.send(JSON.stringify({ type: "stream-abort" }));
        }
        setIncomingRequest(null);
      }
    } else {
      try {
        const streamSaver = (await import("streamsaver")).default;

        // Use StreamSaver write stream
        const file = streamSaver.createWriteStream(incomingRequest.name, {
          size: incomingRequest.size,
        });

        streamSaverWriter.current = file.getWriter();

        queuedBytes.current = 0;
        writtenBytes.current = 0;
        writeError.current = null;
        writeQueue.current = Promise.resolve();
        isAborting.current = false;
        isFinalizingEof.current = false;

        setDownloadProgress(incomingRequest.size === 0 ? 100 : 0);
        setIsDownloading(true);
        activeDownload.current = {
          name: incomingRequest.name,
          size: incomingRequest.size,
        };

        dataChannel.current.send(JSON.stringify({ type: "stream-ready" }));
        setIncomingRequest(null);
      } catch (fallbackErr) {
        console.error("StreamSaver initialization failed:", fallbackErr);
        if (dataChannel.current?.readyState === "open") {
          dataChannel.current.send(JSON.stringify({ type: "stream-abort" }));
        }
        setIncomingRequest(null);
      }
    }
  };

  return {
    incomingRequest,
    activeDownload,
    handleDataMessage,
    handleFileSelect,
    acceptFile,
    declineFile,
    abortAndCleanup,
    uploadProgress,
    isUploading,
    downloadProgress,
    isDownloading,
  };
}
