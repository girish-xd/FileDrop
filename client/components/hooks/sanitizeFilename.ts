/**
 * Sanitizes an incoming filename to prevent path traversal, control character injection,
 * and dangerous filesystem characters across Windows, macOS, and Linux.
 */
export function sanitizeFilename(rawName: string): string {
  if (typeof rawName !== "string") return "downloaded_file";

  // 1. Strip directory path separators and traversal sequences (take basename only)
  let clean = rawName.split(/[/\\]/).pop() || "";

  // 2. Remove null bytes and ASCII control characters (0-31, 127)
  clean = clean.replace(/[\x00-\x1f\x7f]/g, "");

  // 3. Remove Windows/Unix reserved filename characters: < > : " / \ | ? *
  clean = clean.replace(/[<>:"/\\|?*]/g, "_");

  // 4. Trim surrounding whitespace and trailing dots
  clean = clean.trim().replace(/\.+$/, "");

  // 5. Fallback if the name is empty or reserved relative paths
  if (!clean || clean === "." || clean === "..") {
    clean = "downloaded_file";
  }

  // 6. Enforce safe length limit (max 255 characters) preserving extension if possible
  if (clean.length > 255) {
    const dotIdx = clean.lastIndexOf(".");
    if (dotIdx > 0 && clean.length - dotIdx <= 10) {
      const ext = clean.slice(dotIdx);
      clean = clean.slice(0, 255 - ext.length) + ext;
    } else {
      clean = clean.slice(0, 255);
    }
  }

  return clean;
}
