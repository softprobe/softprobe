/**
 * Configuration utilities for the context-viewer library
 */

/**
 * Get the API base URL from environment variable
 * Falls back to empty string if not set (caller should handle this)
 */
export function getApiBaseUrl(): string {
  if (typeof window !== 'undefined') {
    return process.env.NEXT_PUBLIC_API_BASE_URL || '';
  }
  return process.env.NEXT_PUBLIC_API_BASE_URL || '';
}


