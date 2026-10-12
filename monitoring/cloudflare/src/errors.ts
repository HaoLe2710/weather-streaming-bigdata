export type ErrorCode =
  | "INVALID_JSON"
  | "INVALID_SNAPSHOT_SCHEMA"
  | "PAYLOAD_TOO_LARGE"
  | "UNSUPPORTED_MEDIA_TYPE"
  | "AUTHENTICATION_REQUIRED"
  | "AUTHENTICATION_FAILED"
  | "AUTHENTICATION_NOT_CONFIGURED"
  | "READ_AUTH_REQUIRED"
  | "READ_AUTH_NOT_CONFIGURED"
  | "SNAPSHOT_ID_CONFLICT"
  | "INVALID_QUERY"
  | "NOT_FOUND"
  | "INTERNAL_ERROR";

export class ApiError extends Error {
  constructor(
    public readonly status: 400 | 401 | 403 | 404 | 409 | 413 | 415 | 429 | 500 | 503,
    public readonly code: ErrorCode,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export function errorBody(code: ErrorCode, message: string, requestId: string) {
  return { error: { code, message, requestId } };
}
