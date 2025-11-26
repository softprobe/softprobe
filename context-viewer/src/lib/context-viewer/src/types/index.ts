/**
 * Type definitions for the context-viewer library
 */

export interface TraceFilterData {
  appId?: string;
  startTime: Date | null;
  endTime: Date | null;
  serviceName?: string;
  httpStatusCode?: number;
  sessionId?: string;
}

export interface UserSession {
  appId: string;
  sessionId: string;
  createTime: number;
  spanIds?: string[];
}

export interface QuerySessionResponseType {
  sessions: UserSession[];
  totalCount: number;
  nextCursor: string | null;
  hasMore: boolean;
}

// Session 相关的类型定义
export interface Session {
  sessionId: string;
  createdAt: string;
  serviceName?: string | null;
  httpStatusCode?: number | null;
  spanIds?: string[] | null;
}

export interface SessionResponseType {
  success: boolean;
  message: string | null;
  sessions: Session[];
  totalCount: number;
  createdAt: string;
  nextCursor: string | null;
  hasMore?: boolean;
  currentPage?: number;
  pageSize?: number;
  totalPages?: number;
}

export interface Project {
  id: string;
  name: string;
  description?: string;
}

// 新的类型定义，用于匹配 traceviewMockdata.json 的数据结构
export interface SpanAttribute {
  [key: string]: any;
}

export interface TraceViewResponse {
  success: boolean;
  message: string | null;
  sessionId: string;
  traces: Context[];
  totalTraces: number;
  totalSpans: number;
}

export interface Context {
  traceId: string;
  spans: Span[];
  totalDuration: number;
  startTime: string;
  endTime: string;
}

export interface Span {
  traceId: string;
  spanId: string;
  parentSpanId: string | null;
  name: string;
  kind: string;
  startTime: string;
  endTime: string;
  duration: number;
  status: string;
  attributes: SpanAttribute;
  serviceName: string | null;
  spanType: string;
  children: Span[];
  requestLogs: any[];
  responseLogs: any[];
}

