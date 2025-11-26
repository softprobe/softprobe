import { getApiBaseUrl } from '../utils/config';
import type { TraceFilterData, UserSession, QuerySessionResponseType, SessionResponseType, TraceViewResponse, Span } from '../types';

/**
 * Query session data with pagination and optional filters
 * GET {API_BASE_URL}/sessions
 * Query: size, cursor?, serviceName?, httpStatusCode?, startTimeFrom?, startTimeTo?
 */
export const querySessions = async (
  cursor: string | null = null,
  size: number = 10,
  filterData?: TraceFilterData
): Promise<QuerySessionResponseType> => {
  const apiBaseUrl = getApiBaseUrl();
  if (!apiBaseUrl) {
    throw new Error('API_BASE_URL is not configured. Please set NEXT_PUBLIC_API_BASE_URL environment variable.');
  }

  // Build pagination and filter parameters
  const params = new URLSearchParams({
    size: String(size),
  });
  if (cursor) params.set('cursor', cursor);
  if (filterData?.startTime) params.set('startTimeFrom', filterData.startTime.toISOString());
  if (filterData?.endTime) params.set('startTimeTo', filterData.endTime.toISOString());
  if (filterData?.serviceName) params.set('serviceName', filterData.serviceName);
  if (filterData?.httpStatusCode !== undefined && filterData?.httpStatusCode !== null) {
    params.set('httpStatusCode', String(filterData.httpStatusCode));
  }

  const url = `${apiBaseUrl}/sessions?${params.toString()}`;

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      'Accept-Encoding': 'gzip, deflate, br',
    },
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  const sessionResponse: SessionResponseType = await response.json();

  // Server returns data sorted by time descending
  const sessions: UserSession[] = sessionResponse.sessions.map((session) => ({
    appId: filterData?.appId || '',
    sessionId: session.sessionId,
    createTime: new Date(session.createdAt).getTime(),
    spanIds: session.spanIds || undefined,
  }));

  // 兼容后端不返回 hasMore 的情况：通过 nextCursor 或页码信息计算
  const computedHasMore = Boolean(sessionResponse.nextCursor) ||
    (typeof sessionResponse.currentPage === 'number' && typeof sessionResponse.totalPages === 'number'
      ? sessionResponse.currentPage < sessionResponse.totalPages - 1
      : false);

  return {
    sessions,
    totalCount: sessionResponse.totalCount,
    nextCursor: sessionResponse.nextCursor,
    hasMore: sessionResponse.hasMore ?? computedHasMore,
  };
};

// 解析 raw_json 字符串为对象
const parseRawJson = (rawJsonString: string): any => {
  try {
    return JSON.parse(rawJsonString);
  } catch (error) {
    console.warn('Failed to parse raw_json:', error);
    return {};
  }
};

// 处理 span 数据，解析 raw_json
const processSpanData = (span: Span): Span => {
  // 处理当前span的raw_json
  let processedSpan = span;
  if (span.attributes?.raw_json) {
    const parsedRawJson = parseRawJson(span.attributes.raw_json);
    processedSpan = {
      ...span,
      attributes: {
        ...span.attributes,
        raw_json: parsedRawJson,
      },
    };
  }

  // 递归处理children
  if (span.children && span.children.length > 0) {
    processedSpan = {
      ...processedSpan,
      children: span.children.map(child => processSpanData(child))
    };
  }

  return processedSpan;
};

export const fetchTraceDataNew = async (sessionId: string): Promise<TraceViewResponse | null> => {
  const apiBaseUrl = getApiBaseUrl();
  if (!apiBaseUrl) {
    throw new Error('API_BASE_URL is not configured. Please set NEXT_PUBLIC_API_BASE_URL environment variable.');
  }

  try {
    const url = `${apiBaseUrl}/sessions/${sessionId}`;

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        'Accept-Encoding': 'gzip, deflate, br',
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data: TraceViewResponse = await response.json();

    // 处理每个 trace 中的 spans，解析 raw_json
    const processedData: TraceViewResponse = {
      ...data,
      traces: data.traces.map(trace => ({
        ...trace,
        spans: trace.spans.map(processSpanData),
      })),
    };

    return processedData;
  } catch (error) {
    console.log('🔄 [fetchTraceDataNew] 使用 mock 数据作为 fallback');
    // 返回适配后的 mock 数据作为 fallback
    return null;
  }
};

