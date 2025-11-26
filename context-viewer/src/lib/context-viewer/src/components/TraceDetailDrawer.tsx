'use client';

import React, { useState, useEffect } from 'react';
import dynamic from 'next/dynamic';

// 动态导入 react-json-view 以避免 SSR 问题
const ReactJson = dynamic(() => import('react-json-view'), {
  ssr: false,
  loading: () => (
    <div className="p-3 text-sm text-gray-500">Loading JSON viewer...</div>
  ),
});

import {
  Database,
  Code,
  ArrowRight,
  Clock,
  Globe,
  Settings,
  Zap,
  X,
  Copy,
  Check,
  Maximize2,
  Minimize2,
  RotateCcw,
} from 'lucide-react';
// 从 types 导入 Span 类型
import type { Span } from '../types';
// 导入新的组件
import { UserNodeDetails } from './UserNodeDetails';
import { ServerNodeDetails } from './ServerNodeDetails';

interface TraceDetailDrawerProps {
  node: Span | null;
  isOpen: boolean;
  onClose: () => void;
}

// 扩展node类型以支持UserNode
interface UserNodeData {
  id: string;
  label: string;
  subtitle: string;
  type: string;
  status: 'success' | 'error';
  duration: number;
  serviceName?: string;
  startTime?: string;
  endTime?: string;
  attributes?: any;
  nodeData?: any;
  isRoot?: boolean;
  isLeaf?: boolean;
  traceData?: any;
}

export const TraceDetailDrawer: React.FC<TraceDetailDrawerProps> = ({
  node,
  isOpen,
  onClose,
}) => {
  // State for copy feedback
  const [copiedStates, setCopiedStates] = useState<{ [key: string]: boolean }>(
    {}
  );

  // State for expanded JSON view
  const [expandedJsonKey, setExpandedJsonKey] = useState<string | null>(null);

  // State for closing animation
  const [isClosing, setIsClosing] = useState(false);

  // State to preserve the JSON key during closing animation
  const [closingJsonKey, setClosingJsonKey] = useState<string | null>(null);

  // State for drawer width
  const [drawerWidth, setDrawerWidth] = useState(63); // Default 63% of screen width
  const [isResizing, setIsResizing] = useState(false);

  // Reset expanded state when drawer closes
  useEffect(() => {
    if (!isOpen) {
      setExpandedJsonKey(null);
      setIsClosing(false);
      setClosingJsonKey(null);
      // Ensure scrolling is re-enabled when drawer closes
      if (typeof window !== 'undefined') {
        const drawerContent = document.getElementById('drawer-content');
        if (drawerContent) {
          drawerContent.style.overflow = 'auto';
        }
      }
    }
  }, [isOpen]);

  // Handle mouse down on resize handle
  const handleMouseDown = (e: React.MouseEvent) => {
    e.preventDefault();
    setIsResizing(true);
  };

  // Handle mouse move for resizing
  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing) return;

      const windowWidth = window.innerWidth;
      const newWidth = ((windowWidth - e.clientX) / windowWidth) * 100;

      // Constrain width between 20% and 80% of screen width
      const constrainedWidth = Math.min(Math.max(newWidth, 20), 80);
      setDrawerWidth(constrainedWidth);
    };

    const handleMouseUp = () => {
      setIsResizing(false);
    };

    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
  }, [isResizing]);

  // Copy JSON function
  const copyJsonToClipboard = async (data: any, key: string) => {
    try {
      const jsonString = JSON.stringify(data, null, 2);
      await navigator.clipboard.writeText(jsonString);

      // Show success feedback
      setCopiedStates((prev) => ({ ...prev, [key]: true }));

      // Reset feedback after 2 seconds
      setTimeout(() => {
        setCopiedStates((prev) => ({ ...prev, [key]: false }));
      }, 2000);
    } catch (error) {
      console.error('Failed to copy JSON:', error);
    }
  };

  // Expand/minimize JSON view functions
  const expandJsonView = (key: string) => {
    setIsClosing(false); // Reset closing state
    setExpandedJsonKey(key);

    // Smooth scroll drawer content to top and disable scrolling
    if (typeof window !== 'undefined') {
      const drawerContent = document.getElementById('drawer-content');
      if (drawerContent) {
        drawerContent.scrollTo({
          top: 0,
          behavior: 'smooth',
        });

        // Disable scrolling after smooth scroll completes
        setTimeout(() => {
          if (drawerContent) {
            drawerContent.style.overflow = 'hidden';
          }
        }, 150); // Wait for smooth scroll to complete
      }
    }
  };

  const minimizeJsonView = () => {
    setClosingJsonKey(expandedJsonKey); // Preserve the current key for closing animation
    setIsClosing(true);

    // Start closing animation, then clean up after animation completes
    setTimeout(() => {
      setExpandedJsonKey(null);
      setIsClosing(false);
      setClosingJsonKey(null);

      // Re-enable scrolling
      if (typeof window !== 'undefined') {
        const drawerContent = document.getElementById('drawer-content');
        if (drawerContent) {
          drawerContent.style.overflow = 'auto';
        }
      }
    }, 150); // Match the animation duration
  };

  // Helper function to render JSON content
  const renderJsonContent = (body: string, isExpanded: boolean = false) => {
    try {
      const jsonData = JSON.parse(body);
      return (
        <ReactJson
          src={jsonData}
          theme="rjv-default"
          collapsed={isExpanded ? false : 2}
          displayDataTypes={false}
          displayObjectSize={false}
          enableClipboard={false}
          name={null}
          style={{
            backgroundColor: 'transparent',
            fontFamily: 'Monaco, Consolas, "Courier New", monospace',
            fontSize: isExpanded ? '14px' : '13px',
            padding: '12px',
          }}
        />
      );
    } catch (error) {
      return (
        <pre
          className={`p-3 whitespace-pre-wrap text-gray-900 ${isExpanded ? 'text-sm' : 'text-sm'}`}
        >
          {body}
        </pre>
      );
    }
  };

  // 检查是否为UserNode
  const isUserNode = node && (node as any).type === 'user';
  
  // Map Span data to the format expected by the component
  const nodeData = node
    ? (() => {
        // 如果是UserNode，返回特殊的数据结构
        if (isUserNode) {
          const userData = node as any;
          const traceData = userData.traceData;
          
          // 计算整体时间范围
          const allSpans = traceData?.traces?.flatMap((trace: any) => trace.spans) || [];
          const startTimes = allSpans.map((span: any) => new Date(span.startTime).getTime()).filter((t: any) => !isNaN(t));
          const endTimes = allSpans.map((span: any) => new Date(span.endTime).getTime()).filter((t: any) => !isNaN(t));
          const overallStartTime = startTimes.length > 0 ? Math.min(...startTimes) : null;
          const overallEndTime = endTimes.length > 0 ? Math.max(...endTimes) : null;
          const totalDuration = overallStartTime && overallEndTime ? overallEndTime - overallStartTime : 0;
          
          return {
            operationName: 'User Session',
            categoryType: { name: 'user' },
            timeUsed: totalDuration,
            creationTime: overallStartTime || 0,
            transactionId: traceData?.sessionId || 'Unknown',
            sessionInfo: {
              sessionId: traceData?.sessionId || 'Unknown',
              totalTraces: traceData?.totalTraces || 0,
              totalSpans: traceData?.totalSpans || 0,
              overallStartTime,
              overallEndTime,
              totalDuration,
            },
            traces: traceData?.traces || [],
          };
        }
        
        // 普通Span节点的处理
        const rawJson = node.attributes?.raw_json || {};

        return {
          operationName: node.name,
          categoryType: {
            name:
              node.kind === 'SPAN_KIND_SERVER' || node.kind === '2'
                ? 'server'
                : node.kind === 'SPAN_KIND_CLIENT' || node.kind === '3'
                  ? 'service'
                  : node.kind === 'SPAN_KIND_PRODUCER' || node.kind === '4'
                    ? 'service'
                    : node.kind === 'SPAN_KIND_CONSUMER' || node.kind === '5'
                      ? 'service'
                      : node.kind === 'SPAN_KIND_INTERNAL' || node.kind === '1'
                        ? 'service'
                        : node.kind === 'SPAN_KIND_UNSPECIFIED' ||
                            node.kind === '0'
                          ? 'service'
                          : 'service',
          },
          timeUsed: node.duration,
          creationTime: new Date(node.startTime).getTime(),
          transactionId: node.traceId,
          targetRequest: {
            attributes: {
              HttpMethod: rawJson?.['http_request_header_:method'],
              RequestPath: rawJson?.['http_request_header_:path'],
              Headers: rawJson?.['http_request_header_user-agent'],
              dbName: rawJson?.['db.name'],
              parameters: rawJson?.['db.statement'],
              TableName: rawJson?.['db.mongodb.collection'],
              CacheHit: rawJson?.['cache.hit'],
              TTL: rawJson?.['cache.ttl'],
              MemoryUsage: rawJson?.['cache.memory_usage'],
              // Add more network and system information
              ClientAddress: rawJson?.['http_request_header_:authority'],
              ServerAddress: rawJson?.['server.address'],
              ThreadName: rawJson?.['thread.name'],
              NetworkProtocol: rawJson?.['network.protocol.version'],
              UrlScheme: rawJson?.['http_request_header_:scheme'],
              ContentType: rawJson?.['http_request_header_content-type'],
              RequestId: rawJson?.['http_request_header_x-request-id'],
              SessionId: rawJson?.['http_request_header_x-sp-session-id'],
              ServiceName: rawJson?.['sp_service_name'],
              SpanType: rawJson?.['sp_span_type'],
              TrafficDirection: rawJson?.['sp_traffic_direction'],
              ApiKey: rawJson?.['sp_api_key'],
              UrlHost: rawJson?.['url_host'],
              UrlPath: rawJson?.['url_path'],
            },
            body:
              rawJson?.['http_request_body'] ||
              JSON.stringify(node.attributes, null, 2),
          },
          targetResponse: {
            body:
              rawJson?.['http_response_body'] ||
              JSON.stringify(
                {
                  status: node.status,
                  duration: node.duration,
                  serviceName: node.serviceName,
                  httpStatus: rawJson?.['http_response_status_code'],
                },
                null,
                2
              ),
            headers: {
              status: rawJson?.['http_response_header_:status'],
              contentType: rawJson?.['http_response_header_content-type'],
              date: rawJson?.['http_response_header_date'],
              transferEncoding:
                rawJson?.['http_response_header_transfer-encoding'],
              vary: rawJson?.['http_response_header_vary'],
              upstreamServiceTime:
                rawJson?.['http_response_header_x-envoy-upstream-service-time'],
            },
          },
        };
      })()
    : null;

  // Helper function to get expanded JSON data
  const getExpandedJsonData = (key: string) => {
    if (!nodeData) return null;
    switch (key) {
      case 'request-body':
        return { body: nodeData.targetRequest?.body || '', title: 'Request Body' };
      case 'response-body':
        return { body: nodeData.targetResponse?.body || '', title: 'Response Body' };
      case 'sql-statement':
        return { body: nodeData.targetRequest?.body || '', title: 'SQL Statement' };
      case 'operation-details':
        return {
          body: nodeData.targetRequest?.body || '',
          title: 'Operation Details',
        };
      default:
        return null;
    }
  };

  // Always render component, control display through animation
  // 始终渲染组件以确保动画连续性
  return (
    <>
      {/* CSS Animation Styles */}
      <style jsx>{`
        @keyframes expandIn {
          from {
            opacity: 0;
            transform: scale(0.95) translateY(-10px);
          }
          to {
            opacity: 1;
            transform: scale(1) translateY(0);
          }
        }

        @keyframes slideUpFadeIn {
          from {
            opacity: 0;
            transform: translateY(20px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }

        @keyframes expandOut {
          from {
            opacity: 1;
            transform: scale(1) translateY(0);
          }
          to {
            opacity: 0;
            transform: scale(0.95) translateY(-10px);
          }
        }

        @keyframes slideDownFadeOut {
          from {
            opacity: 1;
            transform: translateY(0);
          }
          to {
            opacity: 0;
            transform: translateY(20px);
          }
        }
      `}</style>
      {/* Background overlay - transparent */}
      <div
        className={`fixed inset-0 z-40 transition-all duration-150 ease-in-out ${
          isOpen && node
            ? 'bg-transparent'
            : 'pointer-events-none bg-transparent'
        }`}
        onClick={isOpen && node ? onClose : undefined}
      />

      {/* Drawer body */}
      <div
        className={`fixed top-0 right-0 z-50 flex h-full flex-col bg-white shadow-2xl transition-all duration-250 ease-out ${
          isOpen && node ? 'translate-x-0' : 'translate-x-full'
        }`}
        style={{
          width: `${drawerWidth}%`,
          boxShadow: isOpen && node ? '0 0 50px rgba(0, 0, 0, 0.3)' : 'none',
        }}
      >
        {/* Resize handle */}
        <div
          className={`group absolute top-0 left-0 h-full w-1 cursor-col-resize bg-gray-300 transition-colors duration-150 hover:bg-gray-400 ${
            isResizing ? 'bg-blue-400' : ''
          }`}
          onMouseDown={handleMouseDown}
          style={{
            zIndex: 10,
          }}
          title="拖拽调整宽度"
        >
          {/* Visual indicator dots */}
          <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 transform opacity-0 transition-opacity duration-150 group-hover:opacity-100">
            <div className="flex flex-col space-y-1">
              <div className="h-1 w-1 rounded-full bg-gray-500"></div>
              <div className="h-1 w-1 rounded-full bg-gray-500"></div>
              <div className="h-1 w-1 rounded-full bg-gray-500"></div>
            </div>
          </div>
        </div>
        {/* 始终渲染内容结构以保持动画连续性 */}
        <>
          {/* Drawer header */}
          <div className="flex items-center justify-between border-b border-gray-200 bg-gray-50 px-6 py-4">
            <div
              className={`flex-1 transition-all duration-400 ease-out ${
                isOpen ? 'translate-x-0 opacity-100' : 'translate-x-4 opacity-0'
              }`}
              style={{
                transitionDelay: isOpen ? '100ms' : '0ms',
              }}
            >
              <h2 className="text-lg font-semibold text-gray-900">
                Node Details
              </h2>
              <p className="mt-1 truncate text-sm text-gray-600">
                {nodeData?.operationName || 'No operation name'}
              </p>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setDrawerWidth(63)}
                className={`rounded-full p-2 text-gray-400 transition-all duration-150 ease-out hover:bg-gray-100 hover:text-gray-600 ${
                  isOpen ? 'scale-100 opacity-100' : 'scale-75 opacity-0'
                }`}
                style={{
                  transitionDelay: isOpen ? '150ms' : '0ms',
                }}
                title="重置宽度"
              >
                <RotateCcw className="h-4 w-4" />
              </button>
              <button
                onClick={onClose}
                className={`ml-2 rounded-full p-2 text-gray-400 transition-all duration-150 ease-out hover:bg-gray-100 hover:text-gray-600 ${
                  isOpen
                    ? 'scale-100 rotate-0 opacity-100'
                    : 'scale-75 rotate-90 opacity-0'
                }`}
                style={{
                  transitionDelay: isOpen ? '150ms' : '0ms',
                }}
              >
                <X className="h-5 w-5" />
              </button>
            </div>
          </div>

          {/* Drawer content */}
          <div
            id="drawer-content"
            className="relative min-h-0 flex-1 overflow-y-auto p-6"
          >
            <div
              className={`space-y-6 transition-all duration-250 ease-out ${
                isOpen ? 'translate-y-0 opacity-100' : 'translate-y-4 opacity-0'
              }`}
              style={{
                transitionDelay: isOpen ? '125ms' : '0ms',
              }}
            >
              {/* Basic Information */}
              <div
                className={`space-y-4 transition-all duration-400 ease-out ${
                  isOpen
                    ? 'translate-y-0 opacity-100'
                    : 'translate-y-2 opacity-0'
                }`}
                style={{
                  transitionDelay: isOpen ? '175ms' : '0ms',
                }}
              >
                <div className="flex items-center justify-between">
                  <h3 className="text-base font-medium text-gray-900">
                    Basic Information
                  </h3>
                  <div className="rounded-full bg-green-100 px-3 py-1 text-sm text-green-800">
                    Success
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div className="flex items-center gap-2">
                    {nodeData?.categoryType?.name?.toLowerCase() ===
                    'database' ? (
                      <Database className="h-4 w-4 text-blue-500" />
                    ) : nodeData?.categoryType?.name?.toLowerCase() ===
                      'redis' ? (
                      <Zap className="h-4 w-4 text-red-500" />
                    ) : nodeData?.categoryType?.name?.toLowerCase() ===
                      'server' ? (
                      <Globe className="h-4 w-4 text-purple-500" />
                    ) : nodeData?.categoryType?.name?.toLowerCase() ===
                      'user' ? (
                      <Settings className="h-4 w-4 text-indigo-500" />
                    ) : (
                      <Settings className="h-4 w-4 text-gray-500" />
                    )}
                    <div>
                      <span className="font-medium text-gray-700">Type:</span>
                      <span className="ml-2 text-gray-900">
                        {nodeData?.categoryType?.name === 'server'
                          ? 'Server'
                          : nodeData?.categoryType?.name === 'database'
                            ? 'Database'
                            : nodeData?.categoryType?.name === 'redis'
                              ? 'Redis Cache'
                              : nodeData?.categoryType?.name === 'user'
                                ? 'User Session'
                                : 'Unknown Type'}
                      </span>
                    </div>
                  </div>

                  <div className="flex items-center gap-2">
                    <Clock className="h-4 w-4 text-orange-500" />
                    <div>
                      <span className="font-medium text-gray-700">
                        Duration:
                      </span>
                      <span className="ml-2 text-gray-900">
                        {nodeData?.timeUsed || 0}ms
                      </span>
                    </div>
                  </div>

                  <div className="col-span-2">
                    <span className="font-medium text-gray-700">
                      Start Time:
                    </span>
                    <span className="ml-2 text-gray-900">
                      {new Date(nodeData?.creationTime || 0).toLocaleString(
                        'zh-CN'
                      )}
                    </span>
                  </div>

                  <div className="col-span-2">
                    <span className="font-medium text-gray-700">
                      {isUserNode ? 'Session ID:' : 'Transaction ID:'}
                    </span>
                    <span className="ml-2 font-mono text-sm text-gray-900">
                      {nodeData?.transactionId}
                    </span>
                  </div>

                  {/* UserNode特殊信息 */}
                  {isUserNode && nodeData?.sessionInfo && (
                    <>
                      <div className="col-span-2">
                        <span className="font-medium text-gray-700">Total Traces:</span>
                        <span className="ml-2 text-gray-900">
                          {nodeData.sessionInfo.totalTraces}
                        </span>
                      </div>
                      <div className="col-span-2">
                        <span className="font-medium text-gray-700">Total Spans:</span>
                        <span className="ml-2 text-gray-900">
                          {nodeData.sessionInfo.totalSpans}
                        </span>
                      </div>
                      {nodeData.sessionInfo.overallStartTime && nodeData.sessionInfo.overallEndTime && (
                        <>
                          <div className="col-span-2">
                            <span className="font-medium text-gray-700">Time Range:</span>
                            <span className="ml-2 text-gray-900">
                              {new Date(nodeData.sessionInfo.overallStartTime).toLocaleString('zh-CN')} - {new Date(nodeData.sessionInfo.overallEndTime).toLocaleString('zh-CN')}
                            </span>
                          </div>
                          <div className="col-span-2">
                            <span className="font-medium text-gray-700">Total Duration:</span>
                            <span className="ml-2 text-gray-900">
                              {nodeData.sessionInfo.totalDuration.toFixed(0)}ms
                            </span>
                          </div>
                        </>
                      )}
                    </>
                  )}

                  {/* Service Information */}
                  {nodeData?.targetRequest?.attributes?.ServiceName && (
                    <div className="col-span-2">
                      <span className="font-medium text-gray-700">
                        Service Name:
                      </span>
                      <span className="ml-2 font-mono text-sm text-gray-900">
                        {nodeData.targetRequest.attributes.ServiceName}
                      </span>
                    </div>
                  )}

                  {nodeData?.targetRequest?.attributes?.SpanType && (
                    <div className="col-span-2">
                      <span className="font-medium text-gray-700">
                        Span Type:
                      </span>
                      <span className="ml-2 font-mono text-sm text-gray-900">
                        {nodeData.targetRequest.attributes.SpanType}
                      </span>
                    </div>
                  )}

                  {nodeData?.targetRequest?.attributes?.TrafficDirection && (
                    <div className="col-span-2">
                      <span className="font-medium text-gray-700">
                        Traffic Direction:
                      </span>
                      <span className="ml-2 font-mono text-sm text-gray-900">
                        {nodeData.targetRequest.attributes.TrafficDirection}
                      </span>
                    </div>
                  )}

                  {/* Tags display */}
                  {node?.attributes?.tag && (
                    <div className="col-span-2">
                      <span className="font-medium text-gray-700">Tag:</span>
                      <div className="mt-2">
                        <span className="inline-flex items-center rounded-full bg-purple-100 px-3 py-1 text-sm font-medium text-purple-800">
                          {node.attributes.tag}
                        </span>
                      </div>
                    </div>
                  )}
                </div>
              </div>

              {/* UserNode specific information */}
              {isUserNode && (
                <UserNodeDetails nodeData={nodeData} isOpen={isOpen} />
              )}

              {/* Server specific information */}
              {nodeData?.categoryType?.name?.toLowerCase() === 'server' && (
                <ServerNodeDetails
                  nodeData={nodeData}
                  isOpen={isOpen}
                  copiedStates={copiedStates}
                  setCopiedStates={setCopiedStates}
                  copyJsonToClipboard={copyJsonToClipboard}
                  expandJsonView={expandJsonView}
                />
              )}


              {/* Database specific information */}
              {nodeData?.categoryType?.name?.toLowerCase() === 'database' && (
                <div
                  className={`space-y-4 transition-all duration-400 ease-out ${
                    isOpen
                      ? 'translate-y-0 opacity-100'
                      : 'translate-y-3 opacity-0'
                  }`}
                  style={{
                    transitionDelay: isOpen ? '250ms' : '0ms',
                  }}
                >
                  <h3 className="flex items-center gap-2 text-base font-medium text-gray-900">
                    <Database className="h-4 w-4 text-blue-500" />
                    Database Operation
                  </h3>
                  <div className="rounded-lg bg-gray-50 p-4">
                    <div className="space-y-3">
                      <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>
                          <span className="font-medium text-gray-700">
                            Operation Type:
                          </span>
                          <span className="ml-2 text-gray-900">SQL Query</span>
                        </div>
                        <div>
                          <span className="font-medium text-gray-700">
                            Database:
                          </span>
                          <span className="ml-2 text-gray-900">
                            {nodeData?.targetRequest?.attributes?.dbName ||
                              'Unknown'}
                          </span>
                        </div>
                      </div>

                      {nodeData?.targetRequest?.attributes?.TableName && (
                        <div>
                          <span className="font-medium text-gray-700">
                            Table Name:
                          </span>
                          <span className="ml-2 font-mono text-gray-900">
                            {nodeData?.targetRequest.attributes.TableName}
                          </span>
                        </div>
                      )}

                      {nodeData?.targetRequest?.body && (
                        <div>
                          <span className="font-medium text-gray-700">
                            SQL Statement:
                          </span>
                          <div
                            className="group relative mt-1 overflow-y-auto rounded bg-white"
                            style={{ maxHeight: '50vh' }}
                          >
                            {/* Action buttons */}
                            <div className="absolute top-2 right-2 z-10 flex gap-1">
                              <button
                                onClick={() => {
                                  try {
                                    const jsonData = JSON.parse(
                                      nodeData?.targetRequest.body
                                    );
                                    copyJsonToClipboard(
                                      jsonData,
                                      'sql-statement'
                                    );
                                  } catch (error) {
                                    // If not JSON, copy raw text
                                    navigator.clipboard.writeText(
                                      nodeData?.targetRequest.body || ''
                                    );
                                    setCopiedStates((prev) => ({
                                      ...prev,
                                      'sql-statement': true,
                                    }));
                                    setTimeout(() => {
                                      setCopiedStates((prev) => ({
                                        ...prev,
                                        'sql-statement': false,
                                      }));
                                    }, 2000);
                                  }
                                }}
                                className="rounded bg-transparent p-1.5 text-gray-600 transition-all duration-200 group-hover:bg-gray-100 hover:bg-gray-200 hover:text-gray-800"
                                title="Copy JSON to clipboard"
                              >
                                {copiedStates['sql-statement'] ? (
                                  <Check className="h-4 w-4 text-green-600" />
                                ) : (
                                  <Copy className="h-4 w-4" />
                                )}
                              </button>
                              <button
                                onClick={() => expandJsonView('sql-statement')}
                                className="rounded bg-transparent p-1.5 text-gray-600 transition-all duration-200 group-hover:bg-gray-100 hover:scale-105 hover:bg-gray-200 hover:text-gray-800 active:scale-95"
                                title="Expand JSON view"
                              >
                                <Maximize2 className="h-4 w-4" />
                              </button>
                            </div>
                            {(() => {
                              try {
                                // Try to parse as JSON and display with ReactJson
                                const jsonData = JSON.parse(
                                  nodeData?.targetRequest.body
                                );
                                return (
                                  <ReactJson
                                    src={jsonData}
                                    theme="rjv-default"
                                    collapsed={2}
                                    displayDataTypes={false}
                                    displayObjectSize={false}
                                    enableClipboard={false}
                                    name={null}
                                    style={{
                                      backgroundColor: 'transparent',
                                      fontFamily:
                                        'Monaco, Consolas, "Courier New", monospace',
                                      fontSize: '13px',
                                      padding: '12px',
                                    }}
                                  />
                                );
                              } catch (error) {
                                // If not JSON, display raw content
                                return (
                                  <pre className="p-3 font-mono text-sm whitespace-pre-wrap text-gray-900">
                                    {nodeData?.targetRequest.body}
                                  </pre>
                                );
                              }
                            })()}
                          </div>
                        </div>
                      )}

                      <div>
                        <span className="font-medium text-gray-700">
                          Execution Time:
                        </span>
                        <span className="ml-2 text-gray-900">
                          {nodeData?.timeUsed || 0}ms
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* Redis specific information */}
              {nodeData?.categoryType?.name?.toLowerCase() === 'redis' && (
                <div
                  className={`space-y-4 transition-all duration-400 ease-out ${
                    isOpen
                      ? 'translate-y-0 opacity-100'
                      : 'translate-y-3 opacity-0'
                  }`}
                  style={{
                    transitionDelay: isOpen ? '275ms' : '0ms',
                  }}
                >
                  <h3 className="flex items-center gap-2 text-base font-medium text-gray-900">
                    <Zap className="h-4 w-4 text-red-500" />
                    Redis Operation
                  </h3>
                  <div className="rounded-lg bg-gray-50 p-4">
                    <div className="space-y-3">
                      <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>
                          <span className="font-medium text-gray-700">
                            Operation Type:
                          </span>
                          <span className="ml-2 text-gray-900">
                            Cache Operation
                          </span>
                        </div>
                        <div>
                          <span className="font-medium text-gray-700">
                            Redis Command:
                          </span>
                          <span className="ml-2 font-mono text-gray-900">
                            {nodeData?.operationName || 'Unknown'}
                          </span>
                        </div>
                      </div>

                      {nodeData?.targetRequest?.attributes?.parameters && (
                        <div>
                          <span className="font-medium text-gray-700">
                            Cache Key:
                          </span>
                          <div className="mt-1 rounded bg-white p-2 font-mono text-sm text-gray-900">
                            {nodeData?.targetRequest.attributes.parameters}
                          </div>
                        </div>
                      )}

                      {nodeData?.targetRequest?.body && (
                        <div>
                          <span className="font-medium text-gray-700">
                            Operation Details:
                          </span>
                          <div
                            className="group relative mt-1 overflow-y-auto rounded bg-white"
                            style={{ maxHeight: '50vh' }}
                          >
                            {/* Action buttons */}
                            <div className="absolute top-2 right-2 z-10 flex gap-1">
                              <button
                                onClick={() => {
                                  try {
                                    const jsonData = JSON.parse(
                                      nodeData?.targetRequest.body
                                    );
                                    copyJsonToClipboard(
                                      jsonData,
                                      'operation-details'
                                    );
                                  } catch (error) {
                                    // If not JSON, copy raw text
                                    navigator.clipboard.writeText(
                                      nodeData?.targetRequest.body || ''
                                    );
                                    setCopiedStates((prev) => ({
                                      ...prev,
                                      'operation-details': true,
                                    }));
                                    setTimeout(() => {
                                      setCopiedStates((prev) => ({
                                        ...prev,
                                        'operation-details': false,
                                      }));
                                    }, 2000);
                                  }
                                }}
                                className="rounded bg-transparent p-1.5 text-gray-600 transition-all duration-200 group-hover:bg-gray-100 hover:bg-gray-200 hover:text-gray-800"
                                title="Copy JSON to clipboard"
                              >
                                {copiedStates['operation-details'] ? (
                                  <Check className="h-4 w-4 text-green-600" />
                                ) : (
                                  <Copy className="h-4 w-4" />
                                )}
                              </button>
                              <button
                                onClick={() =>
                                  expandJsonView('operation-details')
                                }
                                className="rounded bg-transparent p-1.5 text-gray-600 transition-all duration-200 group-hover:bg-gray-100 hover:scale-105 hover:bg-gray-200 hover:text-gray-800 active:scale-95"
                                title="Expand JSON view"
                              >
                                <Maximize2 className="h-4 w-4" />
                              </button>
                            </div>
                            {(() => {
                              try {
                                // Try to parse as JSON and display with ReactJson
                                const jsonData = JSON.parse(
                                  nodeData?.targetRequest.body
                                );
                                return (
                                  <ReactJson
                                    src={jsonData}
                                    theme="rjv-default"
                                    collapsed={2}
                                    displayDataTypes={false}
                                    displayObjectSize={false}
                                    enableClipboard={false}
                                    name={null}
                                    style={{
                                      backgroundColor: 'transparent',
                                      fontFamily:
                                        'Monaco, Consolas, "Courier New", monospace',
                                      fontSize: '13px',
                                      padding: '12px',
                                    }}
                                  />
                                );
                              } catch (error) {
                                // If not JSON, display raw content
                                return (
                                  <pre className="p-3 font-mono text-sm whitespace-pre-wrap text-gray-900">
                                    {nodeData?.targetRequest.body}
                                  </pre>
                                );
                              }
                            })()}
                          </div>
                        </div>
                      )}

                      <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>
                          <span className="font-medium text-gray-700">
                            Cache Hit:
                          </span>
                          <span className="ml-2 text-gray-900">
                            {nodeData?.targetRequest?.attributes?.CacheHit
                              ? 'Yes'
                              : 'No'}
                          </span>
                        </div>
                        <div>
                          <span className="font-medium text-gray-700">
                            Execution Time:
                          </span>
                          <span className="ml-2 text-gray-900">
                            {nodeData?.timeUsed || 0}ms
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Expanded JSON overlay */}
            {(expandedJsonKey || isClosing) && (
              <div
                className="absolute z-20 flex flex-col overflow-hidden rounded-lg bg-white shadow-lg"
                style={{
                  top: '8px',
                  right: '8px',
                  bottom: '8px',
                  left: '8px',
                  animation: isClosing
                    ? 'expandOut 0.3s ease-out'
                    : 'expandIn 0.3s ease-out',
                  transform: 'scale(1)',
                  opacity: 1,
                }}
              >
                {/* Expanded JSON header */}
                <div
                  className="flex items-center justify-between border-b border-gray-200 bg-gray-50 px-6 py-4"
                  style={{
                    animation: isClosing
                      ? 'slideDownFadeOut 0.25s ease-out both'
                      : 'slideUpFadeIn 0.4s ease-out 0.1s both',
                  }}
                >
                  <h3 className="text-lg font-semibold text-gray-900">
                    {
                      getExpandedJsonData(
                        expandedJsonKey || closingJsonKey || ''
                      )?.title
                    }
                  </h3>
                  <div className="group flex gap-2">
                    <button
                      onClick={() => {
                        const currentKey = expandedJsonKey || closingJsonKey;
                        const data = currentKey
                          ? getExpandedJsonData(currentKey)
                          : null;
                        if (data && currentKey) {
                          try {
                            const jsonData = JSON.parse(data.body);
                            copyJsonToClipboard(jsonData, currentKey);
                          } catch (error) {
                            navigator.clipboard.writeText(data.body || '');
                            setCopiedStates((prev) => ({
                              ...prev,
                              [currentKey]: true,
                            }));
                            setTimeout(() => {
                              setCopiedStates((prev) => ({
                                ...prev,
                                [currentKey]: false,
                              }));
                            }, 2000);
                          }
                        }
                      }}
                      className="rounded bg-transparent p-2 text-gray-600 transition-all duration-200 group-hover:bg-gray-100 hover:scale-105 hover:bg-gray-200 hover:text-gray-800 active:scale-95"
                      title="Copy JSON to clipboard"
                      style={{
                        animation: isClosing
                          ? 'slideDownFadeOut 0.2s ease-out both'
                          : 'slideUpFadeIn 0.4s ease-out 0.3s both',
                      }}
                    >
                      {copiedStates[expandedJsonKey || closingJsonKey || ''] ? (
                        <Check className="h-4 w-4 text-green-600" />
                      ) : (
                        <Copy className="h-4 w-4" />
                      )}
                    </button>
                    <button
                      onClick={minimizeJsonView}
                      className="rounded bg-transparent p-2 text-gray-600 transition-all duration-200 group-hover:bg-gray-100 hover:scale-105 hover:bg-gray-200 hover:text-gray-800 active:scale-95"
                      title="Minimize view"
                      style={{
                        animation: isClosing
                          ? 'slideDownFadeOut 0.15s ease-out both'
                          : 'slideUpFadeIn 0.4s ease-out 0.35s both',
                      }}
                    >
                      <Minimize2 className="h-4 w-4" />
                    </button>
                  </div>
                </div>

                {/* Expanded JSON content */}
                <div
                  className="group flex-1 overflow-y-auto bg-white p-6"
                  style={{
                    animation: isClosing
                      ? 'slideDownFadeOut 0.3s ease-out both'
                      : 'slideUpFadeIn 0.5s ease-out 0.2s both',
                  }}
                >
                  {(() => {
                    const currentKey = expandedJsonKey || closingJsonKey;
                    const data = currentKey
                      ? getExpandedJsonData(currentKey)
                      : null;
                    return data ? renderJsonContent(data.body, true) : null;
                  })()}
                </div>
              </div>
            )}
          </div>
        </>
      </div>
    </>
  );
};
