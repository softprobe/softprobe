'use client';

import React, { useState } from 'react';
import {
  ArrowRight,
  Code,
  Copy,
  Check,
  Maximize2,
} from 'lucide-react';
import dynamic from 'next/dynamic';

// 动态导入 react-json-view 以避免 SSR 问题
const ReactJson = dynamic(() => import('react-json-view'), {
  ssr: false,
  loading: () => (
    <div className="p-3 text-sm text-gray-500">Loading JSON viewer...</div>
  ),
});

interface ServerNodeDetailsProps {
  nodeData: any;
  isOpen: boolean;
  copiedStates: { [key: string]: boolean };
  setCopiedStates: (fn: (prev: any) => any) => void;
  copyJsonToClipboard: (data: any, key: string) => Promise<void>;
  expandJsonView: (key: string) => void;
}

export const ServerNodeDetails: React.FC<ServerNodeDetailsProps> = ({
  nodeData,
  isOpen,
  copiedStates,
  setCopiedStates,
  copyJsonToClipboard,
  expandJsonView,
}) => {
  if (!nodeData?.targetRequest && !nodeData?.targetResponse) {
    return null;
  }

  return (
    <div
      className={`space-y-4 transition-all duration-400 ease-out ${
        isOpen
          ? 'translate-y-0 opacity-100'
          : 'translate-y-3 opacity-0'
      }`}
      style={{
        transitionDelay: isOpen ? '225ms' : '0ms',
      }}
    >
      {/* HTTP request information */}
      {nodeData?.targetRequest && (
        <div className="space-y-4">
          <h3 className="flex items-center gap-2 text-base font-medium text-gray-900">
            <ArrowRight className="h-4 w-4 text-blue-500" />
            HTTP Request
          </h3>
          <div className="rounded-lg bg-gray-50 p-4">
            <div className="space-y-3">
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="font-medium text-gray-700">
                    Request Method:
                  </span>
                  <span className="ml-2 font-mono text-blue-600">
                    {nodeData?.targetRequest.attributes
                      ?.HttpMethod || 'N/A'}
                  </span>
                </div>
                <div>
                  <span className="font-medium text-gray-700">
                    Status:
                  </span>
                  <span className="ml-2 text-green-600">
                    Success
                  </span>
                </div>
              </div>

              {/* Additional HTTP Information */}
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="font-medium text-gray-700">
                    Scheme:
                  </span>
                  <span className="ml-2 font-mono text-gray-900">
                    {nodeData?.targetRequest.attributes
                      ?.UrlScheme || 'N/A'}
                  </span>
                </div>
                <div>
                  <span className="font-medium text-gray-700">
                    Host:
                  </span>
                  <span className="ml-2 font-mono text-gray-900">
                    {nodeData?.targetRequest.attributes?.UrlHost ||
                      'N/A'}
                  </span>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="font-medium text-gray-700">
                    Request ID:
                  </span>
                  <span className="ml-2 font-mono text-sm text-gray-900">
                    {nodeData?.targetRequest.attributes
                      ?.RequestId || 'N/A'}
                  </span>
                </div>
                <div>
                  <span className="font-medium text-gray-700">
                    Session ID:
                  </span>
                  <span className="ml-2 font-mono text-sm text-gray-900">
                    {nodeData?.targetRequest.attributes
                      ?.SessionId || 'N/A'}
                  </span>
                </div>
              </div>

              <div>
                <span className="font-medium text-gray-700">
                  Request Path:
                </span>
                <div className="mt-1 rounded bg-white p-2 font-mono text-sm text-gray-900">
                  {nodeData?.targetRequest.attributes
                    ?.RequestPath || 'N/A'}
                </div>
              </div>

              {nodeData?.targetRequest.attributes?.Headers && (
                <div>
                  <span className="font-medium text-gray-700">
                    Request Headers:
                  </span>
                  <div className="mt-1 max-h-32 overflow-y-auto rounded bg-white p-2 text-sm">
                    <div className="flex justify-between">
                      <span className="font-mono text-blue-600">
                        User-Agent:
                      </span>
                      <span className="ml-2 text-gray-900">
                        {nodeData?.targetRequest.attributes.Headers}
                      </span>
                    </div>
                  </div>
                </div>
              )}

              {nodeData?.targetRequest.body && 
               nodeData?.targetRequest.attributes?.HttpMethod?.toUpperCase() !== 'GET' && (
                <div>
                  <span className="font-medium text-gray-700">
                    Request Body:
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
                              'request-body'
                            );
                          } catch (error) {
                            // If not JSON, copy raw text
                            navigator.clipboard.writeText(
                              nodeData?.targetRequest.body || ''
                            );
                            setCopiedStates((prev) => ({
                              ...prev,
                              'request-body': true,
                            }));
                            setTimeout(() => {
                              setCopiedStates((prev) => ({
                                ...prev,
                                'request-body': false,
                              }));
                            }, 2000);
                          }
                        }}
                        className="rounded bg-transparent p-1.5 text-gray-600 transition-all duration-200 group-hover:bg-gray-100 hover:bg-gray-200 hover:text-gray-800"
                        title="Copy JSON to clipboard"
                      >
                        {copiedStates['request-body'] ? (
                          <Check className="h-4 w-4 text-green-600" />
                        ) : (
                          <Copy className="h-4 w-4" />
                        )}
                      </button>
                      <button
                        onClick={() =>
                          expandJsonView('request-body')
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
                          <pre className="p-3 text-sm whitespace-pre-wrap text-gray-900">
                            {nodeData?.targetRequest.body}
                          </pre>
                        );
                      }
                    })()}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* HTTP response information */}
      {nodeData?.targetResponse && (
        <div className="space-y-4">
          <h3 className="flex items-center gap-2 text-base font-medium text-gray-900">
            <Code className="h-4 w-4 text-green-500" />
            HTTP Response
          </h3>
          <div className="rounded-lg bg-gray-50 p-4">
            <div className="space-y-3">
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="font-medium text-gray-700">
                    Status Code:
                  </span>
                  <span className="ml-2 font-mono text-green-600">
                    {nodeData?.targetResponse?.headers?.status ||
                      '200 OK'}
                  </span>
                </div>
                <div>
                  <span className="font-medium text-gray-700">
                    Content Type:
                  </span>
                  <span className="ml-2 font-mono text-gray-900">
                    {nodeData?.targetResponse?.headers
                      ?.contentType || 'N/A'}
                  </span>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="font-medium text-gray-700">
                    Date:
                  </span>
                  <span className="ml-2 font-mono text-sm text-gray-900">
                    {nodeData?.targetResponse?.headers?.date ||
                      'N/A'}
                  </span>
                </div>
                <div>
                  <span className="font-medium text-gray-700">
                    Upstream Time:
                  </span>
                  <span className="ml-2 font-mono text-sm text-gray-900">
                    {nodeData?.targetResponse?.headers
                      ?.upstreamServiceTime || 'N/A'}
                  </span>
                </div>
              </div>

              {nodeData?.targetResponse.body && (
                <div>
                  <span className="font-medium text-gray-700">
                    Response Body:
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
                              nodeData?.targetResponse.body
                            );
                            copyJsonToClipboard(
                              jsonData,
                              'response-body'
                            );
                          } catch (error) {
                            // If not JSON, copy raw text
                            navigator.clipboard.writeText(
                              nodeData?.targetResponse.body || ''
                            );
                            setCopiedStates((prev) => ({
                              ...prev,
                              'response-body': true,
                            }));
                            setTimeout(() => {
                              setCopiedStates((prev) => ({
                                ...prev,
                                'response-body': false,
                              }));
                            }, 2000);
                          }
                        }}
                        className="rounded bg-transparent p-1.5 text-gray-600 transition-all duration-200 group-hover:bg-gray-100 hover:bg-gray-200 hover:text-gray-800"
                        title="Copy JSON to clipboard"
                      >
                        {copiedStates['response-body'] ? (
                          <Check className="h-4 w-4 text-green-600" />
                        ) : (
                          <Copy className="h-4 w-4" />
                        )}
                      </button>
                      <button
                        onClick={() =>
                          expandJsonView('response-body')
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
                          nodeData?.targetResponse.body
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
                          <pre className="p-3 text-sm whitespace-pre-wrap text-gray-900">
                            {nodeData?.targetResponse.body}
                          </pre>
                        );
                      }
                    })()}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
