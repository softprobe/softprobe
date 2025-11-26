'use client';

import React from 'react';
import { Settings } from 'lucide-react';

interface UserNodeDetailsProps {
  nodeData: any;
  isOpen: boolean;
}

export const UserNodeDetails: React.FC<UserNodeDetailsProps> = ({
  nodeData,
  isOpen,
}) => {
  if (!nodeData?.sessionInfo || !nodeData?.traces) {
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
      {/* Session Traces列表 */}
      {nodeData.traces && nodeData.traces.length > 0 && (
        <div className="space-y-4">
          <h3 className="flex items-center gap-2 text-base font-medium text-gray-900">
            <Settings className="h-5 w-5 text-indigo-500" />
            Session Traces
          </h3>
          <div className="space-y-3">
            {nodeData.traces.map((trace: any, index: number) => (
              <div key={index} className="rounded-lg border border-gray-200 bg-gray-50 p-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div className="h-2 w-2 rounded-full bg-indigo-500"></div>
                    <span className="font-medium text-gray-900">
                      Context {index + 1}
                    </span>
                  </div>
                  <div className="text-sm text-gray-600">
                    {trace.spans?.length || 0} spans
                  </div>
                </div>
                {trace.spans && trace.spans.length > 0 && (
                  <div className="mt-2 space-y-1">
                    <div className="text-sm text-gray-600">
                      <span className="font-medium">Root:</span> {trace.spans[0].name}
                    </div>
                    <div className="text-sm text-gray-600">
                      <span className="font-medium">Duration:</span> {trace.spans[0].duration}ms
                    </div>
                    <div className="text-sm text-gray-600">
                      <span className="font-medium">Service:</span> {trace.spans[0].serviceName}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};
