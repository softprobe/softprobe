'use client';

import React, { useState, useEffect } from 'react';
import { Search, Calendar, X } from 'lucide-react';
import type { TraceFilterData } from '../types';

// These UI components should be provided by the consuming application
// For TypeScript, we'll define minimal interfaces
interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'default' | 'outline' | 'destructive' | 'secondary' | 'ghost' | 'link';
  size?: 'default' | 'sm' | 'lg' | 'icon';
  children: React.ReactNode;
}

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {}

// The consuming app should provide these components
// For now, we'll use dynamic imports or expect them to be available
// In the OSS version, these will be copied from the UI components folder
interface TraceFilterPanelProps {
  onFilter: (filterData: TraceFilterData) => void;
  isLoading?: boolean;
  // Optional: allow consuming app to provide UI components
  Button?: React.ComponentType<ButtonProps>;
  Input?: React.ComponentType<InputProps>;
}

// Default implementations that will be replaced by consuming app's components
// These are just for type safety - the actual components should be imported
// by the consuming app and passed as props, or the consuming app should
// set up module aliases to provide them
const DefaultButton: React.ComponentType<ButtonProps> = ({ children, className, ...props }) => (
  <button className={className} {...props}>{children}</button>
);

const DefaultInput: React.ComponentType<InputProps> = ({ className, ...props }) => (
  <input className={className} {...props} />
);

export const TraceFilterPanel: React.FC<TraceFilterPanelProps> = ({
  onFilter,
  isLoading = false,
  Button: ButtonComponent = DefaultButton,
  Input: InputComponent = DefaultInput,
}) => {
  const [startDate, setStartDate] = useState<string>('');
  const [endDate, setEndDate] = useState<string>('');
  const [serviceName, setServiceName] = useState<string>('');
  const [httpStatusCodeInput, setHttpStatusCodeInput] = useState<string>('');
  const [sessionIdInput, setSessionIdInput] = useState<string>('');

  // 日期格式化为 datetime-local 字符串（精确到秒）
  const formatDateTimeLocal = (date: Date) => {
    const pad = (n: number) => String(n).padStart(2, '0');
    const y = date.getFullYear();
    const m = pad(date.getMonth() + 1);
    const d = pad(date.getDate());
    const hh = pad(date.getHours());
    const mm = pad(date.getMinutes());
    const ss = pad(date.getSeconds());
    return `${y}-${m}-${d}T${hh}:${mm}:${ss}`;
  };

  // 默认时间范围（一小时前到现在）应用到输入框
  const applyDefaultTimeRange = () => {
    const now = new Date();
    const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
    setStartDate(formatDateTimeLocal(oneHourAgo));
    setEndDate(formatDateTimeLocal(now));
  };

  // 设置默认日期范围为：结束=现在；开始=一小时前
  useEffect(() => {
    applyDefaultTimeRange();
  }, []);

  const handleReset = () => {
    applyDefaultTimeRange();
    setServiceName('');
    setHttpStatusCodeInput('');
    setSessionIdInput('');
  };

  const handleSearch = () => {
    const httpStatusCode = httpStatusCodeInput.trim()
      ? Number(httpStatusCodeInput.trim())
      : undefined;
  
    if (
      httpStatusCode !== undefined &&
      (Number.isNaN(httpStatusCode) || !Number.isInteger(httpStatusCode))
    ) {
      alert('HTTP Status Code must be an integer');
      return;
    }
  
    // 时间为必填：若为空则自动套用默认范围（一小时前到现在）
    let effectiveStart = startDate;
    let effectiveEnd = endDate;
    if (!effectiveStart || !effectiveEnd) {
      const now = new Date();
      const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
      effectiveStart = formatDateTimeLocal(oneHourAgo);
      effectiveEnd = formatDateTimeLocal(now);
      setStartDate(effectiveStart);
      setEndDate(effectiveEnd);
    }
  
    const filterData: TraceFilterData = {
      startTime: new Date(effectiveStart),
      endTime: new Date(effectiveEnd),
      serviceName: serviceName.trim() || undefined,
      httpStatusCode,
      sessionId: sessionIdInput.trim() || undefined,
    };
    onFilter(filterData);
  };

  return (
    <div className="mb-6 rounded-lg border border-gray-200 bg-white p-6">
      <div className="flex flex-wrap items-end gap-4">
        {/* Start Date */}
        <div className="min-w-[220px]">
          <label className="text-sm font-medium text-gray-700">Start Date</label>
          <div className="relative">
            <Calendar className="absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 transform text-gray-400" />
            <InputComponent
              type="datetime-local"
              step={1}
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="border-gray-300 bg-white pl-10 text-gray-900 focus:border-blue-500 focus:ring-blue-500"
              style={{ colorScheme: 'light' }}
            />
          </div>
        </div>

        {/* End Date */}
        <div className="min-w-[220px]">
          <label className="text-sm font-medium text-gray-700">End Date</label>
          <div className="relative">
            <Calendar className="absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 transform text-gray-400" />
            <InputComponent
              type="datetime-local"
              step={1}
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="border-gray-300 bg-white pl-10 text-gray-900 focus:border-blue-500 focus:ring-blue-500"
              style={{ colorScheme: 'light' }}
            />
          </div>
        </div>

        {/* Service Name */}
        <div className="min-w-[220px]">
          <label className="text-sm font-medium text-gray-700">Service Name</label>
          <div className="relative">
            <InputComponent
              type="text"
              value={serviceName}
              onChange={(e) => setServiceName(e.target.value)}
              placeholder="e.g. orders-service"
              className="pr-10"
            />
            {serviceName && (
              <button
                type="button"
                aria-label="Clear service name"
                onClick={() => setServiceName('')}
                className="absolute right-2 top-1/2 -translate-y-1/2 rounded-full bg-gray-200 hover:bg-gray-300 p-1"
              >
                <X className="h-3 w-3 text-gray-600" />
              </button>
            )}
          </div>
        </div>

        {/* HTTP Status Code */}
        <div className="min-w-[180px]">
          <label className="text-sm font-medium text-gray-700">HTTP Status Code</label>
          <div className="relative">
            <InputComponent
              type="text"
              value={httpStatusCodeInput}
              onChange={(e) => setHttpStatusCodeInput(e.target.value.replace(/[^0-9]/g, ''))}
              placeholder="e.g. 200"
              className="pr-10"
            />
            {httpStatusCodeInput && (
              <button
                type="button"
                aria-label="Clear HTTP status"
                onClick={() => setHttpStatusCodeInput('')}
                className="absolute right-2 top-1/2 -translate-y-1/2 rounded-full bg-gray-200 hover:bg-gray-300 p-1"
              >
                <X className="h-3 w-3 text-gray-600" />
              </button>
            )}
          </div>
        </div>

        {/* Session ID */}
        <div className="min-w-[240px]">
          <label className="text-sm font-medium text-gray-700">Session ID</label>
          <div className="relative">
            <InputComponent
              type="text"
              value={sessionIdInput}
              onChange={(e) => setSessionIdInput(e.target.value)}
              placeholder="Enter Session ID..."
              className="pr-10"
            />
            {sessionIdInput && (
              <button
                type="button"
                aria-label="Clear session id"
                onClick={() => setSessionIdInput('')}
                className="absolute right-2 top-1/2 -translate-y-1/2 rounded-full bg-gray-200 hover:bg-gray-300 p-1"
              >
                <X className="h-3 w-3 text-gray-600" />
              </button>
            )}
          </div>
        </div>

        {/* Action Buttons */}
        <div className="flex items-end gap-2">
          <ButtonComponent
            onClick={handleSearch}
            className="w-36 bg-blue-600 px-6 text-white hover:bg-blue-700"
          >
            <Search className="mr-2 h-4 w-4" />
            {isLoading ? 'Searching...' : 'Search'}
          </ButtonComponent>
          <ButtonComponent
            variant="outline"
            onClick={handleReset}
            className="w-36 px-6"
          >
            Reset
          </ButtonComponent>
        </div>
      </div>
    </div>
  );
};

