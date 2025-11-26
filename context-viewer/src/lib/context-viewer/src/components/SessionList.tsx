'use client';

import React from 'react';
import type { UserSession } from '../types';

type SessionListProps = {
  sessions: UserSession[];
  selectedSessionId?: string;
  onSessionClick: (session: UserSession) => void;
};

export function SessionList({
  sessions,
  selectedSessionId,
  onSessionClick,
}: SessionListProps) {
  return (
    <div className="space-y-0">
      {sessions.map((session, index) => {
        const isSelected = selectedSessionId === session.sessionId;
        return (
          <div key={session.sessionId}>
            <div
              className={`cursor-pointer px-4 py-3 transition-colors border-r-2 ${
                isSelected
                  ? 'text-primary bg-slate-100/80'
                  : 'hover:bg-slate-100/30 border-transparent'
              }`}
              onClick={() => onSessionClick(session)}
            >
              <div className="flex flex-col space-y-1">
                <div className={
                  "truncate text-sm font-medium"
                  + (isSelected ? ' text-primary' : ' text-gray-900')
                }>
                  {session.sessionId}
                </div>
                <div className={
                  "text-xs"
                  + (isSelected ? ' text-primary/70' : ' text-gray-400')
                }>
                  {new Date(session.createTime).toLocaleString()}
                </div>
              </div>
            </div>
            {index < sessions.length - 1 && (
              <div className="px-4 border-t" />
            )}
          </div>
        );
      })}
    </div>
  );
}

