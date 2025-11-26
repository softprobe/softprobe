'use client';

import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'next/navigation';
import { toast } from 'sonner';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Tabs } from '@/components/ui/tabs';

import { ReactFlowTraceView, SessionList, querySessions, fetchTraceDataNew } from '@/lib/context-viewer';
import { TraceFilterPanel as BaseTraceFilterPanel } from '@/lib/context-viewer';
import type { UserSession, TraceViewResponse, TraceFilterData } from '@/lib/context-viewer';

// Wrap TraceFilterPanel to provide UI components
const TraceFilterPanel = (props: Parameters<typeof BaseTraceFilterPanel>[0]) => (
  <BaseTraceFilterPanel {...props} Button={Button} Input={Input} />
);

// Tab data structure
interface SessionTab {
  id: string; // sessionId
  session: UserSession;
  traceData: TraceViewResponse | null;
  isLoading: boolean;
  createdAt: number;
  hasUserAdjustedView: boolean;
  hasInitialFitView: boolean;
}

const MAX_TABS = 20;

const TraceViewPage = () => {
  const [isLoadingSessions, setIsLoadingSessions] = useState(false);
  const [sessions, setSessions] = useState<UserSession[]>([]);
  const [tabs, setTabs] = useState<SessionTab[]>([]);
  const [activeTabId, setActiveTabId] = useState<string | null>(null);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [pageSize] = useState(10);
  const [totalCount, setTotalCount] = useState(0);
  const [hasAutoSearched, setHasAutoSearched] = useState(false);
  const [currentFilter, setCurrentFilter] = useState<TraceFilterData | undefined>(undefined);
  const [pages, setPages] = useState<Array<{ items: UserSession[]; cursorStart: string | null; nextCursor: string | null }>>([]);
  const [cursorHistory, setCursorHistory] = useState<(string | null)[]>([]);
  const [activePageIndex, setActivePageIndex] = useState<number>(0);

  // Initialize default filter: end=now, start=1 hour ago
  useEffect(() => {
    const now = new Date();
    const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
    setCurrentFilter({ startTime: oneHourAgo, endTime: now });
  }, []);

  // Auto search on initial load
  useEffect(() => {
    if (!hasAutoSearched && currentFilter) {
      setHasAutoSearched(true);
      handleFilter(currentFilter, null);
    }
  }, [currentFilter, hasAutoSearched]);

  // Handle filter
  const handleFilter = async (filterData?: TraceFilterData, cursor: string | null = null) => {
    if (filterData) {
      setCurrentFilter(filterData);
    }
    const effectiveFilter = filterData ?? currentFilter;

    setIsLoadingSessions(true);

    // If sessionId is provided, load it directly
    if (filterData?.sessionId && filterData.sessionId.trim()) {
      try {
        const sid = filterData.sessionId.trim();
        const tempSession: UserSession = {
          sessionId: sid,
          appId: filterData?.appId || '',
          createTime: Date.now(),
        };
        setPages([{ items: [tempSession], cursorStart: null, nextCursor: null }]);
        setCursorHistory([null]);
        setActivePageIndex(0);
        setSessions([tempSession]);
        setTotalCount(1);
        setNextCursor(null);
        setHasMore(false);
        await openOrActivateTab(tempSession);
      } finally {
        setIsLoadingSessions(false);
      }
      return;
    }

    try {
      // Check if page is cached
      if (cursor !== null) {
        const existingIndex = cursorHistory.findIndex((c) => c === cursor);
        if (existingIndex >= 0) {
          setActivePageIndex(existingIndex);
          setSessions(pages[existingIndex]?.items ?? []);
          setIsLoadingSessions(false);
          return;
        }
      }

      const response = await querySessions(cursor, pageSize, effectiveFilter);

      if (cursor === null) {
        const firstPage = { items: response.sessions, cursorStart: null, nextCursor: response.nextCursor };
        setPages([firstPage]);
        setCursorHistory([null]);
        setActivePageIndex(0);
        setSessions(response.sessions);
      } else {
        const newPage = { items: response.sessions, cursorStart: cursor, nextCursor: response.nextCursor };
        setPages((prev) => [...prev, newPage]);
        setCursorHistory((prev) => [...prev, cursor]);
        setActivePageIndex((prev) => prev + 1);
        setSessions(response.sessions);
      }

      setNextCursor(response.nextCursor);
      setHasMore(response.hasMore);
      setTotalCount(response.totalCount);
      setIsLoadingSessions(false);

      if (cursor === null && response.sessions?.length > 0) {
        const firstSession = response.sessions[0];
        await openOrActivateTab(firstSession);
      } else if (response.sessions.length === 0) {
        toast.error('No results found. Please try adjusting your search criteria.');
      }
    } catch (error) {
      console.error('Failed to filter sessions:', error);
      toast.error('Failed to load sessions');
      setIsLoadingSessions(false);
    }
  };

  // Load next page
  const handleNextPage = () => {
    if (activePageIndex < pages.length - 1) {
      const newIndex = activePageIndex + 1;
      setActivePageIndex(newIndex);
      setSessions(pages[newIndex]?.items ?? []);
      return;
    }
    const nc = pages[activePageIndex]?.nextCursor || nextCursor;
    if (nc) {
      handleFilter(undefined, nc);
    }
  };

  // Load previous page
  const handlePrevPage = () => {
    if (activePageIndex > 0) {
      const newIndex = activePageIndex - 1;
      setActivePageIndex(newIndex);
      setSessions(pages[newIndex]?.items ?? []);
    }
  };

  // Get display pages for pagination
  type PageToken = number | 'ellipsis';
  const getDisplayPages = (activeIndex: number, totalPages: number | null): PageToken[] => {
    if (!totalPages || totalPages <= 1) return [1];
    const ai = activeIndex + 1;
    const tp = totalPages;
    const result: PageToken[] = [];

    if (tp <= 7) {
      for (let i = 1; i <= tp; i++) result.push(i);
      return result;
    }

    result.push(1);

    if (ai <= 4) {
      result.push(2, 3, 4, 5);
      result.push('ellipsis');
      result.push(tp);
      return result;
    }

    if (ai >= tp - 3) {
      result.push('ellipsis');
      result.push(tp - 4, tp - 3, tp - 2, tp - 1, tp);
      return result;
    }

    result.push('ellipsis');
    result.push(ai - 1, ai, ai + 1);
    result.push('ellipsis');
    result.push(tp);
    return result;
  };

  // Go to page
  const gotoPage = async (targetIndex: number) => {
    if (targetIndex < 0) return;

    if (targetIndex < pages.length) {
      setActivePageIndex(targetIndex);
      setSessions(pages[targetIndex]?.items ?? []);
      return;
    }

    const newPages = [...pages];
    const newCursorHistory = [...cursorHistory];
    let cursor = newPages.length === 0 ? null : (newPages[newPages.length - 1].nextCursor || nextCursor);
    let lastTotalCount = totalCount;

    setIsLoadingSessions(true);
    try {
      while (newPages.length <= targetIndex && cursor) {
        const response = await querySessions(cursor, pageSize, currentFilter);
        const nextPage = { items: response.sessions, cursorStart: cursor, nextCursor: response.nextCursor };
        newPages.push(nextPage);
        newCursorHistory.push(cursor);
        cursor = response.nextCursor;
        lastTotalCount = response.totalCount ?? lastTotalCount;
        if (!response.sessions || response.sessions.length === 0) break;
      }
      setPages(newPages);
      setCursorHistory(newCursorHistory);
      setNextCursor(cursor);
      setHasMore(!!cursor);
      if (lastTotalCount !== totalCount) setTotalCount(lastTotalCount);

      const finalIndex = Math.min(targetIndex, newPages.length - 1);
      setActivePageIndex(finalIndex);
      setSessions(newPages[finalIndex]?.items ?? []);
    } catch (error) {
      console.error('Failed to load page:', error);
      toast.error('Failed to load page');
    } finally {
      setIsLoadingSessions(false);
    }
  };

  // Open or activate tab
  const openOrActivateTab = async (session: UserSession) => {
    const existingTab = tabs.find(tab => tab.id === session.sessionId);
    if (existingTab) {
      setActiveTabId(session.sessionId);
      return;
    }

    if (tabs.length >= MAX_TABS) {
      const oldestTab = tabs.reduce((oldest, current) => 
        current.createdAt < oldest.createdAt ? current : oldest
      );
      closeTab(oldestTab.id);
      toast.warning(`Maximum ${MAX_TABS} tabs reached. The oldest tab has been closed.`);
    }

    const newTab: SessionTab = {
      id: session.sessionId,
      session,
      traceData: null,
      isLoading: true,
      createdAt: Date.now(),
      hasUserAdjustedView: false,
      hasInitialFitView: false,
    };

    setTabs(prev => [...prev, newTab]);
    setActiveTabId(session.sessionId);

    try {
      const traceData = await fetchTraceDataNew(session.sessionId);
      setTabs(prev => prev.map(tab => 
        tab.id === session.sessionId 
          ? { ...tab, traceData, isLoading: false }
          : tab
      ));
      if (traceData) {
        toast.success('Context data loaded successfully');
      } else {
        toast.error('No trace data found for this Session ID');
      }
    } catch (error) {
      console.error('Failed to load trace data:', error);
      setTabs(prev => prev.map(tab => 
        tab.id === session.sessionId 
          ? { ...tab, isLoading: false }
          : tab
      ));
      toast.error('Failed to load trace data');
    }
  };

  // Handle viewport change
  const handleViewportChange = (tabId: string) => {
    setTabs(prev => prev.map(tab => 
      tab.id === tabId 
        ? { ...tab, hasUserAdjustedView: true }
        : tab
    ));
  };

  // Handle initial fit view complete
  const handleInitialFitViewComplete = (tabId: string) => {
    setTabs(prev => prev.map(tab => 
      tab.id === tabId 
        ? { ...tab, hasInitialFitView: true }
        : tab
    ));
  };

  // Close tab
  const closeTab = (tabId: string) => {
    setTabs(prev => {
      const newTabs = prev.filter(tab => tab.id !== tabId);
      if (activeTabId === tabId) {
        if (newTabs.length > 0) {
          setActiveTabId(newTabs[newTabs.length - 1].id);
        } else {
          setActiveTabId(null);
        }
      }
      return newTabs;
    });
  };

  // Handle session click
  const onSessionClick = (session: UserSession) => {
    openOrActivateTab(session);
  };

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto">
        <h1 className="text-2xl font-bold text-gray-900 mb-6">Context Viewer</h1>

        {/* Filter Panel */}
        <Card className="mb-6">
          <CardHeader>
            <CardTitle>Filter Sessions</CardTitle>
          </CardHeader>
          <CardContent>
            <TraceFilterPanel onFilter={handleFilter} isLoading={isLoadingSessions} />
          </CardContent>
        </Card>

        <div className="flex flex-col lg:flex-row gap-4">
          {/* Left Panel */}
          <div className="w-full lg:w-[375px] flex flex-col gap-4 flex-shrink-0">
            <div className="flex flex-col gap-3">
              <h2 className="text-base font-semibold text-gray-900">Session List</h2>
              
              <Card>
                <CardContent className="p-0">
                  {isLoadingSessions ? (
                    <div className="flex items-center justify-center p-6 min-h-[200px]">
                      <div className="text-center">
                        <div className="mx-auto mb-4 h-8 w-8 animate-spin rounded-full border-4 border-gray-300 border-t-blue-600"></div>
                        <h3 className="mb-2 text-lg font-medium text-gray-900">Loading Sessions...</h3>
                        <p className="text-sm text-gray-500">Please wait while we fetch the session list</p>
                      </div>
                    </div>
                  ) : sessions.length > 0 ? (
                    <div className="max-h-[650px] overflow-y-auto">
                      <SessionList
                        sessions={sessions}
                        selectedSessionId={activeTabId ?? undefined}
                        onSessionClick={onSessionClick}
                      />
                    </div>
                  ) : (
                    <div className="flex items-center justify-center p-6 min-h-[200px]">
                      <div className="text-center">
                        <h3 className="mb-2 text-lg font-medium text-gray-900">No Sessions</h3>
                        <p className="text-sm text-gray-500">Use the search controls above to find sessions</p>
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* Pagination */}
              <div className="flex items-center justify-between gap-2 text-sm px-1">
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handlePrevPage}
                    disabled={isLoadingSessions || activePageIndex === 0}
                    className="h-7"
                  >
                    {'<'}
                  </Button>
                  {getDisplayPages(activePageIndex, totalCount > 0 ? Math.ceil(totalCount / pageSize) : null).map((token, idx) => (
                    token === 'ellipsis' ? (
                      <span key={`ellipsis-${idx}`} className="px-2 text-gray-500">…</span>
                    ) : (
                      <Button
                        key={`pg-${token}`}
                        variant={activePageIndex + 1 === token ? 'default' : 'outline'}
                        size="sm"
                        className={activePageIndex + 1 === token ? 'h-7 bg-purple-200 text-purple-700' : 'h-7'}
                        disabled={isLoadingSessions}
                        onClick={() => gotoPage(token - 1)}
                      >
                        {token}
                      </Button>
                    )
                  ))}
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleNextPage}
                    disabled={isLoadingSessions || (!pages[activePageIndex]?.nextCursor && activePageIndex >= pages.length - 1)}
                    className="h-7"
                  >
                    {'>'}
                  </Button>
                </div>
              </div>
            </div>
          </div>

          {/* Right Panel - Context View */}
          <div className="flex-1 min-w-0">
            <h2 className="text-base font-semibold text-gray-900 mb-3">Context Visualization</h2>
            <Card className="h-[calc(100vh-6rem)]">
              {tabs.length === 0 ? (
                <>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base">No Session Selected</CardTitle>
                    <p className="text-sm text-gray-500">
                      Select a session from the left panel to view its trace data
                    </p>
                  </CardHeader>
                  <CardContent className="h-[calc(100%-5rem)] p-0">
                    <div className="flex h-full items-center justify-center p-6">
                      <div className="py-8 text-center">
                        <h3 className="mb-2 text-lg font-medium text-gray-900">No Context Data</h3>
                        <p className="text-sm text-gray-500">Select a session from the left panel to view its trace data</p>
                      </div>
                    </div>
                  </CardContent>
                </>
              ) : (
                <div className="h-full flex flex-col">
                  <Tabs
                    items={tabs.map(tab => ({
                      id: tab.id,
                      label: tab.session.sessionId,
                      closable: true,
                      onClose: () => closeTab(tab.id),
                      content: (
                        <div className="h-full p-6">
                          {tab.isLoading ? (
                            <div className="flex h-full items-center justify-center">
                              <div className="text-center">
                                <div className="mx-auto mb-4 h-8 w-8 animate-spin rounded-full border-4 border-gray-300 border-t-blue-600"></div>
                                <h3 className="mb-2 text-lg font-medium text-gray-900">Loading Context Data...</h3>
                                <p className="text-sm text-gray-500">Please wait while we fetch the trace visualization</p>
                              </div>
                            </div>
                          ) : tab.traceData ? (
                            <div className="h-full w-full">
                              <ReactFlowTraceView
                                traceData={tab.traceData}
                                appId={tab.session.appId || ''}
                                transactionId={tab.session.sessionId}
                                spanIdsToFocus={tab.session.spanIds}
                                autoFitView={!tab.hasUserAdjustedView && !tab.hasInitialFitView && tab.id === activeTabId}
                                onViewportChange={() => handleViewportChange(tab.id)}
                                onInitialFitViewComplete={() => handleInitialFitViewComplete(tab.id)}
                              />
                            </div>
                          ) : (
                            <div className="flex h-full items-center justify-center">
                              <div className="text-center">
                                <h3 className="mb-2 text-lg font-medium text-gray-900">No Context Data</h3>
                                <p className="text-sm text-gray-500">No trace data found for this session</p>
                              </div>
                            </div>
                          )}
                        </div>
                      ),
                    }))}
                    activeId={activeTabId}
                    onTabChange={setActiveTabId}
                    className="h-full"
                  />
                </div>
              )}
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TraceViewPage;

