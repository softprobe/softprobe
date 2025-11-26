'use client';

import React, { useCallback, useMemo, useState, useEffect, useRef } from 'react';
import {
  ReactFlow,
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  Background,
  Controls,
  MiniMap,
  useReactFlow,
  type Connection,
  type Edge,
  type EdgeChange,
  type Node,
  type NodeChange,
  type Viewport,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import dagre from '@dagrejs/dagre';
import { TraceDetailDrawer } from './TraceDetailDrawer';
import type { NodeProps } from '@xyflow/react';
import {
  Handle,
  Position,
  MarkerType,
} from '@xyflow/react';

interface SpanAttribute {
  [key: string]: any;
}

// 从 types 导入类型
import type {
  Span,
  TraceViewResponse,
  Context,
} from '../types';

interface ReactFlowTraceViewProps {
  traceData: TraceViewResponse;
  appId: string;
  transactionId: string;
  spanIdsToFocus?: string[];
  autoFitView?: boolean; // 是否自动fitView（首次加载时）
  onViewportChange?: () => void; // 用户手动调整视图时的回调
  onInitialFitViewComplete?: () => void; // 初始fitView完成时的回调
}

interface TraceViewContentProps extends ReactFlowTraceViewProps {
  isZoomEnabled: boolean;
}

type TraceNodeData = {
  id: string;
  label: string;
  subtitle: string;
  type: string;
  status: 'success' | 'error';
  duration: number;
  method?: string;
  url?: string;
  operation?: string;
  serviceName?: string;
  startTime?: string;
  endTime?: string;
  attributes?: SpanAttribute;
  nodeData?: Span;
  isRoot?: boolean;
  isLeaf?: boolean;
  traceData?: any; // 添加traceData字段用于user节点
  themeColor?: string; // 添加主题颜色字段
  flashSignal?: number; // 闪烁驱动信号
};

// 视图状态接口
interface ViewState {
  viewport: Viewport;
  nodePositions: Record<string, { x: number; y: number }>;
}

// 使用ReactFlow hooks的内部组件
const ReactFlowInteractionHandler: React.FC<{
  selectedNode: Span | null;
  setSelectedNode: (node: Span | null) => void;
  setIsDrawerOpen: (open: boolean) => void;
  savedViewState: ViewState | null;
  setSavedViewState: (state: ViewState | null) => void;
  isAnimating: boolean;
  setIsAnimating: (animating: boolean) => void;
  isZoomEnabled: boolean;
}> = ({
  selectedNode,
  setSelectedNode,
  setIsDrawerOpen,
  savedViewState,
  setSavedViewState,
  isAnimating,
  setIsAnimating,
  isZoomEnabled,
}) => {
  const reactFlowInstance = useReactFlow();

  // 节点点击处理函数
  const handleNodeClick = useCallback(
    async (_: any, node: Node<TraceNodeData>) => {
      if (isAnimating) return; // 防止重复点击

      const span = node.data?.nodeData ?? null;
      setIsAnimating(true);

      // 只有在缩放功能启用时才执行缩放逻辑
      if (isZoomEnabled) {
        // 保存当前视图状态
        const currentViewport = reactFlowInstance.getViewport();
        const currentNodes = reactFlowInstance.getNodes();
        const nodePositions: Record<string, { x: number; y: number }> = {};
        currentNodes.forEach((n) => {
          nodePositions[n.id] = { x: n.position.x, y: n.position.y };
        });

        setSavedViewState({
          viewport: currentViewport,
          nodePositions,
        });

        // 计算屏幕中心位置
        const container = reactFlowInstance.getViewport();

        // 确保在客户端环境中执行 DOM 操作
        if (typeof window === 'undefined') return;

        const containerRect = document
          .querySelector('.react-flow__viewport')
          ?.getBoundingClientRect();

        if (containerRect) {
          const centerX = containerRect.width / 2;
          const centerY = containerRect.height / 2;

          // 计算节点的实际屏幕位置
          const nodeScreenX = node.position.x * container.zoom + container.x;
          const nodeScreenY = node.position.y * container.zoom + container.y;

          // 计算需要移动的距离
          const deltaX = centerX - nodeScreenX;
          const deltaY = centerY - nodeScreenY;

          // 计算新的视口位置
          const newX = container.x + deltaX;
          const newY = container.y + deltaY;

          // 计算合适的缩放级别，让节点高度约为屏幕的1/3
          const nodeHeight = 140; // 节点高度约为140px
          const targetHeight = containerRect.height / 3;
          const targetZoom = Math.min(
            Math.max(targetHeight / nodeHeight, 0.5),
            3
          ); // 限制缩放范围

          // 应用视图变换（带动画）
          await reactFlowInstance.setViewport(
            {
              x: newX,
              y: newY,
              zoom: targetZoom,
            },
            { duration: 400 }
          );
        }
      }

      // 打开详情抽屉
      setSelectedNode(span);
      setIsDrawerOpen(true);
      setIsAnimating(false);
    },
    [
      isAnimating,
      reactFlowInstance,
      setSelectedNode,
      setIsDrawerOpen,
      setIsAnimating,
      setSavedViewState,
      isZoomEnabled,
    ]
  );

  // 恢复视图状态函数
  const restoreViewState = useCallback(async () => {
    // 只有在缩放功能启用且有保存状态时才执行恢复逻辑
    if (isZoomEnabled && savedViewState && !isAnimating) {
      setIsAnimating(true);

      // 恢复视口状态
      await reactFlowInstance.setViewport(savedViewState.viewport, {
        duration: 400,
      });

      // 恢复节点位置（如果需要的话）
      const currentNodes = reactFlowInstance.getNodes();
      const updatedNodes = currentNodes.map((node) => ({
        ...node,
        position: savedViewState.nodePositions[node.id] || node.position,
      }));
      reactFlowInstance.setNodes(updatedNodes);

      // 清理保存的状态
      setSavedViewState(null);
      setIsAnimating(false);
    }
  }, [
    savedViewState,
    isAnimating,
    reactFlowInstance,
    setIsAnimating,
    setSavedViewState,
    isZoomEnabled,
  ]);

  // 使用useEffect来设置事件监听器
  useEffect(() => {
    // 暴露函数给父组件使用
    (window as any).reactFlowHandleNodeClick = handleNodeClick;
    (window as any).reactFlowRestoreViewState = restoreViewState;
    (window as any).reactFlowInstance = reactFlowInstance;

    return () => {
      delete (window as any).reactFlowHandleNodeClick;
      delete (window as any).reactFlowRestoreViewState;
      delete (window as any).reactFlowInstance;
    };
  }, [handleNodeClick, restoreViewState, isAnimating, savedViewState, reactFlowInstance]);

  return null; // 这个组件不渲染任何内容，只提供功能
};

// 定义颜色池 - 为第一层级节点分配不同的颜色（高饱和度时髦配色）
const COLOR_PALETTE = [
  '#8B5CF6', // 鲜艳紫色 (Vibrant Purple)
  '#EC4899', // 热情粉红 (Hot Pink)
  '#06B6D4', // 电光青色 (Electric Cyan)
  '#F59E0B', // 活力琥珀 (Vibrant Amber)
  '#10B981', // 翡翠绿 (Emerald Green)
  '#3B82F6', // 明亮蓝 (Bright Blue)
  // '#EF4444', // 鲜红色 (Vivid Red)
  '#F97316', // 活力橙 (Energetic Orange)
  '#A855F7', // 霓虹紫 (Neon Purple)
  '#14B8A6', // 青绿色 (Teal)
];

// 智能状态判断函数
const determineNodeStatus = (span: Span): 'success' | 'error' => {
  // 首先检查 HTTP 状态码
  const rawJson = span.attributes?.raw_json;
  if (rawJson) {
    try {
      const parsedJson = typeof rawJson === 'string' ? JSON.parse(rawJson) : rawJson;
      const httpStatusCode = parsedJson.http_response_status_code;
      
      if (httpStatusCode !== undefined) {
        // HTTP 状态码存在时，根据状态码判断
        // 2xx 为成功，其他为错误
        return httpStatusCode >= 200 && httpStatusCode < 300 ? 'success' : 'error';
      }
    } catch (error) {
      // JSON 解析失败，继续使用 span.status
      console.warn('Failed to parse raw_json for HTTP status code:', error);
    }
  }
  
  // 如果没有 HTTP 状态码，使用原有的 span.status 判断
  return span.status === 'STATUS_CODE_OK' ? 'success' : 'error';
};

const TraceViewContent: React.FC<TraceViewContentProps> = ({ 
  traceData, 
  appId, 
  transactionId, 
  isZoomEnabled, 
  spanIdsToFocus,
  autoFitView = false,
  onViewportChange,
  onInitialFitViewComplete,
}) => {
  const [selectedNode, setSelectedNode] = useState<Span | null>(null);
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);
  
  // 为服务分配颜色映射
  const [serviceColors] = useState<Map<string, string>>(() => {
    const colorMap = new Map<string, string>();
    const allSpans = traceData.traces.flatMap((trace) => trace.spans);
    const serviceNames = new Set<string>();
    
    // 收集所有唯一的服务名称
    allSpans.forEach((span) => {
      const rawJson = span.attributes?.raw_json;
      let serviceName = 'Unknown Service';
      
      if (rawJson) {
        try {
          const parsedJson = typeof rawJson === 'string' ? JSON.parse(rawJson) : rawJson;
          serviceName = parsedJson.sp_service_name || span.serviceName || 'Unknown Service';
        } catch (error) {
          serviceName = span.serviceName || 'Unknown Service';
        }
      } else {
        serviceName = span.serviceName || 'Unknown Service';
      }
      
      serviceNames.add(serviceName);
    });
    
    // 为每个服务分配颜色
    Array.from(serviceNames).forEach((serviceName, index) => {
      colorMap.set(serviceName, COLOR_PALETTE[index % COLOR_PALETTE.length]);
    });
    
    return colorMap;
  });

  // 根据 span 获取服务颜色的辅助函数
  const getServiceColor = useCallback((span: Span): string => {
    const rawJson = span.attributes?.raw_json;
    let serviceName = 'Unknown Service';
    
    if (rawJson) {
      try {
        const parsedJson = typeof rawJson === 'string' ? JSON.parse(rawJson) : rawJson;
        serviceName = parsedJson.sp_service_name || span.serviceName || 'Unknown Service';
      } catch (error) {
        serviceName = span.serviceName || 'Unknown Service';
      }
    } else {
      serviceName = span.serviceName || 'Unknown Service';
    }
    
    return serviceColors.get(serviceName) || '#A14EFF';
  }, [serviceColors]);
  
  // 初始化折叠状态：User Session 下一层级的所有节点默认收起
  const [collapsed, setCollapsed] = useState<Set<string>>(() => {
    // 默认所有节点都是展开的
    return new Set<string>();
  });

  // 保存视图状态
  const [savedViewState, setSavedViewState] = useState<ViewState | null>(null);
  const [isAnimating, setIsAnimating] = useState(false);
  
  // 用于 fitView 的去抖动
  const fitViewTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  
  // 用于节点闪烁的状态（支持多个节点）
  const [flashingNodeIds, setFlashingNodeIds] = useState<Set<string>>(new Set());
  const [flashCount, setFlashCount] = useState(0);
  const flashIntervalRef = useRef<NodeJS.Timeout | null>(null);

  const computeLayoutPositions = useCallback((spans: Span[], collapsedSet: Set<string>) => {
    const dagreGraph = new dagre.graphlib.Graph({ compound: false });
    dagreGraph.setDefaultEdgeLabel(() => ({}));
    dagreGraph.setGraph({
      rankdir: 'LR', // 从左到右
      nodesep: 100, // 节点水平间距
      ranksep: 150, // 节点垂直间距
      marginx: 50,
      marginy: 50,
    });

    // 收集所有节点，但跳过收起节点的子孙
    const visibleSpans: Span[] = [];
    const collectVisibleSpans = (span: Span) => {
      visibleSpans.push(span);
      // 只有当父节点未折叠时，才继续收集其子节点
      if (span.children && span.children.length > 0 && !collapsedSet.has(span.spanId)) {
        span.children.forEach(collectVisibleSpans);
      }
    };

    spans.forEach(collectVisibleSpans);

    // 添加用户节点
    dagreGraph.setNode('user_0', { width: 240, height: 160 });

    // 添加所有可见的 span 节点到图中
    visibleSpans.forEach((span) => {
      dagreGraph.setNode(span.spanId, { width: 240, height: 160 });
    });

    // 从 User Session 连接到根节点
    spans.forEach((span) => {
      dagreGraph.setEdge('user_0', span.spanId);
    });

    // 添加父子关系边（仅连接可见节点）
    visibleSpans.forEach((span) => {
      if (span.children && span.children.length > 0 && !collapsedSet.has(span.spanId)) {
        span.children.forEach((child) => {
          // 仅当子节点也是可见的时才添加边
          if (visibleSpans.some(s => s.spanId === child.spanId)) {
            dagreGraph.setEdge(span.spanId, child.spanId);
          }
        });
      }
    });

    // 执行 dagre 布局计算
    dagre.layout(dagreGraph);

    // 提取计算后的位置
    const positions: Record<string, { x: number; y: number }> = {};
    dagreGraph.nodes().forEach((nodeId) => {
      const node = dagreGraph.node(nodeId);
      positions[nodeId] = { x: node.x, y: node.y };
    });

    return positions;
  }, []);

  const { nodes: baseNodes, edges: baseEdges } = useMemo(() => {
    const nodes: Node<TraceNodeData>[] = [];
    const edges: Edge[] = [];
    // 合并所有 traces 中的 spans
    const spans = traceData.traces.flatMap((trace) => trace.spans);

    // 现在数据结构已经整理好，每个trace的spans数组的第一个元素就是根节点
    const rootSpans = traceData.traces
      .map((trace) => trace.spans[0])
      .filter(Boolean);
    if (rootSpans.length > 0) {
      const userNodeData: TraceNodeData = {
        id: 'user_0',
        label: 'User',
        subtitle: 'User Operation',
        type: 'user',
        status: 'success',
        duration: 0,
        serviceName: 'User',
        startTime: rootSpans[0].startTime,
        endTime: rootSpans[rootSpans.length - 1].endTime,
        attributes: {},
        nodeData: {
          spanId: 'user_0',
          parentSpanId: null,
          traceId: rootSpans[0].traceId,
          name: 'User Operation',
          kind: 'SPAN_KIND_INTERNAL',
          startTime: rootSpans[0].startTime,
          endTime: rootSpans[rootSpans.length - 1].endTime,
          duration: 0,
          status: 'STATUS_CODE_OK',
          attributes: {},
          serviceName: 'User',
          spanType: 'user',
          children: [],
          requestLogs: [],
          responseLogs: [],
        },
        isRoot: true,
        isLeaf: false,
        traceData: traceData, // 传递完整的traceData给user节点
      };
      nodes.push({
        id: 'user_0',
        data: userNodeData,
        position: { x: 0, y: 0 },
        type: 'userNode',
      });
      for (const rs of rootSpans) {
        const edgeColor = getServiceColor(rs);
        edges.push({
          id: `user_0-${rs.spanId}`,
          source: 'user_0',
          target: rs.spanId,
          style: { stroke: edgeColor, strokeWidth: 2 },
          markerEnd: { type: MarkerType.ArrowClosed, color: edgeColor },
        });
      }
    }

    const processSpan = (span: Span, level = 0, inheritedColor?: string) => {
      const attributes = span.attributes || {};
      const rawJson: Record<string, any> = attributes.raw_json || {};

      // 确定节点颜色：使用服务颜色
      const nodeColor = getServiceColor(span);

      // 根据span.kind字段直接判断nodeType
      let nodeType = 'service'; // 默认值

      // 根据枚举值判断nodeType
      if (span.kind === 'SPAN_KIND_SERVER' || span.kind === '2') {
        nodeType = 'server';
      } else if (span.kind === 'SPAN_KIND_CLIENT' || span.kind === '3') {
        nodeType = 'service';
      } else if (span.kind === 'SPAN_KIND_PRODUCER' || span.kind === '4') {
        nodeType = 'service';
      } else if (span.kind === 'SPAN_KIND_CONSUMER' || span.kind === '5') {
        nodeType = 'service';
      } else if (span.kind === 'SPAN_KIND_INTERNAL' || span.kind === '1') {
        nodeType = 'service';
      } else if (span.kind === 'SPAN_KIND_UNSPECIFIED' || span.kind === '0') {
        nodeType = 'service';
      }

      // 如果attributes中有span.type，优先使用
      if (attributes['span_type']) {
        nodeType = attributes['span_type'];
      }

      const label = span.name || 'Unknown Operation';
      let subtitle = '';
      let method = '';
      let url = '';
      let operation = '';

      switch (nodeType) {
        case 'server':
          // 从raw_json中提取HTTP信息
          method = rawJson?.['http_request_header_:method'] || '';
          url = rawJson?.['http_request_header_:path'] || '';
          subtitle = `${method} ${url}`.trim();
          break;
        case 'database':
          operation = rawJson?.['db.operation'] || 'Database Operation';
          subtitle = operation;
          break;
        case 'redis':
          operation = rawJson?.['db.operation'] || 'Redis Operation';
          subtitle = operation;
          break;
        case 'service': {
          const serviceMethod = rawJson?.['http_request_header_:method'] || '';
          const serviceUrl = rawJson?.['http_request_header_:path'] || '';
          subtitle =
            serviceMethod && serviceUrl
              ? `${serviceMethod} ${serviceUrl}`
              : span.serviceName || 'Service';
          break;
        }
        default:
          subtitle = span.serviceName || 'Unknown';
      }
      const isLeaf = !span.children || span.children.length === 0;
      const isRoot = !span.parentSpanId;
      const data: TraceNodeData = {
        id: span.spanId,
        label,
        subtitle,
        type: nodeType,
        status: determineNodeStatus(span),
        duration: span.duration || 0,
        method: method || undefined,
        url,
        operation,
        serviceName: span.serviceName || undefined,
        startTime: span.startTime,
        endTime: span.endTime,
        attributes,
        nodeData: span,
        isRoot,
        isLeaf,
        themeColor: nodeColor,
      };
      let rfNodeType: Node<TraceNodeData>['type'] = 'serviceNode';
      if (nodeType === 'server') rfNodeType = 'serverNode';
      else if (nodeType === 'database') rfNodeType = 'databaseNode';
      else if (nodeType === 'redis') rfNodeType = 'redisNode';
      else if (nodeType === 'user') rfNodeType = 'userNode';
      nodes.push({
        id: span.spanId,
        data,
        position: { x: 0, y: 0 },
        type: rfNodeType,
      });

      if (span.children && span.children.length > 0) {
        for (const child of span.children) {
          edges.push({
            id: `${span.spanId}-${child.spanId}`,
            source: span.spanId,
            target: child.spanId,
            style: { stroke: nodeColor, strokeWidth: 2 },
            markerEnd: { type: MarkerType.ArrowClosed, color: nodeColor },
          });
          processSpan(child, level + 1, nodeColor);
        }
      }
    };

    for (const s of rootSpans) {
      processSpan(s, 0);
    }

    // Recompute isRoot/isLeaf using built edges, so it accounts for synthetic user->root edges
    const hasOutgoing = new Set<string>();
    const hasIncoming = new Set<string>();
    for (const e of edges) {
      if (e.source) hasOutgoing.add(e.source);
      if (e.target) hasIncoming.add(e.target);
    }
    for (const n of nodes) {
      const d = n.data as TraceNodeData;
      d.isLeaf = !hasOutgoing.has(n.id);
      d.isRoot = !hasIncoming.has(n.id);
      n.data = d;
    }

    const positions = computeLayoutPositions(spans, collapsed);
    for (const n of nodes) {
      const p = positions[n.id];
      if (p) n.position = p;
    }
    return { nodes, edges };
  }, [traceData, computeLayoutPositions, serviceColors, collapsed]);

  // Build adjacency for descendant calculation
  const childrenBySource = useMemo(() => {
    const map = new Map<string, string[]>();
    for (const e of baseEdges) {
      if (!e.source || !e.target) continue;
      if (!map.has(e.source)) map.set(e.source, []);
      map.get(e.source)!.push(e.target);
    }
    return map;
  }, [baseEdges]);

  const getDescendants = useCallback(
    (startId: string) => {
      const visited = new Set<string>();
      const stack = [startId];
      while (stack.length) {
        const id = stack.pop()!;
        const children = childrenBySource.get(id) || [];
        for (const c of children) {
          if (!visited.has(c)) {
            visited.add(c);
            stack.push(c);
          }
        }
      }
      // exclude the start node itself; we only hide its descendants
      visited.delete(startId);
      return visited;
    },
    [childrenBySource]
  );

  const hiddenNodeIds = useMemo(() => {
    const hidden = new Set<string>();
    for (const id of collapsed) {
      const desc = getDescendants(id);
      for (const d of desc) hidden.add(d);
    }
    return hidden;
  }, [collapsed, getDescendants]);

  const nodes = useMemo(
    () => baseNodes.filter((n) => !hiddenNodeIds.has(n.id)),
    [baseNodes, hiddenNodeIds]
  );
  const edges = useMemo(
    () =>
      baseEdges.filter(
        (e) => !hiddenNodeIds.has(e.source) && !hiddenNodeIds.has(e.target)
      ),
    [baseEdges, hiddenNodeIds]
  );

  const [rfNodes, setRfNodes] = useState<Node<TraceNodeData>[]>(nodes);
  const [rfEdges, setRfEdges] = useState<Edge[]>(edges);

  // 将闪烁计数写入节点data，强制ReactFlow更新节点
  useEffect(() => {
    setRfNodes((prev) => {
      if (!prev || prev.length === 0) return prev;
      return prev.map((n) => {
        if (flashingNodeIds.size > 0 && flashingNodeIds.has(n.id)) {
          return { ...n, data: { ...(n.data as any), flashSignal: flashCount } } as Node<TraceNodeData>;
        }
        if (flashingNodeIds.size === 0 && (n.data as any)?.flashSignal !== undefined) {
          const newData = { ...(n.data as any) };
          delete (newData as any).flashSignal;
          return { ...n, data: newData } as Node<TraceNodeData>;
        }
        return n;
      });
    });
  }, [flashingNodeIds, flashCount]);

  // 处理spanIdsToFocus的聚焦逻辑
  useEffect(() => {
    if (!spanIdsToFocus || spanIdsToFocus.length === 0) return;

    // 构建 parentMap 与 allIds 以支持任意深度查找
    const parentMap = new Map<string, string | null>();
    const collect = (span: Span, parentId: string | null) => {
      parentMap.set(span.spanId, parentId);
      if (span.children && span.children.length > 0) {
        for (const c of span.children) collect(c, span.spanId);
      }
    };
    for (const trace of traceData.traces) {
      for (const root of trace.spans) collect(root, null);
    }

    // 展开从目标到根的整条父链
    setCollapsed(prev => {
      const next = new Set(prev);
      for (const target of spanIdsToFocus) {
        let cur: string | null | undefined = target;
        while (cur) {
          next.delete(cur);
          cur = parentMap.get(cur) ?? null;
        }
      }
      return next;
    });

    // 清除之前的闪烁定时器
    if (flashIntervalRef.current) {
      clearInterval(flashIntervalRef.current);
    }

    // 等待节点渲染稳定后开始闪烁与聚焦
    const startEffects = () => {
      const targetSet = new Set<string>(spanIdsToFocus);
      setFlashingNodeIds(targetSet);
      // 立即进入高亮态，让用户第一时间看到闪烁开始
      setFlashCount(1);

      // 立即把 flashSignal 写入目标节点，确保首帧就高亮
      setRfNodes((prev) =>
        prev.map((n) => (targetSet.has(n.id)
          ? ({ ...n, data: { ...(n.data as any), flashSignal: 1 } } as Node<TraceNodeData>)
          : n))
      );

      // 闪烁3次（1秒一周期：显示/隐藏各500ms → 切换间隔1000ms更明显）
      flashIntervalRef.current = setInterval(() => {
        setFlashCount(prev => {
          if (prev >= 2) { // 2次切换=1次完整闪烁
            if (flashIntervalRef.current) clearInterval(flashIntervalRef.current);
            setFlashingNodeIds(new Set());
            return 0;
          }
          return prev + 1;
        });
      }, 1000);

      // 自动滚动到目标节点（若节点尚未可得，短暂重试）
      const tryFit = (attempts: number) => {
        if (attempts <= 0) return;
        if (typeof window !== 'undefined' && (window as any).reactFlowInstance) {
          const instance = (window as any).reactFlowInstance;
          const nodesToFit = spanIdsToFocus.map((id: string) => ({ id }));
          // 当任何目标节点存在时执行
          const hasAny = nodesToFit.some(({ id }) => instance.getNode(id));
          if (hasAny) {
            instance.fitView({
              nodes: nodesToFit,
              padding: 0.3,
              duration: 800,
              maxZoom: 1.5,
              minZoom: 0.7,
            });
            return;
          }
        }
        setTimeout(() => tryFit(attempts - 1), 150);
      };
      tryFit(5);
    };

    // 给布局更新留出时间
    const t = setTimeout(startEffects, 200);

    return () => {
      clearTimeout(t);
      if (flashIntervalRef.current) clearInterval(flashIntervalRef.current);
      // 清理 flashSignal，防止残留
      setRfNodes((prev) =>
        prev.map((n) => {
          if ((n.data as any)?.flashSignal !== undefined) {
            const newData = { ...(n.data as any) };
            delete (newData as any).flashSignal;
            return { ...n, data: newData } as Node<TraceNodeData>;
          }
          return n;
        })
      );
    };
  }, [spanIdsToFocus, traceData]);

  // 用于跟踪是否已经执行过初始fitView
  const hasPerformedInitialFitViewRef = useRef(false);
  // 用于跟踪是否正在执行自动fitView（防止用户交互触发回调）
  const isAutoFittingRef = useRef(false);
  
  // 当autoFitView变为true时，重置hasPerformedInitialFitViewRef（允许重新执行fitView）
  useEffect(() => {
    if (autoFitView) {
      hasPerformedInitialFitViewRef.current = false;
    }
  }, [autoFitView]);
  
  // Keep rf state in sync when visibility changes
  useEffect(() => {
    setRfNodes(nodes);
    setRfEdges(edges);
    
    // 只有在autoFitView为true且还没有执行过初始fitView时才自动fitView
    if (autoFitView && !hasPerformedInitialFitViewRef.current && nodes.length > 0) {
      // 清除之前的超时
      if (fitViewTimeoutRef.current) {
        clearTimeout(fitViewTimeoutRef.current);
      }
      
      // 当布局变化时，使用 fitView 重新定位视图（使用去抖动避免频繁调用）
      fitViewTimeoutRef.current = setTimeout(() => {
        if (typeof window !== 'undefined' && (window as any).reactFlowInstance) {
          const instance = (window as any).reactFlowInstance;
          isAutoFittingRef.current = true;
          // 使用较小的 padding 和稍长的动画时间以获得平滑效果
          instance.fitView({ 
            padding: 0.2,  // 减小内边距，避免过度缩放
            duration: 500,  // 稍长的动画时间
            maxZoom: 1,    // 限制最大缩放，避免过度放大
            minZoom: 0.5,  // 限制最小缩放
          }).then(() => {
            hasPerformedInitialFitViewRef.current = true;
            isAutoFittingRef.current = false;
            // 通知父组件初始fitView已完成
            if (onInitialFitViewComplete) {
              onInitialFitViewComplete();
            }
          }).catch(() => {
            // 如果fitView失败，也要重置标志
            isAutoFittingRef.current = false;
          });
        }
      }, 100); // 100ms 的去抖动延迟
    }
    
    // 清理
    return () => {
      if (fitViewTimeoutRef.current) {
        clearTimeout(fitViewTimeoutRef.current);
      }
    };
  }, [nodes, edges, autoFitView, onInitialFitViewComplete]);

  const onNodesChange = useCallback(
    (changes: NodeChange[]) =>
      setRfNodes(
        (nds) => applyNodeChanges(changes, nds) as Node<TraceNodeData>[]
      ),
    []
  );
  const onEdgesChange = useCallback(
    (changes: EdgeChange[]) =>
      setRfEdges((eds) => applyEdgeChanges(changes, eds)),
    []
  );
  const onConnect = useCallback(
    (params: Connection) => setRfEdges((eds) => addEdge(params, eds)),
    []
  );

  const onNodeClick = useCallback(
    async (event: any, node: Node<TraceNodeData>) => {
      // 调用通过window暴露的处理函数
      if (
        typeof window !== 'undefined' &&
        (window as any).reactFlowHandleNodeClick
      ) {
        await (window as any).reactFlowHandleNodeClick(event, node);
      }
    },
    []
  );

  const toggleCollapse = useCallback((nodeId: string) => {
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(nodeId)) {
        // 展开：从折叠集合中移除，同时把所有直接子节点加入折叠集合
        next.delete(nodeId);
        const directChildren = childrenBySource.get(nodeId) || [];
        for (const child of directChildren) {
          next.add(child);
        }
      } else {
        // 折叠：加入折叠集合
        next.add(nodeId);
      }
      return next;
    });
  }, [childrenBySource]);

  // 基础 handle 样式（用于普通连接点，保持简洁）
  const handleStyle: React.CSSProperties = useMemo(() => ({
    width: 12,
    height: 12,
    borderRadius: 6,
    border: '2px solid #4F46E5',
    background: '#ffffff',
    cursor: 'default',
  }), []);

  // 可展开/折叠节点的 handle 样式（有子节点的节点）
  const getExpandableHandleStyle = useCallback((nodeId: string): React.CSSProperties => {
    const isCollapsed = collapsed.has(nodeId);
    return {
      width: 20,
      height: 20,
      borderRadius: 10,
      border: '3px solid',
      borderColor: isCollapsed ? '#4F46E5' : '#10B981',
      background: isCollapsed 
        ? 'linear-gradient(135deg, #4F46E5 0%, #6366F1 100%)' // 折叠状态：蓝色背景（暗示可展开）
        : 'linear-gradient(135deg, #10B981 0%, #34D399 100%)', // 展开状态：绿色背景（暗示可折叠）
      cursor: 'pointer',
      boxShadow: isCollapsed
        ? '0 3px 12px rgba(79, 70, 229, 0.6), 0 0 0 3px rgba(79, 70, 229, 0.15)'
        : '0 3px 12px rgba(16, 185, 129, 0.6), 0 0 0 3px rgba(16, 185, 129, 0.15)',
      transition: 'all 0.2s ease-in-out',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      fontSize: '14px',
      fontWeight: 'bold',
      color: '#ffffff',
    };
  }, [collapsed]);

  const UserNode = useCallback((props: NodeProps) => {
    const data = props.data as TraceNodeData;
    const { traceData } = props.data as any;
    
    // 使用 data.flashSignal 控制闪烁（由上层写入，确保ReactFlow重渲染）
    const isFlashing = typeof (data as any)?.flashSignal === 'number' && ((data as any).flashSignal % 2 === 1);
    
    // 从traceData中提取用户相关信息
    const sessionId = traceData?.sessionId || 'Unknown';
    const totalTraces = traceData?.totalTraces || 0;
    const totalSpans = traceData?.totalSpans || 0;

    // 计算整体时间范围
    const allSpans =
      traceData?.traces?.flatMap((trace: any) => trace.spans) || [];
    const startTimes = allSpans
      .map((span: any) => new Date(span.startTime).getTime())
      .filter((t: any) => !isNaN(t));
    const endTimes = allSpans
      .map((span: any) => new Date(span.endTime).getTime())
      .filter((t: any) => !isNaN(t));

    const overallStartTime =
      startTimes.length > 0 ? Math.min(...startTimes) : null;
    const overallEndTime = endTimes.length > 0 ? Math.max(...endTimes) : null;
    const totalDuration =
      overallStartTime && overallEndTime
        ? overallEndTime - overallStartTime
        : 0;

    const formatTime = (timestamp: number) => {
      return new Date(timestamp).toLocaleTimeString('zh-CN', {
        hour12: false,
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        fractionalSecondDigits: 3,
      });
    };

    // 处理点击事件
    const handleClick = () => {
      if (typeof window !== 'undefined' && (window as any).reactFlowHandleNodeClick) {
        (window as any).reactFlowHandleNodeClick(null, { data });
      }
    };

    return (
      <div
        style={{
          position: 'relative',
          borderRadius: 25,
          border: isFlashing ? '4px solid #FFD700' : '2px solid #4F46E5',
          backgroundColor: isFlashing ? '#FFFDE7' : '#fff',
          width: 240,
          minHeight: 160,
          cursor: 'pointer',
          boxShadow: isFlashing ? '0 0 28px rgba(255, 215, 0, 0.9)' : 'none',
          transform: isFlashing ? 'scale(1.04)' : 'scale(1.0)',
          transition: 'all 0.25s ease',
        }}
        onClick={handleClick}
      >
        {/* User is synthetic root → no left(target) handle */}
        <Handle type="source" position={Position.Right} style={getExpandableHandleStyle(data.id)} />
        <div
          style={{
            borderRadius: 25,
            display: 'flex',
            alignItems: 'center',
            height: 44,
            padding: '8px 12px',
            justifyContent: 'center',
            background: '#4F46E5',
            color: '#fff',
          }}
        >
          <span style={{ fontSize: 14, fontWeight: 'bold' }}>
            👤 User Session
          </span>
        </div>
        <div style={{ padding: '8px 12px' }}>
           {/* 统计信息 */}
           <div
             style={{
               display: 'flex',
               justifyContent: 'space-between',
               marginBottom: 8,
             }}
           >
             <div style={{ textAlign: 'center', flex: 1 }}>
               <div style={{ fontSize: 10, color: '#6B7280' }}>Traces</div>
               <div
                 style={{ 
                   fontSize: 12, 
                   fontWeight: 'bold', 
                   color: '#4F46E5',
                   backgroundColor: '#EEF2FF',
                   borderRadius: 4,
                   padding: '2px 6px',
                   display: 'inline-block'
                 }}
               >
                 {totalTraces}
               </div>
             </div>
             <div style={{ textAlign: 'center', flex: 1 }}>
               <div style={{ fontSize: 10, color: '#6B7280' }}>Spans</div>
               <div
                 style={{ 
                   fontSize: 12, 
                   fontWeight: 'bold', 
                   color: '#4F46E5',
                   backgroundColor: '#EEF2FF',
                   borderRadius: 4,
                   padding: '2px 6px',
                   display: 'inline-block'
                 }}
               >
                 {totalSpans}
               </div>
             </div>
           </div>

          {/* 基础信息列表 */}
          <div
            style={{
              marginBottom: 8,
              padding: '6px 8px',
              backgroundColor: '#F3F4F6',
              borderRadius: 6,
            }}
          >
            <div style={{ fontSize: 10, color: '#6B7280', marginBottom: 4, fontWeight: 'bold' }}>
              Session Info
            </div>
            
            {/* Session ID */}
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
              <span style={{ fontSize: 9, color: '#6B7280' }}>Session ID:</span>
              <span style={{ fontSize: 9, fontWeight: 'bold', color: '#374151', fontFamily: 'monospace' }}>
                {sessionId.length > 15 ? `${sessionId.substring(0, 15)}...` : sessionId}
              </span>
            </div>
            
            {/* 时间范围 */}
            {overallStartTime && overallEndTime && (
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                <span style={{ fontSize: 9, color: '#6B7280' }}>Time Range:</span>
                <span style={{ fontSize: 9, color: '#9CA3AF' }}>
                  {formatTime(overallStartTime)} - {formatTime(overallEndTime)}
                </span>
              </div>
            )}
            
            {/* Duration */}
            {overallStartTime && overallEndTime && (
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                <span style={{ fontSize: 9, color: '#6B7280' }}>Duration:</span>
                <span style={{ fontSize: 9, fontWeight: 'bold', color: '#059669' }}>
                  {totalDuration.toFixed(0)}ms
                </span>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }, [getExpandableHandleStyle, flashingNodeIds, flashCount]);

  const ServerNode = useCallback((props: NodeProps) => {
    const data = props.data as TraceNodeData;
    const themeColor = data?.themeColor || '#A14EFF';
    
    // 使用 data.flashSignal 控制闪烁
    const isFlashing = typeof (data as any)?.flashSignal === 'number' && ((data as any).flashSignal % 2 === 1);
    
    const displayUrl = (() => {
      const u = data?.url || data?.subtitle || 'SERVER';
      if (u === 'SERVER') return u;
      const parts = u.split('/').filter((p: string) => p.length > 0);
      if (parts.length <= 2) return u;
      return '/' + parts.slice(-2).join('/');
    })();
    
    // 提取服务信息
    const serviceInfo = (() => {
      const a = data?.attributes as any;
      const rawJson: Record<string, any> = a?.raw_json || {};

      // 从raw_json中提取trafficDirection，清理空字符
      const trafficDirectionRaw = rawJson['sp_traffic_direction'];
      console.log('trafficDirectionRaw', trafficDirectionRaw);
      // 去除 null 字符和其他空白字符
      const trafficDirection = trafficDirectionRaw 
        ? trafficDirectionRaw.split(String.fromCharCode(0)).join('').trim() 
        : '';
      
      return {
        serviceName: rawJson['sp_service_name'] || data?.serviceName || 'Unknown Service',
        spanType: rawJson['sp_span_type'] || 'HTTP',
        trafficDirection: trafficDirection || 'inbound',
        method: data?.method || rawJson['http_request_header_:method'] || 'GET',
        status: data?.status || 'success',
      };
    })();

    return (
      <div
        style={{
          position: 'relative',
          borderRadius: 25,
          border: isFlashing ? `4px solid #FFD700` : `2px solid ${themeColor}`,
          backgroundColor: isFlashing ? '#FFFDE7' : '#fff',
          width: 240,
          minHeight: 160,
          cursor: 'pointer',
          boxShadow: isFlashing ? '0 0 28px rgba(255, 215, 0, 0.9)' : 'none',
          transform: isFlashing ? 'scale(1.04)' : 'scale(1.0)',
          transition: 'all 0.25s ease',
        }}
      >
        {!data.isRoot && (
          <Handle 
            type="target" 
            position={Position.Left} 
            style={{
              width: 12,
              height: 12,
              borderRadius: 6,
              border: `2px solid ${themeColor}`,
              background: '#ffffff',
              cursor: 'default',
            }} 
          />
        )}
        {!data.isLeaf && (
          <div
            style={{
              position: 'absolute',
              right: '-1px',
              top: '50%',
              transform: 'translateY(-50%)',
            }}
          >
            <Handle
              type="source"
              position={Position.Right}
              style={getExpandableHandleStyle(data.id)}
              onClick={(e) => {
                e.stopPropagation();
                e.preventDefault();
                toggleCollapse(data.id);
              }}
            />
            <div
              style={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                fontSize: '14px',
                fontWeight: 'bold',
                color: '#ffffff',
                pointerEvents: 'none',
                lineHeight: 1,
              }}
            >
              {collapsed.has(data.id) ? '+' : '−'}
            </div>
          </div>
        )}
        
        {/* Header */}
        <div
          style={{
            borderRadius: 25,
            display: 'flex',
            alignItems: 'center',
            minHeight: 44,
            padding: '8px 20px',
            justifyContent: 'space-between',
            background: themeColor,
            color: '#fff',
          }}
        >
          <span
            style={{
              fontSize: 14,
              fontWeight: 'bold',
              wordWrap: 'break-word',
              wordBreak: 'break-all',
              whiteSpace: 'normal',
              lineHeight: 1.2,
            }}
          >
            {displayUrl}
          </span>
        </div>
        
        <div style={{ padding: '8px 12px' }}>
          {/* 统计信息 */}
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              marginBottom: 8,
            }}
          >
            <div
              style={{ 
                fontSize: 12, 
                fontWeight: 'bold', 
                color: themeColor,
                backgroundColor: '#F3F4F6',
                borderRadius: 4,
                padding: '2px 6px',
                display: 'inline-block'
              }}
            >
              {serviceInfo.method}
            </div>
            {serviceInfo.trafficDirection ? (
              <div
                style={{ 
                  fontSize: 12, 
                  fontWeight: 'bold', 
                  color: themeColor,
                  backgroundColor: '#F3F4F6',
                  borderRadius: 4,
                  padding: '2px 6px',
                  display: 'inline-block'
                }}
              >
                {serviceInfo.trafficDirection?.toUpperCase()}
              </div>
            ) : null}
            <div
              style={{ 
                fontSize: 12, 
                fontWeight: 'bold', 
                color: serviceInfo.status === 'success' ? '#059669' : '#DC2626',
                backgroundColor: serviceInfo.status === 'success' ? '#D1FAE5' : '#FEE2E2',
                borderRadius: 4,
                padding: '2px 6px',
                display: 'inline-block'
              }}
            >
              {serviceInfo.status.toUpperCase()}
            </div>
            
          </div>

          {/* 基础信息列表 */}
          <div
            style={{
              marginBottom: 8,
              padding: '6px 8px',
              backgroundColor: '#F3F4F6',
              borderRadius: 6,
            }}
          >
            {/* Service Name */}
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
              <span style={{ fontSize: 9, color: '#6B7280' }}>Service:</span>
              <span style={{ fontSize: 9, fontWeight: 'bold', color: '#374151' }}>
                {serviceInfo.serviceName.length > 15 ? `${serviceInfo.serviceName.substring(0, 15)}...` : serviceInfo.serviceName}
              </span>
            </div>
            
            {/* URL Path */}
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
              <span style={{ fontSize: 9, color: '#6B7280' }}>Path:</span>
              <span style={{ fontSize: 9, color: '#9CA3AF', fontFamily: 'monospace' }}>
                {displayUrl.length > 20 ? `${displayUrl.substring(0, 20)}...` : displayUrl}
              </span>
            </div>
            
            {/* Duration */}
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
              <span style={{ fontSize: 9, color: '#6B7280' }}>Duration:</span>
              <span style={{ fontSize: 9, fontWeight: 'bold', color: '#059669' }}>
                {data?.duration && data.duration >= 1000
                  ? (data.duration / 1000).toFixed(1) + 's'
                  : (data?.duration ?? 0) + 'ms'}
              </span>
            </div>
            
            {/* Time */}
            {data?.startTime && (
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                <span style={{ fontSize: 9, color: '#6B7280' }}>Time:</span>
                <span style={{ fontSize: 9, color: '#9CA3AF' }}>
                  {new Date(data.startTime).toLocaleTimeString('zh-CN', {
                    hour12: false,
                    hour: '2-digit',
                    minute: '2-digit',
                    second: '2-digit',
                  })}
                </span>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }, [toggleCollapse, getExpandableHandleStyle, collapsed, flashingNodeIds, flashCount]);

  const DatabaseNode = useCallback((props: NodeProps) => {
    const data = props.data as TraceNodeData;
    const themeColor = data?.themeColor || '#868484';
    const dbName = (data as any)?.nodeData?.attributes?.DbName;
    const tableName = (data as any)?.nodeData?.attributes?.TableName;
    
    // 使用 data.flashSignal 控制闪烁
    const isFlashing = typeof (data as any)?.flashSignal === 'number' && ((data as any).flashSignal % 2 === 1);
    return (
      <div
        style={{
          width: 240,
          minHeight: 140,
          display: 'flex',
          alignItems: 'flex-start',
          cursor: 'pointer',
        }}
      >
        <div
          style={{
            position: 'relative',
            borderRadius: 25,
            border: isFlashing ? `4px solid #FFD700` : `2px solid ${themeColor}`,
            backgroundColor: isFlashing ? '#FFFDE7' : '#fff',
            width: 220,
            minHeight: 120,
            boxShadow: isFlashing ? '0 0 28px rgba(255, 215, 0, 0.9)' : 'none',
            transform: isFlashing ? 'scale(1.05)' : 'scale(1.0)',
            transition: 'all 0.25s ease',
          }}
        >
          {!data.isRoot && (
            <Handle
              type="target"
              position={Position.Left}
              style={{
                width: 12,
                height: 12,
                borderRadius: 6,
                border: `2px solid ${themeColor}`,
                background: '#ffffff',
                cursor: 'default',
              }}
            />
          )}
          {!data.isLeaf && (
            <div
              style={{
                position: 'absolute',
                right: '-1px',
                top: '50%',
                transform: 'translateY(-50%)',
              }}
            >
              <Handle
                type="source"
                position={Position.Right}
                style={getExpandableHandleStyle(data.id)}
                onClick={(e) => {
                  e.stopPropagation();
                  e.preventDefault();
                  toggleCollapse(data.id);
                }}
              />
              <div
                style={{
                  position: 'absolute',
                  top: '50%',
                  left: '50%',
                  transform: 'translate(-50%, -50%)',
                  fontSize: '14px',
                  fontWeight: 'bold',
                  color: '#ffffff',
                  pointerEvents: 'none',
                  lineHeight: 1,
                }}
              >
                {collapsed.has(data.id) ? '+' : '−'}
              </div>
            </div>
          )}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              padding: '8px 12px 4px 12px',
              color: '#000',
            }}
          >
            <span
              style={{
                display: 'inline-block',
                width: 12,
                height: 12,
                backgroundColor: '#94FF76',
                borderRadius: 2,
                marginRight: 4,
              }}
            ></span>
            <span style={{ fontSize: 13, fontWeight: 'bold' }}>Database</span>
          </div>
          <div style={{ padding: '0px 12px 12px 28px' }}>
            <div style={{ fontSize: 11, textAlign: 'left', marginBottom: 2 }}>
              DB Name: {dbName}
            </div>
            <div style={{ fontSize: 11, textAlign: 'left', marginBottom: 2 }}>
              Table: {tableName}
            </div>
            <div
              style={{
                fontSize: 11,
                marginBottom: 2,
                textAlign: 'left',
                wordWrap: 'break-word',
                wordBreak: 'break-all',
                whiteSpace: 'normal',
                lineHeight: 1.2,
              }}
            >
              SQL: {data?.label}
            </div>
            <div
              style={{
                borderRadius: 2,
                width: 48,
                height: 16,
                fontSize: 11,
                textAlign: 'center',
                marginBottom: 2,
                backgroundColor: '#ECECEC',
                lineHeight: '16px',
              }}
            >
              {data?.duration && data.duration >= 1000
                ? (data.duration / 1000).toFixed(1) + 's'
                : (data?.duration ?? 0) + 'ms'}
            </div>
          </div>
        </div>
      </div>
    );
  }, [toggleCollapse, getExpandableHandleStyle, collapsed, flashingNodeIds, flashCount]);

  const RedisNode = useCallback((props: NodeProps) => {
    const data = props.data as TraceNodeData;
    const themeColor = data?.themeColor || '#868484';
    const redisCommand = (data as any)?.nodeData?.attributes?.RedisCommand || 'OP';
    const redisKey = (data as any)?.nodeData?.attributes?.RedisKey || 'key';
    
    // 使用 data.flashSignal 控制闪烁
    const isFlashing = typeof (data as any)?.flashSignal === 'number' && ((data as any).flashSignal % 2 === 1);
    return (
      <div
        style={{
          width: 200,
          minHeight: 140,
          display: 'flex',
          alignItems: 'flex-start',
          cursor: 'pointer',
        }}
      >
        <div
          style={{
            position: 'relative',
            borderRadius: 25,
            border: isFlashing ? `4px solid #FFD700` : `2px solid ${themeColor}`,
            backgroundColor: isFlashing ? '#FFFDE7' : '#fff',
            width: 180,
            minHeight: 120,
            boxShadow: isFlashing ? '0 0 28px rgba(255, 215, 0, 0.9)' : 'none',
            transform: isFlashing ? 'scale(1.05)' : 'scale(1.0)',
            transition: 'all 0.25s ease',
          }}
        >
          {!data.isRoot && (
            <Handle
              type="target"
              position={Position.Left}
              style={{
                width: 12,
                height: 12,
                borderRadius: 6,
                border: `2px solid ${themeColor}`,
                background: '#ffffff',
                cursor: 'default',
              }}
            />
          )}
          {!data.isLeaf && (
            <div
              style={{
                position: 'absolute',
                right: '-1px',
                top: '50%',
                transform: 'translateY(-50%)',
              }}
            >
              <Handle
                type="source"
                position={Position.Right}
                style={getExpandableHandleStyle(data.id)}
                onClick={(e) => {
                  e.stopPropagation();
                  e.preventDefault();
                  toggleCollapse(data.id);
                }}
              />
              <div
                style={{
                  position: 'absolute',
                  top: '50%',
                  left: '50%',
                  transform: 'translate(-50%, -50%)',
                  fontSize: '14px',
                  fontWeight: 'bold',
                  color: '#ffffff',
                  pointerEvents: 'none',
                  lineHeight: 1,
                }}
              >
                {collapsed.has(data.id) ? '+' : '−'}
              </div>
            </div>
          )}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              padding: '8px 12px',
              color: '#000',
            }}
          >
            <span
              style={{
                display: 'inline-block',
                width: 12,
                height: 12,
                backgroundColor: '#FF7676',
                borderRadius: 2,
                marginRight: 4,
              }}
            ></span>
            <span style={{ fontSize: 13, fontWeight: 'bold' }}>Redis</span>
          </div>
          <div style={{ padding: '0px 12px 12px 28px' }}>
            <div
              style={{
                fontSize: 11,
                marginBottom: 2,
                textAlign: 'left',
                wordWrap: 'break-word',
                wordBreak: 'break-all',
                whiteSpace: 'normal',
                lineHeight: 1.2,
              }}
            >
              Operation: {data?.label}
            </div>
            <div
              style={{
                fontSize: 11,
                textAlign: 'left',
                marginBottom: 2,
                wordWrap: 'break-word',
                wordBreak: 'break-all',
                whiteSpace: 'normal',
                lineHeight: 1.2,
              }}
            >
              Key: {redisKey || 'N/A'}
            </div>
            <div style={{ fontSize: 11, textAlign: 'left', marginBottom: 2 }}>
              Hits: {data?.status || 'N/A'}
            </div>
            <div
              style={{
                borderRadius: 2,
                width: 48,
                height: 16,
                fontSize: 11,
                textAlign: 'center',
                marginBottom: 2,
                backgroundColor: '#ECECEC',
                lineHeight: '16px',
              }}
            >
              {data?.duration && data.duration >= 1000
                ? (data.duration / 1000).toFixed(1) + 's'
                : (data?.duration ?? 0) + 'ms'}
            </div>
          </div>
        </div>
      </div>
    );
  }, [toggleCollapse, getExpandableHandleStyle, collapsed, flashingNodeIds, flashCount]);

  const ServiceNode = useCallback((props: NodeProps) => {
    const data = props.data as TraceNodeData;
    const themeColor = data?.themeColor || '#0EA5E9';
    
    // 使用 data.flashSignal 控制闪烁（由上层写入，确保ReactFlow重渲染）
    const isFlashing = typeof (data as any)?.flashSignal === 'number' && ((data as any).flashSignal % 2 === 1);
    return (
      <div
        style={{
          position: 'relative',
          borderRadius: 12,
          border: isFlashing ? `4px solid #FFD700` : `2px solid ${themeColor}`,
          backgroundColor: isFlashing ? '#FFFDE7' : '#fff',
          width: 200,
          minHeight: 120,
          cursor: 'pointer',
          boxShadow: isFlashing ? '0 0 28px rgba(255, 215, 0, 0.9)' : 'none',
          transform: isFlashing ? 'scale(1.05)' : 'scale(1.0)',
          transition: 'all 0.25s ease',
        }}
      >
        {!data.isRoot && (
          <Handle 
            type="target" 
            position={Position.Left} 
            style={{
              width: 12,
              height: 12,
              borderRadius: 6,
              border: `2px solid ${themeColor}`,
              background: '#ffffff',
              cursor: 'default',
            }} 
          />
        )}
        {!data.isLeaf && (
          <div
            style={{
              position: 'absolute',
              right: '-1px',
              top: '50%',
              transform: 'translateY(-50%)',
            }}
          >
            <Handle
              type="source"
              position={Position.Right}
              style={getExpandableHandleStyle(data.id)}
              onClick={(e) => {
                e.stopPropagation();
                e.preventDefault();
                toggleCollapse(data.id);
              }}
            />
            <div
              style={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                fontSize: '14px',
                fontWeight: 'bold',
                color: '#ffffff',
                pointerEvents: 'none',
                lineHeight: 1,
              }}
            >
              {collapsed.has(data.id) ? '+' : '−'}
            </div>
          </div>
        )}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            padding: '8px 12px',
            background: `${themeColor}15`,
          }}
        >
          <span style={{ fontSize: 13, fontWeight: 'bold', color: '#0F172A' }}>
            {data?.serviceName || 'Service'}
          </span>
        </div>
        <div style={{ padding: '8px 12px' }}>
          <div style={{ fontSize: 11, color: '#334155', marginBottom: 4 }}>
            {data?.subtitle}
          </div>
          <div
            style={{
              borderRadius: 2,
              width: 48,
              height: 16,
              fontSize: 11,
              textAlign: 'center',
              marginBottom: 4,
              backgroundColor: '#ECECEC',
              lineHeight: '16px',
            }}
          >
            {data?.duration && data.duration >= 1000
              ? (data.duration / 1000).toFixed(1) + 's'
              : (data?.duration ?? 0) + 'ms'}
          </div>
        </div>
      </div>
    );
  }, [toggleCollapse, getExpandableHandleStyle, collapsed, flashingNodeIds, flashCount]);

  const nodeTypes = useMemo(
    () => ({
      userNode: UserNode,
      serverNode: ServerNode,
      databaseNode: DatabaseNode,
      redisNode: RedisNode,
      serviceNode: ServiceNode,
    }),
    [UserNode, ServerNode, DatabaseNode, RedisNode, ServiceNode]
  ) as Record<string, React.ComponentType<NodeProps>>;

  // 监听viewport变化，检测用户手动调整
  // 使用onMoveStart来检测用户开始手动调整视图（拖拽、缩放等）
  const onMoveStart = useCallback((_event: any, _viewport: Viewport) => {
    // 如果正在执行自动fitView，不触发回调
    if (isAutoFittingRef.current) {
      return;
    }
    // 如果已经执行过初始fitView，说明用户可能在手动调整
    if (hasPerformedInitialFitViewRef.current && onViewportChange) {
      onViewportChange();
    }
  }, [onViewportChange]);
  
  // 使用onMoveEnd来检测用户完成手动调整（更准确）
  const onMoveEnd = useCallback((_event: any, _viewport: Viewport) => {
    // 如果正在执行自动fitView，不触发回调
    if (isAutoFittingRef.current) {
      return;
    }
    // 如果已经执行过初始fitView，说明用户可能在手动调整
    if (hasPerformedInitialFitViewRef.current && onViewportChange) {
      onViewportChange();
    }
  }, [onViewportChange]);

  if (
    !traceData ||
    traceData.traces.length === 0 ||
    traceData.traces.every((trace) => trace.spans.length === 0)
  ) {
    return <div className="py-8 text-center text-gray-500">No trace data</div>;
  }

  return (
    <>
      <ReactFlow
        nodes={rfNodes}
        edges={rfEdges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        onMoveStart={onMoveStart}
        onMoveEnd={onMoveEnd}
        nodeTypes={nodeTypes}
        fitView={false}
        // fitViewOptions={{ padding: 0.2 }}
      >
        <Background />
        <MiniMap />
        <Controls />
        
        {/* 展开/折叠所有按钮 */}
        <div
          style={{
            position: 'absolute',
            top: '16px',
            right: '16px',
            zIndex: 5,
          }}
        >
          {collapsed.size > 0 ? (
            // 展开所有按钮 - 有收起的节点时显示
            <button
              onClick={() => setCollapsed(new Set())}
              className="px-4 py-2 text-sm font-medium text-white rounded-lg shadow-lg transition-all duration-200 hover:shadow-xl hover:scale-105 active:scale-95"
              style={{
                background: 'linear-gradient(135deg, #4F46E5 0%, #6366F1 100%)',
                border: 'none',
                cursor: 'pointer',
              }}
              title="Expand All Collapsed Spans"
            >
              📂 Expand All Collapsed Spans
            </button>
          ) : (
            // 折叠所有按钮 - 所有节点都展开时显示
            <button
              onClick={() => {
                const rootSpans = traceData.traces
                  .map((trace) => trace.spans[0])
                  .filter(Boolean);
                const allRootIds = new Set(rootSpans.map((span) => span.spanId));
                setCollapsed(allRootIds);
              }}
              className="px-4 py-2 text-sm font-medium text-white rounded-lg shadow-lg transition-all duration-200 hover:shadow-xl hover:scale-105 active:scale-95"
              style={{
                background: 'linear-gradient(135deg, #10B981 0%, #34D399 100%)',
                border: 'none',
                cursor: 'pointer',
              }}
              title="Collapse All Spans"
            >
              📁 Collapse All Spans
            </button>
          )}
        </div>
        
        <ReactFlowInteractionHandler
          selectedNode={selectedNode}
          setSelectedNode={setSelectedNode}
          setIsDrawerOpen={setIsDrawerOpen}
          savedViewState={savedViewState}
          setSavedViewState={setSavedViewState}
          isAnimating={isAnimating}
          setIsAnimating={setIsAnimating}
          isZoomEnabled={isZoomEnabled}
        />
      </ReactFlow>

      <TraceDetailDrawer
        node={selectedNode}
        isOpen={isDrawerOpen}
        onClose={async () => {
          // 先关闭抽屉，开始关闭动画
          setIsDrawerOpen(false);
          setSelectedNode(null);

          // 等待抽屉关闭动画完成（250ms），然后执行视图复原
          setTimeout(async () => {
            if (
              typeof window !== 'undefined' &&
              (window as any).reactFlowRestoreViewState
            ) {
              await (window as any).reactFlowRestoreViewState();
            }
          }, 250); // 稍微多等50ms确保抽屉动画完全结束
        }}
      />
    </>
  );
};

// 主导出组件，包装ReactFlow Provider
export const ReactFlowTraceView: React.FC<ReactFlowTraceViewProps> = ({
  traceData,
  appId,
  transactionId,
  spanIdsToFocus,
  autoFitView = false,
  onViewportChange,
  onInitialFitViewComplete,
}) => {
  const [isZoomEnabled, setIsZoomEnabled] = useState(false);

  return (
    <div className="relative h-full w-full">
      <div className="absolute top-4 right-4 z-10 flex gap-2">
        {/* 缩放功能切换按钮 */}
        {/* <button
          onClick={() => setIsZoomEnabled(!isZoomEnabled)}
          className={`px-3 py-2 text-sm rounded-md font-medium transition-colors ${
            isZoomEnabled
              ? 'bg-blue-100 text-blue-700 hover:bg-blue-200'
              : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
          }`}
          title={isZoomEnabled ? '关闭自动缩放' : '开启自动缩放'}
        >
          {isZoomEnabled ? '🔍 缩放已启用' : '🚫 缩放已禁用'}
        </button> */}


        
      </div>

      <div
        className="h-full w-full rounded-lg border border-gray-200"
        style={{ backgroundColor: '#f8fafc' }}
      >
        <TraceViewContent
          traceData={traceData}
          appId={appId}
          transactionId={transactionId}
          isZoomEnabled={isZoomEnabled}
          spanIdsToFocus={spanIdsToFocus}
          autoFitView={autoFitView}
          onViewportChange={onViewportChange}
          onInitialFitViewComplete={onInitialFitViewComplete}
        />
      </div>
    </div>
  );
};

export default ReactFlowTraceView;

