# @softprobe/context-viewer

A reusable library for visualizing distributed trace call graphs.

## Features

- Interactive call graph visualization using React Flow
- Session list with filtering capabilities
- Trace detail inspection
- Configurable API endpoint
- No authentication or tenant management dependencies

## Installation

This library is designed to be copied into your project or published as an npm package.

## Usage

```typescript
import { 
  ReactFlowTraceView, 
  TraceFilterPanel, 
  SessionList,
  querySessions,
  fetchTraceDataNew 
} from '@softprobe/context-viewer';
```

## Configuration

Set the `NEXT_PUBLIC_API_BASE_URL` environment variable to configure the API endpoint.

## API

The library expects the following API endpoints:

- `GET {API_BASE_URL}/sessions` - Query sessions
- `GET {API_BASE_URL}/sessions/{sessionId}` - Get trace data

## Components

- `ReactFlowTraceView` - Main visualization component
- `TraceFilterPanel` - Filtering UI for sessions
- `SessionList` - List of sessions
- `TraceDetailDrawer` - Detail view for trace spans

## License

MIT

