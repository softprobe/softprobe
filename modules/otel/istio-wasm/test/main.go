package main

import (
    "context"
    "io"
    "log"
    "net/http"
    "os"
    "strings"
    "time"

    "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

// Initialize OpenTelemetry
func initTracer() *sdktrace.TracerProvider {
    endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
    if endpoint == "" {
        tp := sdktrace.NewTracerProvider()
        otel.SetTracerProvider(tp)
        otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
        return tp
    }

    log.Println("Initializing tracer with endpoint:", endpoint)
    client := otlptracehttp.NewClient(
        otlptracehttp.WithEndpointURL(endpoint),
    )

    exporter, err := otlptrace.New(context.Background(), client)
    if err != nil {
        log.Fatal(err)
    }

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("softprobe-integration-test"),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    _, _ = w.Write([]byte("ok"))
}

// Proxy httpbin
func proxyHttpbin(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()

    path := r.URL.Path
    httpbinPath := "https://httpbin.org" + path

    client := &http.Client{Timeout: 10 * time.Second}
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpbinPath, nil)
    if err != nil {
        http.Error(w, "Failed to create request", http.StatusInternalServerError)
        return
    }

    resp, err := client.Do(req)
    if err != nil {
        http.Error(w, "Failed to fetch httpbin json", http.StatusInternalServerError)
        return
    }
    defer resp.Body.Close()

    for key, values := range resp.Header {
        for _, value := range values {
            w.Header().Add(key, value)
        }
    }
    w.WriteHeader(resp.StatusCode)
    _, _ = io.Copy(w, resp.Body)
}

func main() {
	tp := initTracer()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

    http.HandleFunc("/health", otelhttp.NewHandler(http.HandlerFunc(healthHandler), "health").ServeHTTP)
    http.HandleFunc("/json", otelhttp.NewHandler(http.HandlerFunc(proxyHttpbin), "json").ServeHTTP)
    http.HandleFunc("/delay/", otelhttp.NewHandler(http.HandlerFunc(proxyHttpbin), "delay").ServeHTTP)

	// Start server
	log.Println("Starting server on :80")
	http.ListenAndServe(":80", nil)
}
