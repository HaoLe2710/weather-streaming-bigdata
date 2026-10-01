# Superseded nationwide batch telemetry

This initial successful nationwide pass recorded the correct business counts (4,536 hourly rows, 1,512 warm-up skips, 3,024 forecasts, 0 gaps, and 0 rejects). Its early batch-metrics writer did not yet capture worker IDs, model-load counts, or feature/prediction timings, so those telemetry fields are zero/empty. The log is preserved for provenance; the replay-idempotency log is the authoritative latency and worker-load evidence.

Source code commit: 181b27a85f45d641f78d57ca066916c6dece5e35.
