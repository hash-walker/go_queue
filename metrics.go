type Metrics struct {
	jobsProcessed *prometheus.CounterVec   // labels: job_type, status
	jobDuration   *prometheus.HistogramVec // labels: job_type
	activeWorkers prometheus.Gauge
	queueDepth    *prometheus.GaugeVec // labels: status
}
