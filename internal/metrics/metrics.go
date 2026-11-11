package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EnqueueTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_enqueue_total",
		Help: "Webhook enqueue operations by result",
	}, []string{"result"})

	DeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_delivery_attempts_total",
		Help: "Delivery attempts by outcome",
	}, []string{"outcome"})

	DeadLetters = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_dead_letters_total",
		Help: "Webhooks moved to dead letter",
	})

	InFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "webhook_deliveries_in_flight",
		Help: "Deliveries currently leased",
	})
)
