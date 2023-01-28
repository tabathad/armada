package scheduleringester

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"time"

	"github.com/armadaproject/armada/internal/common/compress"

	"github.com/armadaproject/armada/internal/common/database"

	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/ingest"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Scheduler database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config Configuration) {
	svcMetrics := metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix + "armada_scheduler_ingester_")

	log.Infof("Opening connection pool to postgres")
	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		panic(errors.WithMessage(err, "Error opening connection to postgres"))
	}
	schedulerDb := NewSchedulerDb(db, svcMetrics, 100*time.Millisecond, 60*time.Second)

	// Discard submit job messages not intended for this scheduler.
	msgFilter := func(msg pulsar.Message) bool {
		return msg.Properties()[armadaevents.PULSAR_SCHEDULER_NAME] == "pulsar"
	}

	compressor, err := compress.NewZlibCompressor(1024)
	if err != nil {
		panic(errors.WithMessage(err, "Error creating  compressor"))
	}
	converter := NewInstructionConverter(svcMetrics, config.PriorityClasses, compressor)

	ingester := ingest.NewIngestionPipeline(
		config.Pulsar,
		msgFilter,
		config.SubscriptionName,
		config.BatchSize,
		config.BatchDuration,
		converter,
		schedulerDb,
		config.Metrics,
		svcMetrics)

	err = ingester.Run(app.CreateContextWithShutdown())
	if err != nil {
		panic(errors.WithMessage(err, "Error running ingestion pipeline"))
	}
}
