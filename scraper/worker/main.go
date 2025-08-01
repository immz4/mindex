package main

import (
	"database/sql"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/immz4/mindex/scraper"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"resty.dev/v3"
)

func main() {
	c, err := client.Dial(client.Options{
		HostPort: "127.0.0.1:7233",
	})
	if err != nil {
		log.Fatalln("Unable to create Temporal client", err)
	}
	defer c.Close()

	w := worker.New(c, scraper.ScraperQueueName, worker.Options{})

	httpClient := resty.New()
	defer httpClient.Close()

	chDb := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Username: "default",
		},
	})

	// password leak!! oh no...
	pgDb, err := sql.Open("pgx", "postgres://immz:dev123@localhost:5432/mindex")

	if err != nil {
		log.Fatalln("Unable to open PG connection", err)
	}

	activities := &scraper.ScraperActivities{
		UserAgent:  "MindexBot",
		HTTPClient: httpClient,
		CHClient:   chDb,
		PGClient:   pgDb,
	}

	w.RegisterWorkflow(scraper.GetEntityRobots)
	w.RegisterWorkflow(scraper.GetEntitySitemap)
	w.RegisterActivity(activities)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start Temporal worker", err)
	}
}
