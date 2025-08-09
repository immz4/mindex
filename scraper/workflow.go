package scraper

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type Robot struct {
	Text    string   `json:"text"`
	Sitemap []string `json:"sitemap"`
}

type GetEntityRobotsArgs struct {
	UploadID *string `json:"upload_id,omitempty"`
	EntityID string  `json:"entity_id"`
	Url      string  `json:"url"`
}

func GetEntityRobots(ctx workflow.Context, args GetEntityRobotsArgs) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			MaximumInterval:    time.Minute,
			BackoffCoefficient: 2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var scraperActivities *ScraperActivities

	var robots string
	err := workflow.ExecuteActivity(ctx, scraperActivities.GetRobots, fmt.Sprintf("%s/robots.txt", args.Url)).Get(ctx, &robots)
	if err != nil {
		return fmt.Errorf("Failed to get robots.txt: %s", err)
	}

	var uploadID string
	if args.UploadID == nil || *args.UploadID == "" {
		uploadID = uuid.New().String()
	} else {
		uploadID = *args.UploadID
	}

	err = workflow.ExecuteActivity(ctx, scraperActivities.SaveRobots, SaveRobotsArgs{
		UploadID: uuid.Must(uuid.Parse(uploadID)),
		EntityID: uuid.Must(uuid.Parse(args.EntityID)),
		Body:     robots,
	}).Get(ctx, nil)

	if err != nil {
		return fmt.Errorf("Failed to save robots.txt to table: %s", err)
	}

	return nil
}

type GetEntitySitemapArgs struct {
	UploadID *string `json:"upload_id,omitempty"`
	EntityID string  `json:"entity_id"`
	RobotsID string  `json:"robots_id"`
	OriginID *string `json:"origin_id,omitempty"`
	Url      string  `json:"url"`
}

func GetEntitySitemap(ctx workflow.Context, args GetEntitySitemapArgs) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			MaximumInterval:    time.Duration(2) * time.Minute,
			BackoffCoefficient: 2,
			MaximumAttempts:    20,
		},
	}

	ctx = workflow.WithActivityOptions(ctx, ao)

	var scraperActivities *ScraperActivities

	var sitemapRes SitemapRes
	err := workflow.ExecuteActivity(ctx, scraperActivities.GetSitemap, args.Url).Get(ctx, &sitemapRes)
	if err != nil {
		return fmt.Errorf("Failed to get sitemaps: %s", err)
	}

	var uploadID string
	if args.UploadID == nil || *args.UploadID == "" {
		uploadID = uuid.New().String()
	} else {
		uploadID = *args.UploadID
	}

	var originID *uuid.UUID
	if args.OriginID == nil {
		originID = nil
	} else {
		UUID := uuid.Must(uuid.Parse(*args.OriginID))
		originID = &UUID
	}

	data := SaveSitemapArgs{
		UploadID: uuid.Must(uuid.Parse(uploadID)),
		EntityID: uuid.Must(uuid.Parse(args.EntityID)),
		RobotsID: uuid.Must(uuid.Parse(args.RobotsID)),
		OriginID: originID,
		SaveID:   sitemapRes.SaveID,
	}

	if sitemapRes.Type == "index" {
		err = workflow.ExecuteActivity(ctx, scraperActivities.SaveSitemapIndex, data).Get(ctx, nil)

		if err != nil {
			return fmt.Errorf("Failed to save sitemap index to table: %s", err)
		}
	} else if sitemapRes.Type == "urlset" {
		err = workflow.ExecuteActivity(ctx, scraperActivities.SaveSitemapUrlset, data).Get(ctx, nil)

		if err != nil {
			return fmt.Errorf("Failed to save sitemap urlset to table: %s", err)
		}
	}

	return nil
}
