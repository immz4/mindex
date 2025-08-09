package scraper

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/google/uuid"
	sitemap "github.com/oxffaa/gopher-parse-sitemap"
	"github.com/redis/go-redis/v9"
	"resty.dev/v3"

	model "github.com/immz4/mindex/scraper/.gen/mindex/public/model"
	. "github.com/immz4/mindex/scraper/.gen/mindex/public/table"
)

type HTTPGetter interface {
	R() *resty.Request
}

type ScraperActivities struct {
	UserAgent   string
	HTTPClient  HTTPGetter
	CHClient    *sql.DB
	PGClient    *sql.DB
	RedisClient *redis.Client
}

type SaveRobotsArgs struct {
	UploadID uuid.UUID `json:"upload_id"`
	EntityID uuid.UUID `json:"entity_id"`
	Body     string    `json:"body"`
}

func (sa *ScraperActivities) SaveRobots(ctx context.Context, args SaveRobotsArgs) error {
	_, err := Robots.INSERT(Robots.EntityID, Robots.UploadID, Robots.Data, Robots.Scraped).
		MODEL(model.Robots{
			EntityID: args.EntityID,
			UploadID: args.UploadID,
			Data:     args.Body,
			Scraped:  false,
		}).
		ExecContext(ctx, sa.PGClient)

	if err != nil {
		return err
	}

	return nil
}

type SaveSitemapArgs struct {
	UploadID uuid.UUID  `json:"upload_id"`
	EntityID uuid.UUID  `json:"entity_id"`
	RobotsID uuid.UUID  `json:"robots_id"`
	OriginID *uuid.UUID `json:"origin_id"`
	SaveID   string     `json:"save_id"`
}

func (sa *ScraperActivities) SaveSitemapIndex(ctx context.Context, args SaveSitemapArgs) error {
	data, err := sa.RedisClient.Get(ctx, args.SaveID).Bytes()

	if err != nil {
		return fmt.Errorf("Failed to get sitemap data: %s", err)
	}

	var sitemapIndex SitemapIndexParsed
	err = json.Unmarshal(data, &sitemapIndex)

	if err != nil {
		return fmt.Errorf("Failed to parse sitemap data: %s", err)
	}

	tx, err := sa.PGClient.BeginTx(ctx, &sql.TxOptions{})

	if err != nil {
		return fmt.Errorf("Failed to start DB transaction: %s", err)
	}

	insertModels := make([]model.SitemapIndex, 0, len(sitemapIndex.Index))

	for _, record := range sitemapIndex.Index {
		insertModels = append(insertModels, model.SitemapIndex{
			EntityID:     args.EntityID,
			UploadID:     args.UploadID,
			RobotsID:     args.RobotsID,
			OriginID:     args.OriginID,
			URL:          record.Location,
			LastModified: time.UnixMilli(*record.LastModified),
			Scraped:      false,
		})
	}

	for batch := range slices.Chunk(insertModels, 5000) {
		_, err = SitemapIndex.INSERT(
			SitemapIndex.EntityID,
			SitemapIndex.UploadID,
			SitemapIndex.RobotsID,
			SitemapIndex.OriginID,
			SitemapIndex.URL,
			SitemapIndex.LastModified,
			SitemapIndex.Scraped,
		).
			MODELS(batch).
			ExecContext(ctx, sa.PGClient)

		if err != nil {
			errRollback := tx.Rollback()

			if errRollback != nil {
				return fmt.Errorf("Failed to rollback transaction: %s", err)
			}

			return fmt.Errorf("Failed to save urlset: %s", err)
		}
	}

	err = tx.Commit()

	if err != nil {
		return fmt.Errorf("Failed to commit transaction: %s", err)
	}

	return nil
}

func (sa *ScraperActivities) SaveSitemapUrlset(ctx context.Context, args SaveSitemapArgs) error {
	data, err := sa.RedisClient.Get(ctx, args.SaveID).Bytes()

	if err != nil {
		return fmt.Errorf("Failed to get sitemap data: %s", err)
	}

	var sitemapUrlset SitemapUrlsetParsed
	err = json.Unmarshal(data, &sitemapUrlset)

	if err != nil {
		return fmt.Errorf("Failed to parse sitemap data: %s", err)
	}

	tx, err := sa.PGClient.BeginTx(ctx, &sql.TxOptions{})

	if err != nil {
		return fmt.Errorf("Failed to start DB transaction: %s", err)
	}

	insertModels := make([]model.SitemapUrlset, 0, len(sitemapUrlset.Urlset))

	for _, record := range sitemapUrlset.Urlset {
		insertModels = append(insertModels, model.SitemapUrlset{
			EntityID:     args.EntityID,
			UploadID:     args.UploadID,
			RobotsID:     args.RobotsID,
			OriginID:     args.OriginID,
			URL:          record.Location,
			LastModified: time.UnixMilli(*record.LastModified),
			ChangeFreq:   &record.ChangeFrequency,
			Scraped:      false,
		})
	}

	for batch := range slices.Chunk(insertModels, 5000) {
		_, err = SitemapUrlset.INSERT(
			SitemapUrlset.EntityID,
			SitemapUrlset.UploadID,
			SitemapUrlset.RobotsID,
			SitemapUrlset.OriginID,
			SitemapUrlset.URL,
			SitemapUrlset.LastModified,
			SitemapUrlset.Scraped,
		).
			MODELS(batch).
			ExecContext(ctx, sa.PGClient)

		if err != nil {
			errRollback := tx.Rollback()

			if errRollback != nil {
				return fmt.Errorf("Failed to rollback transaction: %s", err)
			}

			return fmt.Errorf("Failed to save urlset: %s", err)
		}
	}

	err = tx.Commit()

	if err != nil {
		return fmt.Errorf("Failed to commit transaction: %s", err)
	}

	return nil
}

func (sa *ScraperActivities) GetRobots(ctx context.Context, url string) (string, error) {
	resp, err := sa.HTTPClient.R().
		SetHeader("User-Agent", sa.UserAgent).
		SetHeader("Accept", "text/plain").
		SetHeader("Accept-Encoding", "gzip, deflate, br, zstd").
		SetHeader("Set-Fetch-Dest", "document").
		SetHeader("Set-Fetch-Mode", "navigate").
		SetHeader("Set-Fetch-User", "?1").
		Get(url)

	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	robotsBody := string(resp.Bytes())

	return robotsBody, nil
}

type SitemapResUrlset struct {
	Location        string `json:"location"`
	LastModified    *int64 `json:"last_modified,omitempty"`
	ChangeFrequency string `json:"change_frequency"`
}

type SitemapResIndex struct {
	Location     string `json:"location"`
	LastModified *int64 `json:"last_modified,omitempty"`
}

type SitemapUrlsetParsed struct {
	Urlset []SitemapResUrlset `json:"urlset"`
}

type SitemapIndexParsed struct {
	Index []SitemapResIndex `json:"index"`
}

type SitemapRes struct {
	Type   string `json:"type"`
	SaveID string `json:"save_id"`
}

// TODO: Should we split result save into separate activity or just do it in one swoop?
// Currently we save JSON to the Redis, which is fetched in save activities.
func (sa *ScraperActivities) GetSitemap(ctx context.Context, url string) (*SitemapRes, error) {
	sitemapRes, err := sa.HTTPClient.R().
		SetHeader("User-Agent", sa.UserAgent).
		SetHeader("Accept", "application/xml").
		SetHeader("Accept-Encoding", "gzip, deflate, br, zstd").
		SetHeader("Set-Fetch-Dest", "document").
		SetHeader("Set-Fetch-Mode", "navigate").
		SetHeader("Set-Fetch-User", "?1").
		Get(url)

	if err != nil {
		return nil, err
	}

	bodyReader := sitemapRes.Body
	defer bodyReader.Close()

	var sitemapBuffer bytes.Buffer
	teeReader := io.TeeReader(bodyReader, &sitemapBuffer)

	sitemapUrlsetParsed := SitemapUrlsetParsed{
		Urlset: make([]SitemapResUrlset, 0, 500),
	}

	sitemapIndexParsed := SitemapIndexParsed{
		Index: make([]SitemapResIndex, 0, 500),
	}

	urlsetErr := sitemap.Parse(teeReader, func(e sitemap.Entry) error {
		lastModified := e.GetLastModified().UnixMilli()
		sitemapUrlsetParsed.Urlset = append(sitemapUrlsetParsed.Urlset, SitemapResUrlset{
			Location:        e.GetLocation(),
			LastModified:    &lastModified,
			ChangeFrequency: e.GetChangeFrequency(),
		})

		return nil
	})

	if urlsetErr != nil {
		return nil, urlsetErr
	}

	indexErr := sitemap.ParseIndex(&sitemapBuffer, func(e sitemap.IndexEntry) error {
		lastModified := e.GetLastModified().UnixMilli()
		sitemapIndexParsed.Index = append(sitemapIndexParsed.Index, SitemapResIndex{
			Location:     e.GetLocation(),
			LastModified: &lastModified,
		})

		return nil
	})

	if indexErr != nil {
		return nil, indexErr
	}

	var sitemapType string
	var resData []byte
	saveID := uuid.New().String()

	if len(sitemapIndexParsed.Index) > 0 {
		sitemapType = "index"
		resData, err = json.Marshal(sitemapIndexParsed)

		if err != nil {
			return nil, err
		}

		sa.RedisClient.Set(ctx, saveID, resData, time.Duration(10)*time.Minute)
	} else if len(sitemapUrlsetParsed.Urlset) > 0 {
		sitemapType = "urlset"
		resData, err = json.Marshal(sitemapUrlsetParsed)

		if err != nil {
			return nil, err
		}

		sa.RedisClient.Set(ctx, saveID, resData, time.Duration(10)*time.Minute)
	} else {
		sitemapType = "empty"
	}

	return &SitemapRes{
		Type:   sitemapType,
		SaveID: saveID,
	}, nil
}
