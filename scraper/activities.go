package scraper

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log"
	"time"

	"github.com/google/uuid"
	sitemap "github.com/oxffaa/gopher-parse-sitemap"
	"resty.dev/v3"

	model "github.com/immz4/mindex/scraper/.gen/mindex/public/model"
	. "github.com/immz4/mindex/scraper/.gen/mindex/public/table"
)

type HTTPGetter interface {
	R() *resty.Request
}

type ScraperActivities struct {
	UserAgent  string
	HTTPClient HTTPGetter
	CHClient   *sql.DB
	PGClient   *sql.DB
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

type SaveSitemapIndexArgs struct {
	UploadID     uuid.UUID  `json:"upload_id"`
	EntityID     uuid.UUID  `json:"entity_id"`
	RobotsID     uuid.UUID  `json:"robots_id"`
	OriginID     *uuid.UUID `json:"origin_id"`
	Url          string     `json:"url"`
	LastModified time.Time  `json:"last_modified"`
}

func (sa *ScraperActivities) SaveSitemapIndex(ctx context.Context, args []SaveSitemapIndexArgs) error {
	tx, err := sa.PGClient.BeginTx(ctx, &sql.TxOptions{})

	if err != nil {
		return err
	}

	for _, arg := range args {
		_, err := SitemapIndex.INSERT(SitemapIndex.EntityID, SitemapIndex.UploadID, SitemapIndex.RobotsID, SitemapIndex.OriginID, SitemapIndex.URL, SitemapIndex.LastModified, SitemapIndex.Scraped).
			MODEL(model.SitemapIndex{
				EntityID:     arg.EntityID,
				UploadID:     arg.UploadID,
				RobotsID:     arg.RobotsID,
				OriginID:     arg.OriginID,
				URL:          arg.Url,
				LastModified: arg.LastModified,
				Scraped:      false,
			}).
			ExecContext(ctx, tx)

		if err != nil {
			err = tx.Rollback()

			if err != nil {
				return err
			}

			return err
		}
	}

	err = tx.Commit()

	if err != nil {
		return err
	}

	return nil
}

type SaveSitemapUrlsetArgs struct {
	UploadID     uuid.UUID  `json:"upload_id"`
	EntityID     uuid.UUID  `json:"entity_id"`
	RobotsID     uuid.UUID  `json:"robots_id"`
	OriginID     *uuid.UUID `json:"origin_id"`
	Url          string     `json:"url"`
	LastModified time.Time  `json:"last_modified"`
	ChangeFreq   *string    `json:"change_freq"`
}

func (sa *ScraperActivities) SaveSitemapUrlset(ctx context.Context, args SaveSitemapUrlsetArgs) error {
	_, err := SitemapIndex.INSERT(SitemapIndex.EntityID, SitemapIndex.UploadID, SitemapIndex.RobotsID, SitemapIndex.OriginID, SitemapIndex.URL, SitemapIndex.LastModified, SitemapIndex.Scraped).
		MODEL(model.SitemapUrlset{
			EntityID:     args.EntityID,
			UploadID:     args.UploadID,
			RobotsID:     args.RobotsID,
			OriginID:     args.OriginID,
			URL:          args.Url,
			LastModified: args.LastModified,
			ChangeFreq:   args.ChangeFreq,
			Scraped:      false,
		}).
		ExecContext(ctx, sa.PGClient)

	if err != nil {
		return err
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

type SitemapRes struct {
	Urlset []SitemapResUrlset `json:"urlset"`
	Index  []SitemapResIndex  `json:"index"`
}

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

	res := SitemapRes{
		Urlset: make([]SitemapResUrlset, 0, 500),
		Index:  make([]SitemapResIndex, 0, 500),
	}

	urlsetErr := sitemap.Parse(teeReader, func(e sitemap.Entry) error {
		lastModified := e.GetLastModified().UnixMilli()
		res.Urlset = append(res.Urlset, SitemapResUrlset{
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
		res.Index = append(res.Index, SitemapResIndex{
			Location:     e.GetLocation(),
			LastModified: &lastModified,
		})

		return nil
	})

	if indexErr != nil {
		return nil, indexErr
	}

	log.Println(res)
	return &res, nil
}
