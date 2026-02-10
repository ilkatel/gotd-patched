package telegram

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/gotd/td/tg"
)

// DCClient represents a connection to a specific datacenter with auth transfer.
// DCClient connections are pooled and reused by the Client, so Close() should NOT be called.
type DCClient struct {
	invoker CloseInvoker
	api     *tg.Client
	dc      int
}

// API returns tg.Client for the datacenter.
func (d *DCClient) API() *tg.Client {
	return d.api
}

// DC returns the datacenter ID.
func (d *DCClient) DC() int {
	return d.dc
}

// GetDCClient creates or reuses a client for the specified datacenter with auth transfer.
// This method reuses the same connection pool (c.subConns) as automatic DC migration,
// ensuring efficient connection management.
//
// This is useful for manual DC migration when DisableAutoMigration is enabled.
//
// Example usage with manual migration:
//
//	// Enable manual migration mode
//	client := telegram.NewClient(appID, appHash, telegram.Options{
//		DisableAutoMigration: true,
//		// ... other options
//	})
//
//	// Try to get stats
//	stats, err := client.API().StatsGetBroadcastStats(ctx, req)
//	if err != nil {
//		// Check for STATS_MIGRATE error
//		if rpcErr, ok := tgerr.As(err); ok && rpcErr.Type == "STATS_MIGRATE" {
//			targetDC := rpcErr.Argument
//
//			// Create client for target DC (reuses existing connection if available)
//			dcClient, err := client.GetDCClient(ctx, targetDC)
//			if err != nil {
//				return err
//			}
//
//			// Retry request on target DC
//			stats, err = dcClient.API().StatsGetBroadcastStats(ctx, req)
//
//			// Load async graphs using the same DC client
//			if asyncGraph, ok := stats.LanguagesGraph.(*tg.StatsGraphAsync); ok {
//				graph, _ := dcClient.API().StatsLoadAsyncGraph(ctx, &tg.StatsLoadAsyncGraphRequest{
//					Token: asyncGraph.Token,
//				})
//				_ = graph
//			}
//		}
//	}
func (c *Client) GetDCClient(ctx context.Context, dcID int) (*DCClient, error) {
	// Check if connection already exists in cache (same pool as invokeSub)
	c.subConnsMux.Lock()
	if conn, ok := c.subConns[dcID]; ok {
		c.subConnsMux.Unlock()

		// Reuse existing connection from cache
		return &DCClient{
			invoker: conn,
			api:     tg.NewClient(conn),
			dc:      dcID,
		}, nil
	}

	// Create new connection with auth transfer
	conn, err := c.dc(ctx, dcID, 1, c.primaryDC(dcID))
	if err != nil {
		c.subConnsMux.Unlock()
		return nil, errors.Wrapf(err, "create DC %d connection", dcID)
	}

	// Save to cache (same as invokeSub does)
	c.subConns[dcID] = conn
	c.subConnsMux.Unlock()

	return &DCClient{
		invoker: conn,
		api:     tg.NewClient(conn),
		dc:      dcID,
	}, nil
}
