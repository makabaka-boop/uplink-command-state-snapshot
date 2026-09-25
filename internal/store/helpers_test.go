package store_test

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"deepspace/internal/store"
)

func errorIsNoRows(err error) bool {
	return errors.Is(err, store.ErrNoAvailableCommand)
}

func openPool(url string) (*pgxpool.Pool, error) {
	p, err := pgxpool.New(context.Background(), url)
	if err != nil {
		return nil, err
	}
	if err := p.Ping(context.Background()); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}
