package ytsaurus

import (
	"context"
	"danilov/pkg/models"
	"fmt"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

type YTAdapter struct {
	Client yt.Client
}

func New(proxy, token string) (*YTAdapter, error) {
	const op = "ytsaurus.New"
	client, err := ythttp.NewClient(&yt.Config{
		Proxy:                 proxy,
		Token:                 token,
		DisableProxyDiscovery: true,
		UseTLS:                true,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return &YTAdapter{Client: client}, nil
}

func (a *YTAdapter) GetNodeInfo(ctx context.Context, tablePath string) ([]models.SchemaDTO, error) {
	const op = "ytsaurus.GetNodeInfo"

	schemas := make([]models.SchemaDTO, 0)

	if err := a.Client.GetNode(ctx, ypath.Path(fmt.Sprintf("%s/@schema", tablePath)), &schemas, nil); err != nil {
		return schemas, fmt.Errorf("%s: %w", op, err)
	}

	return schemas, nil
}

func (a *YTAdapter) GetStartOffset(ctx context.Context, tablePath, stream string) (models.StreamDTO, error) {
	const op = "ytsaurus.GetStartOffset"

	resultDTO := models.StreamDTO{Stream: stream}
	offsetPath := ypath.Path(fmt.Sprintf("%s/@%s", tablePath, stream))

	if err := a.Client.GetNode(ctx, offsetPath, &resultDTO.OffsetDTO, nil); err != nil {
		var ok bool
		if ok, err = a.Client.NodeExists(ctx, offsetPath, nil); err != nil || !ok {
			resultDTO.OffsetDTO.Offset = 1
			if err = a.Client.SetNode(ctx, offsetPath, resultDTO.OffsetDTO, nil); err != nil {
				return resultDTO, fmt.Errorf("%s SetNode: %w", op, err)
			}
			return resultDTO, nil
		}
	}

	return resultDTO, nil
}

func SetStreamOffset(ctx context.Context, tx yt.Tx, tablePath, stream string, offset uint64) error {
	const op = "ytsaurus.SetStreamOffset"

	offsetPath := ypath.Path(fmt.Sprintf("%s/@%s/offset", tablePath, stream))
	if err := tx.SetNode(ctx, offsetPath, offset, nil); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}
