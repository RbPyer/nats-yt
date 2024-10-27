package getAttr_test

import (
	"context"
	"danilov/internal/config"
	"danilov/pkg/ytsaurus"
	"testing"
)

func TestGetAttr(t *testing.T) {
	ctx := context.Background()
	cfg := config.MustLoad("../../configs/local.yaml")
	adapter, err := ytsaurus.New(cfg.Proxy, cfg.Token)
	if err != nil {
		t.Fatal(err)
	}

	dto, err := adapter.CheckOffset(ctx, "//sandbox/oswyndel/test/@offset")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(dto)

}
