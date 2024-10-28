package writeTable

import (
	"context"
	"danilov/internal/config"
	"danilov/internal/hell"
	"danilov/pkg/ytsaurus"
	"encoding/json"
	"fmt"
	"go.ytsaurus.tech/yt/go/ypath"
	"reflect"
	"testing"
)

func TestWriteTable(t *testing.T) {
	ctx := context.Background()
	cfg := config.MustLoad("../../configs/local.yaml")
	adapter, err := ytsaurus.New(cfg.Proxy, cfg.Token)
	if err != nil {
		t.Fatal(err)
	}

	testData := []byte("{\"id\": 1, \"value\": 1}")

	dtos, err := adapter.GetNodeInfo(ctx, fmt.Sprintf("%s/@schema", cfg.TablePath))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(dtos)

	shitStruct := hell.BornShit(dtos)
	shitInstance := reflect.New(shitStruct).Interface()

	writer, err := adapter.Client.WriteTable(ctx, ypath.Path(cfg.TablePath), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = json.Unmarshal(testData, shitInstance); err != nil {
		t.Fatal(err)
	}

	if err = writer.Write(shitInstance); err != nil {
		t.Fatal(err)
	}
	if err = writer.Commit(); err != nil {
		t.Fatal(err)
	}
}
