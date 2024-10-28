package nats

import (
	"context"
	"danilov/internal/hell"
	"danilov/pkg/ytsaurus"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.ytsaurus.tech/yt/go/ypath"
	"log/slog"
	"reflect"
	"time"
)

const batchSize = 10

type Broker interface {
	SubStream(ctx context.Context, stream, consName, tablePath string, done chan struct{}, ytAdapter *ytsaurus.YTAdapter)
}

type CurrentBroker struct {
	nc *nats.Conn
	js jetstream.JetStream
}

func (b *CurrentBroker) SubStream(ctx context.Context, stream, consName, tablePath string, done chan struct{}, ytAdapter *ytsaurus.YTAdapter) {
	defer func() {
		done <- struct{}{}
		close(done)
	}()

	const op = "nats.Substream1"
	log, ok := ctx.Value("log").(*slog.Logger)
	if !ok {
		return
	}

	streamDTO, err := ytAdapter.GetStartOffset(ctx, tablePath, stream)
	if err != nil {
		log.Error(
			"failed to get start offset",
			slog.String("op", op),
			slog.String("error", err.Error()),
		)
		return
	}
	log.Info("Getting offset", slog.String("op", op), slog.Uint64("offset", streamDTO.Offset))

	// +1 because of duplicates
	consumer, err := b.js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckAllPolicy,
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   streamDTO.Offset + 1,
	})
	if err != nil {
		log.Error("failed to create consumer",
			slog.String("op", op),
			slog.String("error", err.Error()),
		)
		return
	}

	schemaDTOs, err := ytAdapter.GetNodeInfo(ctx, tablePath)
	if err != nil {
		log.Error("failed to get node info", slog.String("error", err.Error()))
		return
	}
	log.Info("node info was received", slog.String("op", op))

	dataStruct := hell.BornShit(schemaDTOs)

	if err = handleBatches(ctx, consumer, log, ytAdapter, stream, tablePath, dataStruct); err != nil {
		log.Error("failed to handle batches", slog.String("error", err.Error()))
		return
	}
}

func handleBatches(ctx context.Context, consumer jetstream.Consumer, log *slog.Logger, ytAdapter *ytsaurus.YTAdapter,
	stream, tablePath string, dataStruct reflect.Type) error {

	const op = "nats.ytConsumer.handleBatches"
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			batch, err := consumer.Fetch(batchSize)
			if err != nil {
				return fmt.Errorf("%s: %w", op, err)
			}

			msgs := make(chan jetstream.Msg, batchSize)

			tryToReadBatch(ctx, batch.Messages(), msgs)

			if len(msgs) < 1 {
				continue
			}
			log.Info("new messages are available")

			if err = parseAndWriteToYT(ctx, msgs, ytAdapter, stream, tablePath, dataStruct); err != nil {
				return fmt.Errorf("%s: %w", op, err)
			}
		}
	}
}

func tryToReadBatch(ctx context.Context, batch <-chan jetstream.Msg, msgs chan<- jetstream.Msg) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		close(msgs)
		return
	case msg := <-batch:
		msgs <- msg
	}
}

func parseAndWriteToYT(ctx context.Context, messages <-chan jetstream.Msg,
	ytAdapter *ytsaurus.YTAdapter, stream, tablePath string, dataStruct reflect.Type) error {

	const op = "nats.parseAndWriteToYT"

	dataStructInstance := reflect.New(dataStruct).Interface()

	txCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := ytAdapter.Client.BeginTx(txCtx, nil)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	writer, err := tx.WriteTable(ctx, ypath.Path(fmt.Sprintf("<append=true>%s", tablePath)), nil)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	for msg := range messages {
		if err = json.Unmarshal(msg.Data(), &dataStructInstance); err != nil {
			if txErr := tx.Abort(); txErr != nil {
				panic(fmt.Sprintf("FAILED TO ABORT TRANSACTION: %s", txErr.Error()))
			}
			return fmt.Errorf("%s json.Unmarshal: %w", op, err)
		}

		if err = writer.Write(dataStructInstance); err != nil {
			if txErr := tx.Abort(); txErr != nil {
				panic(fmt.Sprintf("FAILED TO ABORT TRANSACTION: %s", txErr.Error()))
			}
			return fmt.Errorf("%s writer.Write: %w", op, err)
		}

		if len(messages) == 0 {
			if err = writer.Commit(); err != nil {
				fmt.Println(err)
				if txErr := tx.Abort(); txErr != nil {
					panic(fmt.Sprintf("FAILED TO ABORT TRANSACTION: %s", txErr.Error()))
				}
				return fmt.Errorf("%s writer.Commit: %w", op, err)
			}

			var metadata *jetstream.MsgMetadata
			metadata, err = msg.Metadata()
			if err != nil {
				if txErr := tx.Abort(); txErr != nil {
					panic(fmt.Sprintf("FAILED TO ABORT TRANSACTION: %s", txErr.Error()))
				}
				return fmt.Errorf("%s: %w", op, err)
			}

			if err = ytsaurus.SetStreamOffset(ctx, tx, tablePath, stream, metadata.Sequence.Stream); err != nil {
				if txErr := tx.Abort(); txErr != nil {
					panic(fmt.Sprintf("FAILED TO ABORT TRANSACTION: %s", txErr.Error()))
				}
				return fmt.Errorf("%s: %w", op, err)
			}

			if err = tx.Commit(); err != nil {
				return fmt.Errorf("%s tx.Commit(): %w", op, err)
			}

			if err = msg.Ack(); err != nil {
				return fmt.Errorf("%s msg.Ack: %w", op, err)
			}

			return nil
		}
	}
	return nil
}

func New(address string) (Broker, error) {
	nc, err := nats.Connect(address)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return &CurrentBroker{nc: nc, js: js}, nil
}
