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
	TestConsumer(ctx context.Context, stream string) error
}

type CurrentBroker struct {
	nc *nats.Conn
}

func (b *CurrentBroker) TestConsumer(ctx context.Context, stream string) error {
	const op = "nats.TestConsumer"

	js, err := jetstream.New(b.nc)
	if err != nil {
		return err
	}

	consumer, err := js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckAllPolicy,
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   1,
	})

	if err != nil {
		return err
	}
	var (
		consumerInfo *jetstream.ConsumerInfo
		batch        jetstream.MessageBatch
	)

	for {
		consumerInfo, err = consumer.Info(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("Stream seq: %d\n", consumerInfo.Delivered.Stream)

		batch, err = consumer.Fetch(batchSize, jetstream.FetchMaxWait(1*time.Second))
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}

		for msg := range batch.Messages() {
			fmt.Printf("New message: %s\n", string(msg.Data()))
			err = msg.Ack()
			if err != nil {
				return fmt.Errorf("%s: %w", op, err)
			}
		}
	}

}

func (b *CurrentBroker) SubStream(ctx context.Context, stream, consName, tablePath string, done chan struct{}, ytAdapter *ytsaurus.YTAdapter) {
	defer func() {
		done <- struct{}{}
		close(done)
	}()
	const op = "nats.Substream"
	var err error
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

	js, err := jetstream.New(b.nc)
	if err != nil {
		log.Error("failed to create Jetstream instance",
			slog.String("op", op),
			slog.String("error", err.Error()),
		)
		return
	}

	consumer, err := js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckAllPolicy,
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   streamDTO.Offset,
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
	var (
		consumerInfo *jetstream.ConsumerInfo
		batch        jetstream.MessageBatch
	)
	_ = consumerInfo

	for {
		consumerInfo, err = consumer.Info(ctx)
		if err != nil {
			log.Error("failed to get consumer info",
				slog.String("op", op),
				slog.String("error", err.Error()),
			)
			return
		}
		log.Info("consumer info was received", slog.String("op", op),
			slog.Uint64("stream seq", consumerInfo.Delivered.Stream))

		batch, err = consumer.Fetch(batchSize, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			log.Error("failed to fetch batch", slog.String("op", op), slog.String("error", err.Error()))
			return
		}
		time.Sleep(5 * time.Second)

		messages := batch.Messages()
		if len(messages) == 0 {
			continue
		}

		if err = parseAndWriteToYT(ctx, messages, log, ytAdapter, stream, tablePath, dataStruct); err != nil {
			log.Error("failed to parse and write to YT",
				slog.String("op", op),
				slog.String("error", err.Error()),
			)
			return
		}
	}
}

func parseAndWriteToYT(ctx context.Context, messages <-chan jetstream.Msg, log *slog.Logger,
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

	log.Info("received new messages", slog.String("op", op), slog.Int("current batch size", len(messages)))
	for msg := range messages {
		if err = json.Unmarshal(msg.Data(), &dataStructInstance); err != nil {
			log.Error("failed to unmarshal data")
			if txErr := tx.Abort(); txErr != nil {
				panic(fmt.Sprintf("FAILED TO ABORT TRANSACTION: %s", txErr.Error()))
			}
			return fmt.Errorf("%s json.Unmarshal: %w", op, err)
		}

		if err = writer.Write(dataStructInstance); err != nil {
			log.Error("failed to write data")
			if txErr := tx.Abort(); txErr != nil {
				panic(fmt.Sprintf("FAILED TO ABORT TRANSACTION: %s", txErr.Error()))
			}
			return fmt.Errorf("%s writer.Write: %w", op, err)
		}

		if len(messages) == 0 {
			log.Info("data", slog.String("op", op), slog.Any("data", dataStructInstance))
			log.Info("tablepath", slog.String("tablepath", tablePath))
			if err = writer.Commit(); err != nil {
				log.Error("failed to commit data in writer", slog.String("op", op), slog.String("error", err.Error()))
				if txErr := tx.Abort(); txErr != nil {
					panic(fmt.Sprintf("FAILED TO ABORT TRANSACTION: %s", txErr.Error()))
				}
				return fmt.Errorf("%s writer.Commit: %w", op, err)
			}

			var metadata *jetstream.MsgMetadata
			metadata, err = msg.Metadata()
			if err != nil {
				log.Error("failed to get message metadata", slog.String("op", op), slog.String("error", err.Error()))
				if txErr := tx.Abort(); txErr != nil {
					panic(fmt.Sprintf("FAILED TO ABORT TRANSACTION: %s", txErr.Error()))
				}
				return fmt.Errorf("%s: %w", op, err)
			}

			if err = ytsaurus.SetStreamOffset(ctx, tx, tablePath, stream, metadata.Sequence.Stream); err != nil {
				log.Error("failed to set offset", slog.String("op", op), slog.String("error", err.Error()))
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
	return &CurrentBroker{nc: nc}, nil
}
