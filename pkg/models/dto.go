package models

type ConsumerDTO struct {
	Id    uint64 `yson:"id"`
	Value uint64 `yson:"value"`
}

type SchemaDTO struct {
	Name string `yson:"name"`
	Type string `yson:"type"`
}

type StreamDTO struct {
	Stream string `yson:"stream"`
	OffsetDTO
}

type OffsetDTO struct {
	Offset uint64 `yson:"offset"`
}
