package avro

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/linkedin/goavro/v2"
)

// Deserializer handles Avro deserialization
type Deserializer struct {
	codec *goavro.Codec
}

// NewDeserializer creates a new Avro deserializer with the schema
func NewDeserializer(schemaPath string) (*Deserializer, error) {
	schemaBytes, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	codec, err := goavro.NewCodec(string(schemaBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro codec: %w", err)
	}

	return &Deserializer{
		codec: codec,
	}, nil
}

// Deserialize converts Avro binary data to Go map
func (d *Deserializer) Deserialize(data []byte) (map[string]interface{}, error) {
	native, _, err := d.codec.NativeFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize Avro binary: %w", err)
	}

	result, ok := native.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("deserialized data is not a map")
	}

	return result, nil
}

// DeserializeToJSON converts Avro binary data to JSON
func (d *Deserializer) DeserializeToJSON(data []byte) ([]byte, error) {
	native, err := d.Deserialize(data)
	if err != nil {
		return nil, err
	}

	jsonBytes, err := json.Marshal(native)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal to JSON: %w", err)
	}

	return jsonBytes, nil
}

// Serializer handles Avro serialization
type Serializer struct {
	codec *goavro.Codec
}

// NewSerializer creates a new Avro serializer with the schema
func NewSerializer(schemaPath string) (*Serializer, error) {
	schemaBytes, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	codec, err := goavro.NewCodec(string(schemaBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro codec: %w", err)
	}

	return &Serializer{
		codec: codec,
	}, nil
}

// Serialize converts Go map to Avro binary data
func (s *Serializer) Serialize(data map[string]interface{}) ([]byte, error) {
	binary, err := s.codec.BinaryFromNative(nil, data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize to Avro binary: %w", err)
	}

	return binary, nil
}

// SerializeFromJSON converts JSON to Avro binary data
func (s *Serializer) SerializeFromJSON(jsonData []byte) ([]byte, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return s.Serialize(data)
}