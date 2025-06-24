package models

import (
	"time"
)

// SeedStatus represents the processed seed status record for database storage
type SeedStatus struct {
	ID              int64     `json:"id" db:"id"`
	EventTimestamp  time.Time `json:"event_timestamp" db:"event_timestamp"`
	ResponseMessage *string   `json:"response_message" db:"response_message"`
	Status          string    `json:"status" db:"status"`
	SeedID          *int32    `json:"seed_id" db:"seed_id"`
	SubScenarioID   *int32    `json:"sub_scenario_id" db:"sub_scenario_id"`
	ScenarioID      *int32    `json:"scenario_id" db:"scenario_id"`
	ScriptID        *int32    `json:"script_id" db:"script_id"`
}

// SeedStatusAvro represents the Avro message structure
type SeedStatusAvro struct {
	DateTimestamp   int64   `json:"dateTimestamp"`
	Response        *string `json:"response"`
	Status          *string `json:"status"`
	SeedID          *int32  `json:"seedId"`
	SubScenarioID   *int32  `json:"subScenarioId"`
	ScenarioID      *int32  `json:"scenarioId"`
	ScriptID        *int32  `json:"scriptId"`
}

// ToSeedStatus converts SeedStatusAvro to SeedStatus for database storage
func (s *SeedStatusAvro) ToSeedStatus() *SeedStatus {
	status := ""
	if s.Status != nil {
		status = *s.Status
	}

	return &SeedStatus{
		EventTimestamp:  time.Unix(s.DateTimestamp/1000, (s.DateTimestamp%1000)*1000000), // Convert milliseconds to time.Time
		ResponseMessage: s.Response,
		Status:          status,
		SeedID:          s.SeedID,
		SubScenarioID:   s.SubScenarioID,
		ScenarioID:      s.ScenarioID,
		ScriptID:        s.ScriptID,
	}
}