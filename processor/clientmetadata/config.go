// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package clientmetadata

// Config for the clientmetadata processor.
type Config struct {
	// Extractions defines the list of resource attributes to copy into
	// client.Metadata on the pipeline context.
	Extractions []Extraction `mapstructure:"extractions"`
}

// Extraction maps a resource attribute to a client.Metadata key.
type Extraction struct {
	// Key is the metadata key name that will be set on client.Metadata.
	// This key is what downstream components (e.g., headers_setter with from_context)
	// will use to read the value.
	Key string `mapstructure:"key"`

	// FromResourceAttribute is the resource attribute name to read the value from.
	FromResourceAttribute string `mapstructure:"from_resource_attribute"`
}
