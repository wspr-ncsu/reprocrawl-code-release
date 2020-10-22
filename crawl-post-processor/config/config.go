package config

import (
	"vpp/mongoz"
)

// HandlerFunction is the type of registered job handlers
type HandlerFunction func(c VppConfig) error

// VppConfig stores a vpc-post-processor task configuration (typically parsed from CLI args)
type VppConfig struct {
	Cmd     string                 // kind of post-processing (e.g., "urls" for testing URLs against blacklists/etc.)
	Handler HandlerFunction        // function that implements this command
	Args    []string               // any remaining, unparsed command-line args, for your parsing pleasure
	Mongo   mongoz.MongoConnection // source Mongo session/DB
	NoMongo bool                   // Suppress any connection to Mongo
}
