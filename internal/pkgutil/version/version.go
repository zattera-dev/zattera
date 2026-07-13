// Package version carries the build version, stamped via -ldflags.
package version

// Version is set at build time: -X .../version.Version=v0.x.y
var Version = "dev"
