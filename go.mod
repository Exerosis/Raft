module github.com/exerosis/raft

go 1.19

replace github.com/exerosis/RabiaGo => ../RabiaGo

require (
	github.com/better-concurrent/guc v0.0.0-20190520022744-eb29266403a1
	github.com/cockroachdb/datadriven v1.0.2
	github.com/exerosis/RabiaGo v0.0.0-20230127121507-d215c8b12cba
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.3
	github.com/stretchr/testify v1.8.1
)

require (
	github.com/BertoldVdb/go-misc v0.1.8 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	golang.org/x/sys v0.14.0 // indirect
	google.golang.org/protobuf v1.32.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
