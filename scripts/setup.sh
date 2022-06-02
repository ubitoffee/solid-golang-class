#!/usr/bin/env bash
TAG=""
ALPINE=$( (cat /etc/os-release || echo "" | grep "ID=alpine") )
if [ -n "$ALPINE" ]; then
    TAG=" -tags musl "
fi

# sync module dependcies
echo "Getting vendor module dependencies..."
go mod init event-data-pipeline
go mod tidy
go mod vendor
echo "done"
echo

# build the binary
echo "Building the event-data-pipeline service..."
go build $TAG -mod=vendor -o ./bin/event-data-pipeline .
echo "done"
echo
echo "...complete."