ARG BUILDER_IMAGE=golang:1.22-alpine3.20
ARG BASE_SERVER_IMAGE=temporalio/server:1.23.0
ARG BASE_AUTO_SETUP_IMAGE=temporalio/auto-setup:1.23.0

##### Builder #####
FROM ${BUILDER_IMAGE} as builder

WORKDIR /build

# build
COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /temporal-server ./server/main.go
RUN go install github.com/pressly/goose/v3/cmd/goose@v3.22.0

##### Temporal server #####
FROM ${BASE_SERVER_IMAGE} as temporal-server-ydb

WORKDIR /etc/temporal

# binaries
COPY --from=builder /temporal-server /usr/local/bin

# configs
COPY ./docker/config/config_template.yaml /etc/temporal/config/config_template.yaml


### Server auto-setup image ###
FROM ${BASE_AUTO_SETUP_IMAGE} as temporal-server-ydb-auto-setup

WORKDIR /etc/temporal

# binaries
# temporal-ydb binary
COPY --from=builder /temporal-server /usr/local/bin
# goose binary
COPY --from=builder /go/bin/goose /usr/local/bin

USER temporal

# configs
COPY ./docker/config/config_template.yaml /etc/temporal/config/config_template.yaml

# schema
COPY --chown=temporal:temporal ./schema /etc/temporal/schema/ydb

## scripts
COPY ./docker/auto-setup.sh /etc/temporal/auto-setup.sh
