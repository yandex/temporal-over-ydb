ARG BUILDER_IMAGE=golang:1.23-alpine3.20
ARG BASE_SERVER_IMAGE=temporalio/server:1.26.2
ARG BASE_AUTO_SETUP_IMAGE=temporalio/auto-setup:1.26.2

##### Builder #####
FROM ${BUILDER_IMAGE} AS builder

WORKDIR /build

# build
COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /ydb-migrator ./temporal-over-ydb/cmd/migrator
RUN CGO_ENABLED=0 GOOS=linux go build -o /temporal-server ./temporal-over-ydb/cmd/server

##### Temporal server #####
FROM ${BASE_SERVER_IMAGE} AS temporal-server-ydb

WORKDIR /etc/temporal

# binaries
COPY --from=builder /temporal-server /usr/local/bin

# configs
COPY ./docker/config/config_template.yaml /etc/temporal/config/config_template.yaml


### Server auto-setup image ###
FROM ${BASE_AUTO_SETUP_IMAGE} AS temporal-server-ydb-auto-setup

WORKDIR /etc/temporal

# binaries
# temporal-ydb binary
COPY --from=builder /temporal-server /usr/local/bin
# goose binary
COPY --from=builder /ydb-migrator /usr/local/bin

USER temporal

# configs
COPY ./docker/config/config_template.yaml /etc/temporal/config/config_template.yaml

# schema
COPY --chown=temporal:temporal ./schema /etc/temporal/schema/ydb

## scripts
COPY ./docker/auto-setup.sh /etc/temporal/auto-setup.sh
