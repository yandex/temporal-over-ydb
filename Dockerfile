ARG BUILDER_IMAGE=golang:1.21
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
# ydb-cli binary
USER root
RUN apk add --no-cache gcompat
RUN curl -sSL "https://storage.yandexcloud.net/yandexcloud-ydb/release/2.7.0/linux/amd64/ydb" -o /ydb && chmod +x /ydb
USER temporal

# configs
COPY ./docker/config/config_template.yaml /etc/temporal/config/config_template.yaml

# schema
COPY ./schema /etc/temporal/schema/ydb

## scripts
COPY ./docker/auto-setup.sh /etc/temporal/auto-setup.sh
