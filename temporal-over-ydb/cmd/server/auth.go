package main

import (
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

func GetAuthorizerFromConfig(cfg *config.Config, logger log.Logger) (authorization.Authorizer, error) {
	return authorization.GetAuthorizerFromConfig(
		&cfg.Global.Authorization,
	)
}

func GetClaimMapperFromConfig(cfg *config.Config, logger log.Logger) (authorization.ClaimMapper, error) {
	return authorization.GetClaimMapperFromConfig(&cfg.Global.Authorization, logger)
}
