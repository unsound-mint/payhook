// Package config reads settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr       string
	DatabaseURL    string
	NATSURL        string
	WebhookSecrets map[string][]byte // PAYHOOK_WEBHOOK_SECRETS="halyk=...,kaspi=..."
	CallbackSecret []byte            // signs our callbacks to merchants
	// AllowPrivateCallbacks permits http and internal addresses as callback_url.
	// For local runs and tests only: in production it opens the worker to SSRF.
	AllowPrivateCallbacks bool // PAYHOOK_ALLOW_PRIVATE_CALLBACKS=true
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		NATSURL:     env("NATS_URL", "nats://localhost:4222"),
	}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("DATABASE_URL is required")
	}
	c.WebhookSecrets = map[string][]byte{}
	for _, pair := range strings.Split(os.Getenv("PAYHOOK_WEBHOOK_SECRETS"), ",") {
		name, secret, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if ok && name != "" && secret != "" {
			c.WebhookSecrets[name] = []byte(secret)
		}
	}
	c.CallbackSecret = []byte(os.Getenv("PAYHOOK_CALLBACK_SECRET"))
	if v := os.Getenv("PAYHOOK_ALLOW_PRIVATE_CALLBACKS"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return c, fmt.Errorf("PAYHOOK_ALLOW_PRIVATE_CALLBACKS: %w", err)
		}
		c.AllowPrivateCallbacks = b
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
