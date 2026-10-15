#!/usr/bin/env bash
kind delete cluster --name "${CLUSTER:-payhook}"
