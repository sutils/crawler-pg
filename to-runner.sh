#!/bin/bash
set -e
rm -rf ../crawler/runner/node_modules/crawler-pg/
mkdir -p ../crawler/runner/node_modules/crawler-pg/dist/
cp -rf dist/* ../crawler/runner/node_modules/crawler-pg/dist/
cp -f *.json ../crawler/runner/node_modules/crawler-pg/
