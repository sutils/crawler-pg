#!/bin/bash
set -e
cd ../crawler
npm run build
cp -rf dist/* ../crawler-pg/node_modules/crawler/dist/