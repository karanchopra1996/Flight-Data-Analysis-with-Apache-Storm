#!/bin/sh

curl -s "https://opensky-network.org/api/states/all" | python -m json.tool > flights.txt

