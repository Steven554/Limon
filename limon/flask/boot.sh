#!/bin/sh
#start webserver with 4 workers on port 5000
exec gunicorn -w 4 -b 0.0.0.0:5000 "limon:app"
