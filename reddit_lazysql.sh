#!/bin/bash

# Connect to the Reddit PostgreSQL database using lazysql with SSL disabled

lazysql "postgres://postgres:secret!1234@localhost:5434/reddit?sslmode=disable"
