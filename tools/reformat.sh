#!/bin/sh

isort --atomic --gitignore .
black .
