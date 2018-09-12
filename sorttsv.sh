#!/bin/sh
(head -n 1 $1 && tail -n +2 $1 | sort) > output/$2
