# sn64-chute-analyse

## Run chute audit

- https://github.com/rayonlabs/chutes-audit

## Setting up password-free login

- set up password-free login to primary host
- set up password-free login to chutes audit host

## Edit your config

- refer to config.template file

## Run main.py

- python3 main.py -c config.template

Local sqlite is used to store the host of instance running history, so if you need to
watch a full history you should run above command with a `while` loop with proper interval
you want.

## Result
