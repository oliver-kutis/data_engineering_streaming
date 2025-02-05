# !/bin/bash
export $(cat ../.env | xargs -I% echo TF_VAR_%)