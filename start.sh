#!/bin/bash
# Define AIRFLOW_HOME en función del directorio actual
export AIRFLOW_HOME="$(pwd)/airflow"
echo "AIRFLOW_HOME configurado en: $AIRFLOW_HOME"

# Ejecuta Airflow en modo standalone
airflow standalone