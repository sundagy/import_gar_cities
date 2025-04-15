#!/usr/bin/bash

wget -c -t 0 https://fias-file.nalog.ru/downloads/2025.04.11/gar_xml.zip
unzip gar_xml.zip -d gar_xml '*/AS_ADDR_OBJ_*.XML' '*/AS_ADM_HIERARCHY_*.XML' '*/AS_MUN_HIERARCHY_*.XML' -x '*/AS_ADDR_OBJ_DIVISION_*.XML'
python import_gar.py ./gar_xml