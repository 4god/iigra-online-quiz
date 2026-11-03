#!/bin/bash
# Сводка по серверу ИИгры: регистрации и посещения главной страницы. Запуск на машине: iigra-stats
echo "== Регистрации"
sudo sqlite3 -separator '  ' /var/lib/iigra/iigra.db "SELECT 'всего: ' || COUNT(*) FROM users;" "SELECT date(created,'unixepoch','+3 hours'), COUNT(*) FROM users GROUP BY 1 ORDER BY 1;"
echo "== Посещения главной страницы (по дням, МСК): заходов / разных посетителей"
sudo cat /var/log/caddy/iigra-access.log* 2>/dev/null | jq -r 'select(.request.uri=="/" and .request.method=="GET") | [((.ts+10800)|strftime("%Y-%m-%d")), .request.remote_ip] | @tsv' | awk -F'\t' '{n[$1]++; if(!seen[$1 FS $2]++) u[$1]++} END {for (d in n) print d"  "n[d]" / "u[d]}' | sort
