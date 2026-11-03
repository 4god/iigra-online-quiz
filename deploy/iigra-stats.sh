#!/bin/bash
# Сводка по серверу ИИгры: регистрации и посещения главной страницы.
# Запуск на машине: iigra-stats (боевая копия) или iigra-stats iigra-test (тестовая).
NAME="${1:-iigra}"
echo "== $NAME: регистрации"
sudo sqlite3 -separator '  ' "/var/lib/$NAME/iigra.db" "SELECT 'всего: ' || COUNT(*) FROM users;" "SELECT date(created,'unixepoch','+3 hours'), COUNT(*) FROM users GROUP BY 1 ORDER BY 1;"
echo "== $NAME: посещения главной страницы (по дням, МСК): заходов / разных посетителей"
sudo cat /var/log/caddy/$NAME-access.log* 2>/dev/null | jq -r 'select(.request.uri=="/" and .request.method=="GET") | [((.ts+10800)|strftime("%Y-%m-%d")), .request.remote_ip] | @tsv' | awk -F'\t' '{n[$1]++; if(!seen[$1 FS $2]++) u[$1]++} END {for (d in n) print d"  "n[d]" / "u[d]}' | sort
