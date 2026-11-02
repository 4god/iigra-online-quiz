#!/usr/bin/env bash
# Поднимает ИИгру на чистой Ubuntu 24.04: Python-окружение, автозапуск через systemd,
# HTTPS через Caddy на бесплатном домене вида 1-2-3-4.sslip.io.
#
#   sudo bash setup_vm.sh --host-key МОЙКЛЮЧ [--domain game.example.ru] [--ip 1.2.3.4] [--no-https]
#                         [--opens-at 2026-11-12T19:00:00+03:00] [--live-questions /path/live_questions.csv]
#
# Повторный запуск безопасен: обновит код и перезапустит сервис, база игроков сохранится.
set -euo pipefail

REPO="https://github.com/4god/iigra-online-quiz.git"
APP_DIR=/opt/iigra
DATA_DIR=/var/lib/iigra
HOST_KEY=""
DOMAIN=""
HTTPS=1
OPENS_AT=""
LIVE_FILE=""
PUBLIC_IP=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host-key) HOST_KEY="$2"; shift 2 ;;
    --domain) DOMAIN="$2"; shift 2 ;;
    --no-https) HTTPS=0; shift ;;
    --opens-at) OPENS_AT="$2"; shift 2 ;;
    --live-questions) LIVE_FILE="$2"; shift 2 ;;
    --ip) PUBLIC_IP="$2"; shift 2 ;;
    *) echo "Неизвестный параметр: $1"; exit 1 ;;
  esac
done

[[ $EUID -eq 0 ]] || { echo "Запустите через sudo"; exit 1; }
[[ -n "$HOST_KEY" ]] || { echo "Укажите ключ ведущего: --host-key КЛЮЧ"; exit 1; }

echo "==> Пакеты"
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip git curl >/dev/null
if [[ $HTTPS -eq 1 ]]; then apt-get install -y -qq caddy >/dev/null; fi

echo "==> Код"
if [[ -d "$APP_DIR/.git" ]]; then
  git -c safe.directory="$APP_DIR" -C "$APP_DIR" pull -q
else
  git clone -q "$REPO" "$APP_DIR"
fi
if [[ -n "$LIVE_FILE" ]]; then
  cp "$LIVE_FILE" "$APP_DIR/live_questions.${LIVE_FILE##*.}"
fi
if [[ ! -f "$APP_DIR/live_questions.csv" && ! -f "$APP_DIR/live_questions.json" ]]; then
  echo "!!  live_questions.csv не найден — будет играться обычный вариант (см. GUIDE.md)"
fi

echo "==> Python-окружение"
python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install -q -r "$APP_DIR/requirements.txt"

echo "==> Проверка таблиц с вопросами"
(cd "$APP_DIR" && .venv/bin/python tools/check_questions.py)

id iigra >/dev/null 2>&1 || useradd --system --home "$DATA_DIR" --shell /usr/sbin/nologin iigra
mkdir -p "$DATA_DIR"
chown -R iigra:iigra "$DATA_DIR"
chown -R iigra:iigra "$APP_DIR"

echo "==> Настройки (/etc/iigra.env)"
{
  echo "HOST_KEY=$HOST_KEY"
  echo "DATA_DIR=$DATA_DIR"
  echo "QUESTION_SECONDS=30"
  if [[ -n "$OPENS_AT" ]]; then echo "LIVE_OPENS_AT=$OPENS_AT"; fi
} > /etc/iigra.env
chmod 600 /etc/iigra.env

PORT=8000
BIND=127.0.0.1
if [[ $HTTPS -eq 0 ]]; then BIND=0.0.0.0; PORT=80; fi

cat > /etc/systemd/system/iigra.service <<EOF
[Unit]
Description=ИИгра
After=network-online.target

[Service]
User=iigra
WorkingDirectory=$APP_DIR
EnvironmentFile=/etc/iigra.env
ExecStart=$APP_DIR/.venv/bin/uvicorn server:app --host $BIND --port $PORT --proxy-headers --forwarded-allow-ips 127.0.0.1 --log-level warning
Restart=always
RestartSec=2
LimitNOFILE=65535
AmbientCapabilities=CAP_NET_BIND_SERVICE

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable -q iigra
systemctl restart iigra

public_ip() {
  # Yandex Cloud metadata first (no internet needed), then a few public echo services.
  curl -fsS -m 3 -H "Metadata-Flavor: Google" \
    http://169.254.169.254/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip 2>/dev/null && return
  for u in https://ifconfig.me https://icanhazip.com https://api.ipify.org https://ipinfo.io/ip; do
    curl -fsS -m 5 "$u" 2>/dev/null && return
  done
}
IP="${PUBLIC_IP:-$(public_ip | tr -d '[:space:]')}"
if [[ -z "$IP" && -z "$DOMAIN" ]]; then
  echo "Не удалось узнать публичный IP. Запустите ещё раз с --ip АДРЕС (его видно в консоли облака) или --domain."
  exit 1
fi
if [[ $HTTPS -eq 1 ]]; then
  [[ -n "$DOMAIN" ]] || DOMAIN="${IP//./-}.sslip.io"
  cat > /etc/caddy/Caddyfile <<EOF
$DOMAIN {
    encode gzip
    reverse_proxy 127.0.0.1:$PORT
}
EOF
  systemctl restart caddy
  URL="https://$DOMAIN"
else
  URL="http://$IP"
fi

sleep 3
systemctl is-active --quiet iigra && echo "==> Сервис запущен" || { journalctl -u iigra -n 30 --no-pager; exit 1; }
cat <<EOF

Готово.
  Игроки:   $URL/
  Ведущий:  $URL/host   (ключ: $HOST_KEY)
  Проверка: $URL/verify
  База:     $DATA_DIR/iigra.db

Если HTTPS не открывается в первые пару минут — Caddy ещё получает сертификат:
  journalctl -u caddy -n 50 --no-pager
EOF
