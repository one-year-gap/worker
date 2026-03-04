#!/bin/sh
set -eu

# Accept common DB env styles used by api-server and infra.
db_url="${SPRING_DATASOURCE_URL:-${DATABASE_URL:-${DB_URL:-}}}"
db_user="${SPRING_DATASOURCE_USERNAME:-${DATABASE_USERNAME:-${DB_USERNAME:-${DB_USER:-${POSTGRES_USER:-${RDS_USERNAME:-}}}}}}"
db_pass="${SPRING_DATASOURCE_PASSWORD:-${DATABASE_PASSWORD:-${DB_PASSWORD:-${POSTGRES_PASSWORD:-${RDS_PASSWORD:-}}}}}"

db_host="${DB_HOST:-${POSTGRES_HOST:-${RDS_HOSTNAME:-}}}"
db_port="${DB_PORT:-${POSTGRES_PORT:-${RDS_PORT:-5432}}}"
db_name="${DB_NAME:-${POSTGRES_DB:-${RDS_DB_NAME:-holliverse}}}"

if [ -z "${db_url}" ] && [ -n "${db_host}" ]; then
  if [ "${db_host}" = "127.0.0.1" ] || [ "${db_host}" = "localhost" ]; then
    db_host="host.docker.internal"
  fi
  db_url="jdbc:postgresql://${db_host}:${db_port}/${db_name}"
fi

case "${db_url}" in
  postgres://*|postgresql://*)
    db_url="jdbc:${db_url}"
    ;;
esac

# In containers, localhost points to this container itself.
case "${db_url}" in
  jdbc:postgresql://127.0.0.1:*|jdbc:postgresql://localhost:*)
    db_url="$(echo "${db_url}" | sed -E 's#^jdbc:postgresql://(127\.0\.0\.1|localhost):#jdbc:postgresql://host.docker.internal:#')"
    ;;
esac

db_url="${db_url:-jdbc:postgresql://host.docker.internal:5432/holliverse}"
db_user="${db_user:-postgres}"
db_pass="${db_pass:-postgres}"

export SPRING_DATASOURCE_URL="${db_url}"
export SPRING_DATASOURCE_USERNAME="${db_user}"
export SPRING_DATASOURCE_PASSWORD="${db_pass}"

echo "[entrypoint] SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE:-container}"
echo "[entrypoint] SPRING_DATASOURCE_URL=${SPRING_DATASOURCE_URL}"
echo "[entrypoint] SPRING_DATASOURCE_USERNAME=${SPRING_DATASOURCE_USERNAME}"

exec java -jar /app/app.jar
