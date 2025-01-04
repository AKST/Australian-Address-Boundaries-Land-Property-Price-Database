FROM postgres:16

# Install required packages for PostGIS
RUN apt-get update && apt-get install -y \
    postgresql-16-postgis-3 \
    postgresql-16-postgis-3-scripts \
    && rm -rf /var/lib/apt/lists/*

# Enable PostGIS
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Create the postgis and postgis_topology extensions\n\
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL\n\
    CREATE EXTENSION IF NOT EXISTS postgis;\n\
    CREATE EXTENSION IF NOT EXISTS postgis_topology;\n\
    CREATE EXTENSION IF NOT EXISTS tablefunc;\n\
EOSQL\n' > /docker-entrypoint-initdb.d/init_postgis.sh

# Make sure the script is executable
RUN chmod +x /docker-entrypoint-initdb.d/init_postgis.sh

# Expose default PostgreSQL port
EXPOSE 5432

