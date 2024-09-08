FROM postgres:latest

# Install required packages
RUN apt-get update && apt-get install -y \
    postgis postgresql-13-postgis-3 \
    && rm -rf /var/lib/apt/lists/*

# Add PostGIS extension to the default PostgreSQL template
RUN echo "CREATE EXTENSION postgis;" | psql -U postgres -d postgres
# Add PostGIS extension to the default PostgreSQL template
RUN echo "CREATE EXTENSION postgis_topology;" | psql -U postgres -d postgres

# Expose the default PostgreSQL port
EXPOSE 5432

# Set the default entrypoint to the PostgreSQL entrypoint
ENTRYPOINT ["docker-entrypoint.sh"]

# Set the default command to start PostgreSQL
CMD ["postgres"]