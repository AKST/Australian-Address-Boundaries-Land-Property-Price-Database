CREATE TABLE IF NOT EXISTS nsw_vg.lv_source_file (
    source_file_id SERIAL PRIMARY KEY,
    source_file_name TEXT NOT NULL,
    source_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg.source (
    source_id SERIAL PRIMARY KEY,
    source_file_id INT NOT NULL,
    source_file_position BIGINT NOT NULL
);
