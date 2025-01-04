CREATE UNLOGGED TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a_legacy_source(
    ps_row_a_legacy_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_a_legacy_id) REFERENCES nsw_vg_raw.ps_row_a_legacy(ps_row_a_legacy_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE UNLOGGED TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_legacy_source(
    ps_row_b_legacy_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_b_legacy_id) REFERENCES nsw_vg_raw.ps_row_b_legacy(ps_row_b_legacy_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE INDEX idx_ps_row_a_legacy_source_a ON nsw_vg_raw.ps_row_a_legacy_source(ps_row_a_legacy_id);
CREATE INDEX idx_ps_row_a_legacy_source_b ON nsw_vg_raw.ps_row_a_legacy_source(source_id);
CREATE INDEX idx_ps_row_b_legacy_source_a ON nsw_vg_raw.ps_row_b_legacy_source(ps_row_b_legacy_id);
CREATE INDEX idx_ps_row_b_legacy_source_b ON nsw_vg_raw.ps_row_b_legacy_source(source_id);

CREATE UNLOGGED TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a_source(
    ps_row_a_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_a_id) REFERENCES nsw_vg_raw.ps_row_a(ps_row_a_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE UNLOGGED TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_source(
    ps_row_b_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_b_id) REFERENCES nsw_vg_raw.ps_row_b(ps_row_b_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE UNLOGGED TABLE IF NOT EXISTS nsw_vg_raw.ps_row_c_source(
    ps_row_c_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_c_id) REFERENCES nsw_vg_raw.ps_row_c(ps_row_c_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE UNLOGGED TABLE IF NOT EXISTS nsw_vg_raw.ps_row_d_source(
    ps_row_d_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_d_id) REFERENCES nsw_vg_raw.ps_row_d(ps_row_d_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE INDEX idx_ps_row_a_source_a ON nsw_vg_raw.ps_row_a_source(ps_row_a_id);
CREATE INDEX idx_ps_row_a_source_b ON nsw_vg_raw.ps_row_a_source(source_id);
CREATE INDEX idx_ps_row_b_source_a ON nsw_vg_raw.ps_row_b_source(ps_row_b_id);
CREATE INDEX idx_ps_row_b_source_b ON nsw_vg_raw.ps_row_b_source(source_id);
CREATE INDEX idx_ps_row_c_source_a ON nsw_vg_raw.ps_row_c_source(ps_row_c_id);
CREATE INDEX idx_ps_row_c_source_b ON nsw_vg_raw.ps_row_c_source(source_id);
CREATE INDEX idx_ps_row_d_source_a ON nsw_vg_raw.ps_row_d_source(ps_row_d_id);
CREATE INDEX idx_ps_row_d_source_b ON nsw_vg_raw.ps_row_d_source(source_id);

