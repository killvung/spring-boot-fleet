CREATE TABLE vehicle_positions (
    vehicle_id TEXT NOT NULL CHECK (length(vehicle_id) BETWEEN 1 AND 256),
    route_id TEXT,
    trip_id TEXT,
    direction_id SMALLINT CHECK (direction_id IN (0, 1)),
    latitude DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    bearing REAL CHECK (bearing >= 0 AND bearing < 360),
    speed REAL CHECK (speed >= 0 AND speed < 'Infinity'::real),
    occupancy_status TEXT,
    current_stop_sequence BIGINT CHECK (current_stop_sequence BETWEEN 0 AND 4294967295),
    current_stop_id TEXT,
    source_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (vehicle_id, source_timestamp)
);

CREATE INDEX vehicle_positions_vehicle_time_idx ON vehicle_positions (vehicle_id, source_timestamp DESC);
CREATE INDEX vehicle_positions_route_time_idx ON vehicle_positions (route_id, source_timestamp DESC);
-- Supports retention and globally ordered history queries.
CREATE INDEX vehicle_positions_time_vehicle_idx ON vehicle_positions (source_timestamp DESC, vehicle_id);

CREATE TABLE latest_vehicle_positions (LIKE vehicle_positions INCLUDING CONSTRAINTS);
ALTER TABLE latest_vehicle_positions ADD PRIMARY KEY (vehicle_id);
CREATE INDEX latest_vehicle_positions_route_time_idx
    ON latest_vehicle_positions (route_id, source_timestamp DESC);

-- History is immutable except for the explicit retention DELETE job.
CREATE FUNCTION reject_vehicle_position_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'vehicle_positions is append-only';
END;
$$;
CREATE TRIGGER vehicle_positions_no_update BEFORE UPDATE ON vehicle_positions
    FOR EACH ROW EXECUTE FUNCTION reject_vehicle_position_update();
