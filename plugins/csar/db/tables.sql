-- Insert your database DDL or DML in here!
-- If you need to update the database to a newer version, increase the version in here to the next higher one
-- (v1.2 in this case as we are simulating an update already with the update SQL below), and make sure, that you
-- UPDATE the plugins table in your update script (see example).
CREATE TABLE IF NOT EXISTS csar_events (
    datestamp TIMESTAMP NOT NULL DEFAULT now(),
    ts double precision NOT NULL,
    playername text,
    ucid text,
    savedpilots int,
    helicopterused text,
    CONSTRAINT csar_events_pkey PRIMARY KEY (datestamp, ts)
);
CREATE TABLE IF NOT EXISTS csar_wounded
(
    datestamp TIMESTAMP DEFAULT now(),
    id text,
    coalition int,
    country int,
    pos jsonb,
    coordinates text,
    unitname text,
    typename text,
    playername text,
    freq text,
    CONSTRAINT csar_wounded_pkey PRIMARY KEY (id)
)
