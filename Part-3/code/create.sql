CREATE TABLE trimet.stopevents (
        trip_id integer,
        route_id integer,
        vehicle_id integer,
        service_key service_type,
        direction tripdir_type
);

CREATE VIEW trimet.trip_view as 
        SELECT  DISTINCT tr.trip_id, s.route_id, tr.vehicle_id, tr.service_key, s.direction 
        FROM trip tr, stopevents s 
        WHERE s.trip_id = tr.trip_id;