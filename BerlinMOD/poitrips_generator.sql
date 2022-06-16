-- Random integer in a range with uniform distribution

CREATE OR REPLACE FUNCTION random_int(low int, high int)
	RETURNS int AS $$
BEGIN
	RETURN floor(random() * (high - low + 1) + low);
END;
$$ LANGUAGE plpgsql STRICT;

-- Random float with Gaussian distribution

CREATE OR REPLACE FUNCTION random_gauss(avg float = 0, stddev float = 1)
RETURNS float AS $$
DECLARE
	x1 real; x2 real; w real;
BEGIN
	LOOP
		x1 = 2.0 * random() - 1.0;
		x2 = 2.0 * random() - 1.0;
		w = x1 * x1 + x2 * x2;
		EXIT WHEN w < 1.0;
	END LOOP;
	RETURN avg + x1 * sqrt(-2.0*ln(w)/w) * stddev;
END;
$$ LANGUAGE plpgsql STRICT;

-- Random float with a Gaussian distributed value within [Low, High]

CREATE OR REPLACE FUNCTION random_boundedgauss(low float, high float,
	avg float = 0, stddev float = 1)
RETURNS float AS $$
DECLARE
	-- Result of the function
	result real;
BEGIN
	result = random_gauss(avg, stddev);
	IF result < low THEN
		RETURN low;
	ELSEIF result > high THEN
		RETURN high;
	ELSE
		RETURN result;
	END IF;
END;
$$ LANGUAGE plpgsql STRICT;

-- Creates a random non-zero duration of length [2ms, N min - 4ms]
-- using a uniform distribution

CREATE OR REPLACE FUNCTION createPauseN(Minutes int)
	RETURNS interval AS $$
BEGIN
	RETURN ( 2 + random_int(1, Minutes * 60000 - 6) ) * interval '1 ms';
END;
$$ LANGUAGE plpgsql STRICT;

-- Creates a random duration of length [0ms, 2h] using Gaussian
-- distribution

CREATE OR REPLACE FUNCTION createPause()
RETURNS interval AS $$
BEGIN
	RETURN (((random_boundedgauss(-6.0, 6.0, 0.0, 1.4) * 100.0) + 600.0) * 6000.0)::int * interval '1 ms';
END;
$$ LANGUAGE plpgsql STRICT;

DROP FUNCTION IF EXISTS berlinmod_selectPOIs;
CREATE FUNCTION berlinmod_selectPOIs(vehicId int, noPOIs int, noNodes int)
RETURNS bigint AS $$
DECLARE
	-- Random sequence number
	seqNo int;
	-- Result of the function
	result bigint;
BEGIN
	IF noPOIs > 0 AND random() < 0.8 THEN
		seqNo = random_int(1, noPOIs);
		SELECT osm_id INTO result
		FROM NeighbourPOIs
		WHERE vehicle = vehicId AND seq = seqNo;
	ELSE
		seqNo = random_int(1, noNodes);
		SELECT osm_id INTO result
		FROM Nodes
		Where id = seqNo;
	END IF;
	RETURN result;
END;
$$ LANGUAGE plpgsql STRICT;

DROP FUNCTION IF EXISTS berlinmod_createTrips;
CREATE FUNCTION berlinmod_createTrips(beginVehicle int, endVehicle int, noDays int,
	startDay date, disturbData boolean, messages text, tripGeneration text)
RETURNS void AS $$
DECLARE
	-- Loops over the days for which we generate the data
	d date;
	-- 0 (Sunday) to 6 (Saturday)
	weekday int;
	-- Current timestamp
	t timestamptz;
	-- End timestamp
	targetT timestamptz;
	-- Home and work nodes
	homeNode bigint; workNode bigint;
	-- Source and target nodes of one subtrip of a leisure trip
	sourceNode bigint; targetNode bigint;
	-- Number of leisure trips and number of subtrips of a leisure trip
	noLeisTrip int; noSubtrips int;
	-- Morning or afternoon (1 or 2) leisure trip
	leisNo int;
	-- Number of previous trips generated so far
	tripSeq int = 0;
	POItripSeq  int = 0; 
	-- Loop variables
	i int; j int; k int; m int;
	startTime timestamptz; endTime timestamptz;
	-- for POIs distance
	MINUTES_PER_KM float = 1;
	sourceGeom geometry; targetGeom geometry;
	distance float;
	homeOsm bigint; workOsm bigint;
	homeGeom geometry; workGeom geometry;
BEGIN
	-- Loop for each vehicle
	startTime = clock_timestamp();
	RAISE INFO 'Execution started at %', startTime;	
	FOR i IN beginVehicle..endVehicle LOOP
		IF messages = 'medium' OR messages = 'verbose' THEN
			RAISE INFO '-- Vehicle %', i;
		ELSEIF i % 100 = 1 THEN
			RAISE INFO '  Vehicles % to %', i, least(i + 99, endVehicle);
		END IF;
		-- Get homenode and worknode 
		SELECT home_osm, work_osm, home_geom, work_geom INTO homeOsm, workOsm, homeGeom, workGeom
		FROM user_t V WHERE V.user_id = i;
		SELECT ST_Distance(ST_Transform(homeGeom, 3857), ST_Transform(workGeom, 3857)) INTO distance;
		d = startDay;
		-- Loop for each generation day
		FOR j IN 1..noDays LOOP
			IF messages = 'verbose' THEN
				RAISE INFO '  -- Day %', d;
			END IF;
			weekday = date_part('dow', d);

			-- 1: Monday, 5: Friday
			IF weekday BETWEEN 1 AND 5 THEN
				-- Home -> Work
				t = d + time '08:00:00' + CreatePauseN(120);
				-- INSERT home stay point yesterday to today
				IF j != 1 THEN
					INSERT INTO temporal_coordinate_t(the_point, osm_id, start_time, end_time, user_id) VALUES(homeGeom, homeOsm, targetT, t, i);
				END IF;
				IF messages = 'verbose' OR messages = 'debug' THEN
					RAISE INFO '    Home to work trip started at %', t;
				END IF;
				-- IF tripGeneration = 'C' THEN
				-- 	trip = create_trip(homework, t, disturbData, messages);
				-- ELSE
				-- 	trip = createTrip(homework, t, disturbData, messages);
				-- END IF;
				targetT = t + (distance / 1000 * MINUTES_PER_KM * 60)::int * interval '1 s';

				IF messages = 'medium' THEN
					RAISE INFO '    Home to work trip started at % and ended at %',
						t, targetT;
				END IF;
				-- INSERT INTO Trips VALUES
				-- 	(i, d, 1, homeNode, workNode, trip, trajectory(trip));
				-- Work -> Home
				t = d + time '16:00:00' + CreatePauseN(120);
				-- INSERT work stay point
				INSERT INTO temporal_coordinate_t(the_point, osm_id, start_time, end_time, user_id) VALUES(workGeom, workOsm, targetT, t, i);
				IF messages = 'verbose' OR messages = 'debug' THEN
					RAISE INFO '    Work to home trip started at %', t;
				END IF;
				-- IF tripGeneration = 'C' THEN
				-- 	trip = create_trip(workhome, t, disturbData, messages);
				-- ELSE
				-- 	trip = createTrip(workhome, t, disturbData, messages);
				-- END IF;
				targetT = t + (distance / 1000 * MINUTES_PER_KM * 60)::int * interval '1 s' + CreatePauseN(10);
				IF messages = 'medium' THEN
					RAISE INFO '    Work to home trip started at % and ended at %',
						t, targetT;
				END IF;
				-- INSERT INTO Trips VALUES
				-- 	(i, d, 2, workNode, homeNode, trip, trajectory(trip));
				tripSeq = 2;
			END IF;

			SELECT COUNT(DISTINCT tripNo) INTO noLeisTrip
			FROM LeisureTrip L
			WHERE L.vehicle = i AND L.day = d;
			IF noLeisTrip = 0 AND messages = 'verbose' or messages = 'debug' THEN
				RAISE INFO '    No leisure trip';
			END IF;
			-- Loop for each leisure trip (0, 1, or 2)
			POItripSeq = 1;
			FOR k IN 1..noLeisTrip LOOP
				IF weekday BETWEEN 1 AND 5 THEN
					t = d + time '20:00:00' + CreatePauseN(90);
					IF messages = 'medium' THEN
						RAISE INFO '    Weekday leisure trips started at %', t;
					END IF;
					leisNo = 1;
					-- INSERT temporal stay point at  home
					INSERT INTO temporal_coordinate_t(the_point, osm_id, start_time, end_time, user_id) VALUES(homeGeom, homeOsm, targetT, t, i);
				ELSE
					-- Determine whether there is a morning/afternoon (1/2) trip
					IF noLeisTrip = 2 THEN
						leisNo = k;
					ELSE
						SELECT tripNo INTO leisNo
						FROM LeisureTrip L
						WHERE L.vehicle = i AND L.day = d
						LIMIT 1;
					END IF;
					-- Determine the start time
					IF leisNo = 1 THEN
						t = d + time '09:00:00' + CreatePauseN(120);
						IF messages = 'medium' THEN
							RAISE INFO '    Weekend morning trips started at %', t;
						END IF;
						-- INSERT home stay point yesterday to today
						IF j != 1 THEN
							INSERT INTO temporal_coordinate_t(the_point, osm_id, start_time, end_time, user_id) VALUES(homeGeom, homeOsm, targetT, t, i);
						END IF;
					ELSE
						t = d + time '17:00:00' + CreatePauseN(120);
						IF messages = 'medium' OR messages = 'verbose' or messages = 'debug' THEN
							RAISE INFO '    Weekend afternoon trips started at %', t;
						END IF;
						INSERT INTO temporal_coordinate_t(the_point, osm_id, start_time, end_time, user_id) VALUES(homeGeom, homeOsm, targetT, t, i);
					END IF;
				END IF;
				-- Get the number of subtrips (number of destinations + 1)
				SELECT count(*) INTO noSubtrips
				FROM LeisureTrip L
				WHERE L.vehicle = i AND L.tripNo = leisNo AND L.day = d;
				FOR m IN 1..noSubtrips LOOP
					-- Get the source and destination nodes of the subtrip
					SELECT source, target INTO sourceNode, targetNode
					FROM LeisureTrip L
					WHERE L.vehicle = i AND L.day = d AND L.tripNo = leisNo AND L.seq = m;

					IF messages = 'verbose' OR messages = 'debug' THEN
						RAISE INFO '    Leisure trip from % to % started at %', sourceNode, targetNode, t;
					END IF;

					SELECT ST_Transform(the_geom, 3857) INTO sourceGeom FROM osm_nodes
					WHERE osm_id = sourceNode;
					SELECT ST_Transform(the_geom, 3857) INTO targetGeom FROM osm_nodes
					WHERE osm_id = targetNode;
					SELECT ST_Distance(sourceGeom, targetGeom) INTO distance;
					targetT = t + (distance / 1000 * MINUTES_PER_KM * 60)::int * interval '1 s';
					-- INSERT trips between POIs
					-- INSERT INTO POITrips VALUES(i, d, POItripSeq, sourceNode, targetNode, sourceGeom, targetGeom, t, targetT);
					POItripSeq = POItripSeq + 1;
					-- INSERT stay point
					IF m != noSubtrips THEN
						t = targetT + createPause();
						-- INSERT INTO POITrips VALUES(i, d, POItripSeq, targetNode, targetNode, targetGeom, targetGeom, targetT, t);
						INSERT INTO  temporal_coordinate_t(the_point, osm_id, start_time, end_time, user_id) VALUES(targetGeom, targetNode, targetT, t, i);
						POItripSeq = POItripSeq + 1;
					END IF;
				END LOOP;
			END LOOP;
			d = d + 1 * interval '1 day';
		END LOOP;
	END LOOP;
	endTime = clock_timestamp();
	RAISE INFO 'Execution finished at %', endTime;	
	RAISE INFO 'Execution time %', endTime - startTime;
	RAISE INFO 'Number of trips generated %', endVehicle - beginVehicle + 1;
	RETURN;
END;
$$ LANGUAGE plpgsql STRICT;

DROP FUNCTION IF EXISTS berlinmod_generate;
CREATE FUNCTION berlinmod_generate(scaleFactor float DEFAULT NULL,
	noVehicles int DEFAULT NULL, noDays int DEFAULT NULL,
	startDay date DEFAULT NULL, pathMode text DEFAULT NULL,
	nodeChoice text DEFAULT NULL, disturbData boolean DEFAULT NULL,
	messages text DEFAULT NULL, tripGeneration text DEFAULT NULL)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	----------------------------------------------------------------------
	-- Primary parameters, which are optional arguments of the function
	----------------------------------------------------------------------

	-- Scale factor
	-- Set value to 1.0 or bigger for a full-scaled benchmark
	P_SCALE_FACTOR float = 0.005;

	-- By default, the scale factor determine the number of cars and the
	-- number of days they are observed as follows
	--		noVehicles int = round((2000 * sqrt(P_SCALE_FACTOR))::numeric, 0)::int;
	--		noDays int = round((sqrt(P_SCALE_FACTOR) * 28)::numeric, 0)::int;
	-- For example, for P_SCALE_FACTOR = 1.0 these values will be
	--		noVehicles = 2000
	--		noDays int = 28
	-- Alternatively, you can manually set these parameters to arbitrary
	-- values using the optional arguments in the function call.

	-- The day the observation starts ===
	-- default: P_START_DAY = monday 06/01/2020)
	P_START_DAY date = '2020-06-01';

	-- Method for selecting a path between source and target nodes.
	-- Possible values are 'Fastest Path' (default) and 'Shortest Path'
	P_PATH_MODE text = 'Fastest Path';

	-- Method for selecting home and work nodes.
	-- Possible values are 'Network Based' for chosing the nodes with a
	-- uniform distribution among all nodes (default) and 'Region Based'
	-- to use the population and number of enterprises statistics in the
	-- Regions tables
	P_NODE_CHOICE text = 'Network Based';

	-- Choose imprecise data generation. Possible values are
	-- FALSE (no imprecision, default) and TRUE (disturbed data)
	P_DISTURB_DATA boolean = FALSE;

	-------------------------------------------------------------------------
	--	Secondary Parameters
	-------------------------------------------------------------------------

	-- Seed for the random generator used to ensure deterministic results
	P_RANDOM_SEED float = 0.5;

	-- Radius in meters defining a node's neigbourhood
	-- Default= 3 km
	P_NEIGHBOURHOOD_RADIUS float = 3000.0;
	P_NEIGHBOURPOIS_RADIUS float = 10000.0;

	-- Size for sample relations
	P_SAMPLE_SIZE int = 100;

	-- Number of paths sent in a batch to pgRouting
  P_PGROUTING_BATCH_SIZE int = 1e5;

	-- Quantity of messages shown describing the generation process
	-- Possible values are 'minimal', 'medium', 'verbose', and 'debug'
	P_MESSAGES text = 'minimal';

	-- Determine the language used to generate the trips.
  -- Possible values are 'C' (default) and 'SQL'
	P_TRIP_GENERATION text = 'C';

	----------------------------------------------------------------------
	--	Variables
	----------------------------------------------------------------------

	-- Number of nodes in the graph
	noNodes int;
	-- Number of nodes in the neighbourhood of the home node of a vehicle
	noNeigh int;
	-- Number of leisure trips (1 or 2 on week/weekend) in a day
	noLeisTrips int;
	-- Number of paths
	noPaths int;
	-- Number of calls to pgRouting
	noCalls int;
	-- Number of trips generated
	noTrips int;
	-- Loop variables
	i int; j int; k int;
	-- Home and work node identifiers
	homeNode bigint; workNode bigint;
	-- Home and work osm node
	homeOsm bigint; workOsm bigint;
	homeGeom geometry; workGeom geometry;
	streetId int;
	-- Node identifiers of a trip within a chain of leisure trips
	sourceNode bigint; targetNode bigint;
	-- Day for generating a leisure trip
	day date;
	-- Week day 0 -> 6: Sunday -> Saturday
	weekDay int;
	-- Start and end time of the generation
	startTime timestamptz; endTime timestamptz;
	-- Start and end time of the batch call to pgRouting
	startPgr timestamptz; endPgr timestamptz;
	startVeh timestamptz; endVeh timestamptz;
	startLes timestamptz; endLes timestamptz;
	startUpdate timestamptz; endUpdate timestamptz;
	-- Queries sent to pgrouting for choosing the path according to P_PATH_MODE
	-- and the number of records defined by LIMIT/OFFSET
	query1_pgr text; query2_pgr text;
	-- Random number of destinations (between 1 and 3)
	noDest int;
	-- String to generate the trace message
	str text;
BEGIN

	-------------------------------------------------------------------------
	--	Initialize parameters and variables
	-------------------------------------------------------------------------

	-- Setting the parameters of the generation
	IF scaleFactor IS NULL THEN
		scaleFactor = P_SCALE_FACTOR;
	END IF;
	IF noVehicles IS NULL THEN
		noVehicles = round((2000 * sqrt(scaleFactor))::numeric, 0)::int;
	END IF;
	IF noDays IS NULL THEN
		noDays = round((sqrt(scaleFactor) * 28)::numeric, 0)::int + 2;
	END IF;
	IF startDay IS NULL THEN
		startDay = P_START_DAY;
	END IF;
	IF pathMode IS NULL THEN
		pathMode = P_PATH_MODE;
	END IF;
	IF nodeChoice IS NULL THEN
		nodeChoice = P_NODE_CHOICE;
	END IF;
	IF disturbData IS NULL THEN
		disturbData = P_DISTURB_DATA;
	END IF;
	IF messages IS NULL THEN
		messages = P_MESSAGES;
	END IF;
	IF tripGeneration IS NULL THEN
		tripGeneration = P_TRIP_GENERATION;
	END IF;

	RAISE INFO '------------------------------------------------------------------';
	RAISE INFO 'Starting the BerlinMOD data generator with scale factor %', scaleFactor;
	RAISE INFO '------------------------------------------------------------------';
	RAISE INFO 'Parameters:';
	RAISE INFO '------------';
	RAISE INFO 'No. of vehicles = %, No. of days = %, Start day = %',
		noVehicles, noDays, startDay;
	RAISE INFO 'Path mode = %, Disturb data = %', pathMode, disturbData;
	RAISE INFO 'Verbosity = %, Trip generation = %', messages, tripGeneration;
	startTime = clock_timestamp();
	RAISE INFO 'Execution started at %', startTime;
	RAISE INFO '------------------------------------------------------------------';

	-------------------------------------------------------------------------
	--	Creating the base data
	-------------------------------------------------------------------------

	-- Set the seed so that the random function will return a repeatable
	-- sequence of random numbers that is derived from the P_RANDOM_SEED.
	PERFORM setseed(P_RANDOM_SEED);

	-- Create a table accumulating all pairs (source, target) that will be
	-- sent to pgRouting in a single call. We DO NOT test whether we are
	-- inserting duplicates in the table, the query sent to the pgr_dijkstra
	-- function MUST use 'SELECT DISTINCT ...'

	-- Create a relation with all vehicles, their home and work node and the
	-- number of neighbourhood nodes

	RAISE INFO 'Creating the Users, and Neighbourhood tables';
	DROP TABLE IF EXISTS user_t;
	CREATE TABLE user_t(uid VARCHAR(32), user_id int, home_osm bigint NOT NULL, home_geom GEOMETRY(POINT),
		work_osm bigint NOT NULL, noNeighbours int, work_geom GEOMETRY(POINT), street_id INTEGER);

	-- For sample POIs
	DROP TABLE IF EXISTS NeighbourPOIs;
	CREATE TABLE NeighbourPOIs(vehicle int, seq int, osm_id bigint  NOT NULL,
		PRIMARY KEY (vehicle, seq));

	-- Get the number of nodes
	SELECT COUNT(*) INTO noNodes FROM Nodes;

	startVeh = clock_timestamp();
	FOR i IN 1..noVehicles LOOP
		homeNode = 0;
		workNode = 0;
		WHILE homeNode = workNode LOOP
			homeNode = random_int(1, noNodes);
			workNode = random_int(1, noNodes);
		END LOOP;
		IF i % 1000 = 1 THEN
			RAISE INFO '  Vehicles % to %', i, least(i + 999, noVehicles);
		END IF;
		IF homeNode IS NULL OR workNode IS NULL THEN
			RAISE EXCEPTION '    The home and the work nodes cannot be NULL';
		END IF;
		SELECT ST_Transform(geom, 4326), osm_id INTO homeGeom, homeOsm FROM nodes
		WHERE id = homeNode;
		SELECT ST_Transform(geom, 4326), osm_id INTO workGeom, workOsm FROM nodes
		WHERE id = workNode;
		SELECT id INTO streetId FROM streets WHERE ST_Contains(the_geom, homeGeom);
		INSERT INTO user_t(user_id, home_osm, work_osm, home_geom, work_geom, street_id) VALUES (i, homeOsm, workOsm, homeGeom, workGeom, streetId);

		-- INSERT INTO Neighbourhood
		-- WITH Temp AS (
		-- 	SELECT i AS vehicle, N2.id AS node
		-- 	FROM Nodes N1, Nodes N2
		-- 	WHERE N1.id = homeNode AND N1.id <> N2.id AND
		-- 		ST_DWithin(N1.geom, N2.geom, P_NEIGHBOURHOOD_RADIUS)
		-- )
		-- SELECT i, ROW_NUMBER() OVER () as seq, node
		-- FROM Temp;

		-- initialize neighbourhood POIs
		INSERT INTO NeighbourPOIs
		WITH Tmp AS (
			SELECT i AS vehicle, PT.osm_id AS osm_id
			FROM Nodes N1, pointsofinterest PT
			WHERE N1.id = homeNode AND N1.osm_id <> PT.osm_id AND
				ST_DWithin(N1.geom, PT.geom, P_NEIGHBOURPOIS_RADIUS)
		)
		SELECT i, ROW_NUMBER() OVER () as seq, osm_id
		FROM Tmp;
	END LOOP;
	endVeh = clock_timestamp();
	RAISE INFO 'Call to create vehicles end at % lasted %', endVeh, endVeh - startVeh;

	-- Build indexes to speed up processing
	CREATE UNIQUE INDEX user_t_user_id_idx ON user_t USING BTREE(user_id);
	CREATE UNIQUE INDEX NeighbourPOIs_pkey_idx ON NeighbourPOIs USING BTREE(vehicle, seq);
	RAISE INFO 'Creation of the index table end at %', clock_timestamp();

	startUpdate = clock_timestamp();
	CREATE INDEX idx_neighbourPOIs_vehicle ON NeighbourPOIs(vehicle);
	UPDATE user_t V
	SET noNeighbours = (SELECT COUNT(*) FROM NeighbourPOIs N WHERE N.vehicle = V.user_id);
	endUpdate = clock_timestamp();
	RAISE INFO 'Call to update vehicles end at % lasted %', endUpdate, endUpdate - startUpdate;

	-------------------------------------------------------------------------
	-- Generate the leisure trips.
	-- There is at most 1 leisure trip during the week (evening) and at most
	-- 2 leisure trips during the weekend (morning and afternoon).
	-- The value of attribute tripNo is 1 for evening and morning trips
	-- and is 2 for afternoon trips.
	-------------------------------------------------------------------------

	RAISE INFO 'Creating the LeisureTrip table';
	startLes = clock_timestamp();
	DROP TABLE IF EXISTS LeisureTrip;
	CREATE TABLE LeisureTrip(vehicle int, day date, tripNo int,
		seq int, source bigint, target bigint,
		PRIMARY KEY (vehicle, day, tripNo, seq));
	-- Loop for every vehicle
	FOR i IN 1..noVehicles LOOP
		IF messages = 'verbose' THEN
			RAISE INFO '-- Vehicle %', i;
		END IF;
		IF i % 1000 = 1 THEN
			RAISE INFO '  Vehicles % to %', i, least(i + 999, noVehicles);
		END IF;
		-- Get home node and number of neighbour nodes
		SELECT home_osm, noNeighbours INTO homeOsm, noNeigh
		FROM user_t V WHERE V.user_id = i;
		day = startDay;
		-- Loop for every generation day
		FOR j IN 1..noDays LOOP
			IF messages = 'verbose' THEN
				RAISE INFO '  -- Day %', day;
			END IF;
			weekday = date_part('dow', day);
			-- Generate leisure trips (if any)
			-- 1: Monday, 5: Friday
			IF weekday BETWEEN 1 AND 5 THEN
				noLeisTrips = 1;
			ELSE
				noLeisTrips = 2;
			END IF;
			-- Loop for every leisure trip in a day (1 or 2)
			FOR k IN 1..noLeisTrips LOOP
				-- Generate a set of leisure trips with a probability 0.4
				IF random() <= 0.4 THEN
					-- Select a number of destinations between 1 and 3
					IF random() < 0.8 THEN
						noDest = 1;
					ELSIF random() < 0.5 THEN
						noDest = 2;
					ELSE
						noDest = 3;
					END IF;
					IF messages = 'verbose' THEN
						IF weekday BETWEEN 1 AND 5 THEN
							str = '    Evening';
						ELSE
							IF k = 1 THEN
								str = '    Morning';
							ELSE
								str = '    Afternoon';
							END IF;
						END IF;
						RAISE INFO '% leisure trip with % destinations', str, noDest;
					END IF;
					sourceNode = homeOsm;
					FOR m IN 1..noDest + 1 LOOP
						IF m <= noDest THEN
							targetNode = berlinmod_selectPOIs(i, noNeigh, noNodes);
						ELSE
							targetNode = homeOsm;
						END IF;
						IF targetNode IS NULL THEN
							RAISE EXCEPTION '    Destination node cannot be NULL';
						END IF;
						IF messages = 'verbose' THEN
							RAISE INFO '    Leisure trip from % to %', sourceNode, targetNode;
						END IF;
						INSERT INTO LeisureTrip VALUES
							(i, day, k, m, sourceNode, targetNode);
						sourceNode = targetNode;
					END LOOP;
				ELSE
					IF messages = 'verbose' THEN
						RAISE INFO '    No leisure trip';
					END IF;
				END IF;
			END LOOP;
			day = day + 1 * interval '1 day';
		END LOOP;
	END LOOP;

	-- Build indexes to speed up processing
	endLes = clock_timestamp();
	RAISE INFO 'Call to create leisure trips end at % lasted %', endLes, endLes - startLes;

	-------------------------------------------------------------------------
	-- Generate the trips
	-------------------------------------------------------------------------

	-- DROP TABLE IF EXISTS POITrips;
	-- CREATE TABLE POITrips(vehicle int, day date, seq int, source_osm bigint,
	-- 	target_osm bigint, source_geom GEOMETRY(POINT), target_geom GEOMETRY(POINT), start_time timestamp, end_time timestamp,
	-- 	PRIMARY KEY (vehicle, day, seq));

	DROP TABLE IF EXISTS temporal_coordinate_t;
	CREATE TABLE temporal_coordinate_t (id SERIAL NOT NULL, the_point GEOMETRY(POINT), osm_id bigint, start_time TIMESTAMP, end_time TIMESTAMP, 
		uid VARCHAR(32), user_id INTEGER, trajectory_id INTEGER);

	 -- PERFORM berlinmod_createTrips(noVehicles, noDays, startDay, disturbData, 
	 -- 	messages, tripGeneration);

	 -- -- Get the number of trips generated
	 -- SELECT COUNT(*) INTO noTrips FROM Trips;

	 -- SELECT clock_timestamp() INTO endTime;
	 -- IF messages = 'medium' OR messages = 'verbose' THEN
	 -- 	RAISE INFO '-----------------------------------------------------------------------';
	 -- 	RAISE INFO 'BerlinMOD data generator with scale factor %', scaleFactor;
	 -- 	RAISE INFO '-----------------------------------------------------------------------';
	 -- 	RAISE INFO 'Parameters:';
	 -- 	RAISE INFO '------------';
	 -- 	RAISE INFO 'No. of vehicles = %, No. of days = %, Start day = %',
	 -- 		noVehicles, noDays, startDay;
	 -- 	RAISE INFO 'Path mode = %, Disturb data = %', pathMode, disturbData;
	 -- 	RAISE INFO 'Verbosity = %, Trip generation = %', messages, tripGeneration;
	 -- END IF;
	 -- RAISE INFO '------------------------------------------------------------------';
	 -- RAISE INFO 'Execution started at %', startTime;
	 -- RAISE INFO 'Execution finished at %', endTime;
	 -- RAISE INFO 'Execution time %', endTime - startTime;
	 -- RAISE INFO 'Number of trips generated %', noTrips;
	 -- RAISE INFO '------------------------------------------------------------------';

	-------------------------------------------------------------------

	return 'THE END';
END; $$;


