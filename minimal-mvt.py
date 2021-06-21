import http.server
import socketserver
import re
import psycopg2
import json

# Database to connect to
DATABASE = {
    'user':     'docker',
    'password': 'docker',
    'host':     'localhost',
    'port':     '25433',
    'database': 'danishais'
    }

# Table to query for MVT data, and columns to
# include in the tiles.
TABLE = {}

# HTTP server information
HOST = 'localhost'
PORT = 8080


########################################################################

class TileRequestHandler(http.server.BaseHTTPRequestHandler):

    DATABASE_CONNECTION = None

    # Search REQUEST_PATH for /{z}/{x}/{y}.{format} patterns
    def pathToTile(self, path):
        m = re.search(r'^\/(\d+)\/(\d+)\/(\d+)\.(\w+)', path)
        if (m):
            return {'zoom':   int(m.group(1)), 
                    'x':      int(m.group(2)), 
                    'y':      int(m.group(3)), 
                    'format': m.group(4)}
        else:
            return None


    # Do we have all keys we need? 
    # Do the tile x/y coordinates make sense at this zoom level?
    def tileIsValid(self, tile):
        if not ('x' in tile and 'y' in tile and 'zoom' in tile):
            return False
        if 'format' not in tile or tile['format'] not in ['pbf', 'mvt']:
            return False
        size = 2 ** tile['zoom'];
        if tile['x'] >= size or tile['y'] >= size:
            return False
        if tile['x'] < 0 or tile['y'] < 0:
            return False
        return True


    # Calculate envelope in "Spherical Mercator" (https://epsg.io/3857)
    def tileToEnvelope(self, tile):
        # Width of world in EPSG:3857
        worldMercMax = 20037508.3427892
        worldMercMin = -1 * worldMercMax
        worldMercSize = worldMercMax - worldMercMin
        # Width in tiles
        worldTileSize = 2 ** tile['zoom']
        # Tile width in EPSG:3857
        tileMercSize = worldMercSize / worldTileSize
        # Calculate geographic bounds from tile coordinates
        # XYZ tile coordinates are in "image space" so origin is
        # top-left, not bottom right
        env = dict()
        env['xmin'] = worldMercMin + tileMercSize * tile['x']
        env['xmax'] = worldMercMin + tileMercSize * (tile['x'] + 1)
        env['ymin'] = worldMercMax - tileMercSize * (tile['y'] + 1)
        env['ymax'] = worldMercMax - tileMercSize * (tile['y'])
        return env


    # Generate SQL to materialize a query envelope in EPSG:3857.
    # Densify the edges a little so the envelope can be
    # safely converted to other coordinate systems.
    def envelopeToBoundsSQL(self, env):
        DENSIFY_FACTOR = 4
        env['segSize'] = (env['xmax'] - env['xmin'])/DENSIFY_FACTOR
        sql_tmpl = 'ST_Segmentize(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 3857),{segSize})'
        return sql_tmpl.format(**env)


    # Generate a SQL query to pull a tile worth of MVT data
    # from the table of interest.        
    def envelopeToSQL(self, env):
        tbl = TABLE.copy()
        tbl['env'] = self.envelopeToBoundsSQL(env)
        # Materialize the bounds
        # Select the relevant geometry and clip to MVT bounds
        # Convert to MVT format
        sql_tmpl = """
            WITH 
            bounds AS (
                SELECT {env} AS geom, 
                       {env}::box2d AS b2d
            ),
            mvtgeom AS (
                SELECT ST_AsMVTGeom(ST_Transform(t.{geomColumn}, 3857), bounds.b2d) AS geom
                FROM {table} t, bounds
                WHERE ST_Intersects(t.{geomColumn}, ST_Transform(bounds.geom, {srid}))
            ) 
            SELECT ST_AsMVT(mvtgeom.*) FROM mvtgeom
        """
        return sql_tmpl.format(**tbl)

    def envelopeToSQL2(self, env):
        tbl = TABLE.copy()
        tbl['env'] = self.envelopeToBoundsSQL(env)
        # Materialize the bounds
        # Select the relevant geometry and clip to MVT bounds
        # Convert to MVT format
        sql_tmpl = """
        with
        bounds AS (
            SELECT {env} AS geom
        ), subset_ships AS (
            SELECT * FROM ships, bounds WHERE intersects(ST_Transform(bounds.geom, SRID(trip)), trip) LIMIT 5
        ),
        clipped AS (
            SELECT mmsi, atGeometry(transform(trip, 3857), bounds.geom)::geometry AS geom
            FROM subset_ships, bounds
        ), dump AS (
            SELECT c.mmsi AS mmsi, (ST_DumpPoints(c.geom)).geom AS geom FROM clipped c
        ), timestamps as (
            SELECT array_agg(ST_M(d.geom)) AS TIMESTAMP, d.mmsi AS mmsi
            FROM DUMP AS d INNER JOIN clipped AS c ON d.mmsi = c.mmsi
            GROUP BY d.mmsi
        ), proj_variables as (
            SELECT st_xmax(b.geom) - st_xmin(b.geom) AS width,
                   st_ymax(b.geom) - st_ymin(b.geom) AS height,
                   4096 AS EXTENT,
                   4096 / (st_xmax(b.geom) - st_xmin(b.geom)) AS fx,
                   - 4096 / (st_ymax(b.geom) - st_ymin(b.geom)) AS fy,
                   - st_xmin(b.geom) * (4096 / (st_xmax(b.geom) - st_xmin(b.geom))) AS xoff,
                   - st_ymax(b.geom) * (- 4096 / (st_ymax(b.geom) - st_ymin(b.geom))) AS yoff
            FROM bounds AS b
        ), data AS (
            SELECT c.mmsi,
                   ST_SnapToGrid(st_affine(c.geom, v.fx, 0, 0, v.fy, v.xoff, v.yoff), 1) AS geom,
                   jsonb_build_object('mmsi', c.mmsi, 'timestamps', t.timestamp) AS properties
            FROM clipped c
            INNER JOIN timestamps t ON c.mmsi = t.mmsi, proj_variables AS v
        )
        SELECT ST_AsMVT(d.*)
        from data as d
        """
        return sql_tmpl.format(**tbl)


    # Run tile query SQL and return error on failure conditions
    def sqlToPbf(self, sql):
        # Make and hold connection to database
        if not self.DATABASE_CONNECTION:
            try:
                self.DATABASE_CONNECTION = psycopg2.connect(**DATABASE)
            except (Exception, psycopg2.Error) as error:
                self.send_error(500, "cannot connect: %s" % (str(DATABASE)))
                return None

        # Query for MVT
        with self.DATABASE_CONNECTION.cursor() as cur:
            cur.execute(sql)
            if not cur:
                self.send_error(404, "sql query failed: %s" % (sql))
                return None
            return cur.fetchone()[0]
        
        return None


    # Handle HTTP GET requests
    def do_GET(self):

        tile = self.pathToTile(self.path)
        if not (tile and self.tileIsValid(tile)):
            self.send_error(400, "invalid tile path: %s" % (self.path))
            return

        env = self.tileToEnvelope(tile)
        sql = self.envelopeToSQL2(env)
        pbf = self.sqlToPbf(sql)

        self.log_message("path: %s\ntile: %s\n env: %s" % (self.path, tile, env))
        self.log_message("sql: %s" % (sql))
        
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-type", "application/vnd.mapbox-vector-tile")
        self.end_headers()
        self.wfile.write(pbf)



########################################################################


with http.server.HTTPServer((HOST, PORT), TileRequestHandler) as server:
    try:
        print("serving at port", PORT)
        server.serve_forever()
    except KeyboardInterrupt:
        if self.DATABASE_CONNECTION:
            self.DATABASE_CONNECTION.close()
        print('^C received, shutting down server')
        server.socket.close()


