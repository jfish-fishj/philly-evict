# Simple: CSA polygons -> H3 hex polygons
# deps: pip install geopandas shapely h3 pyogrio
# %%
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.geometry import mapping
import h3
# %%
# %%
# ---------- settings ----------
input_path = "/Users/joefish/Downloads/cb_2018_us_csa_500k/cb_2018_us_csa_500k.shp" # <- your shapefile
res = 8                                       # H3 resolution
out_path = f"hex_res{res}.gpkg"                 # output GeoPackage
id_col = "GEOID"                                   # e.g., "GEOID" or None to use row index
# --------------------------------
# %%
gdf = gpd.read_file(input_path)

# keep only polygonal geometries
gdf = gdf[gdf.geometry.type.isin(["Polygon", "MultiPolygon"])].copy()

# filter for philly csa only for testing
gdf = gdf[gdf.NAME.str.contains("Chicago")].copy()
# ensure WGS84 lon/lat
if gdf.crs is None or gdf.crs.to_epsg() != 4326:
    gdf = gdf.to_crs(4326)

# optional quick clean (fix invalids)
gdf["geometry"] = gdf.geometry.buffer(0)
# %%
gdf = gdf.to_crs(4326)
gdf_dissolve = gdf.dissolve()

# %% 
# plot to check
ax = gdf_dissolve.plot(figsize=(10, 6), edgecolor='black', facecolor='none')
ax.set_title('Dissolved CSA Polygons')
ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')

# %%
cells = gdf_dissolve.geometry.apply(lambda x: h3.geo_to_cells(x, res=res))
# %%
cells_ex = cells.explode().reset_index(drop=True)
# %%
hex_df = cells.rename("h3").explode().dropna().to_frame()
# %%
# 2) H3 -> shapely Polygon (v4: cell_to_boundary gives (lat, lon); flip to (lon, lat))
def h3_to_poly(cell):
    latlon = h3.cell_to_boundary(cell)
    ring = [(lon, lat) for (lat, lon) in latlon]
    if ring[0] != ring[-1]:
        ring.append(ring[0])
    return Polygon(ring)

hex_df["geometry"] = hex_df["h3"].map(h3_to_poly)

# 3) make a GeoDataFrame
hex_gdf = gpd.GeoDataFrame(hex_df, geometry="geometry", crs="EPSG:4326")

# %%
# %% 
# spatial merge csa back on
hex_gdf_m = gpd.sjoin(hex_gdf, gdf.to_crs(4326), how="inner")
hex_gdf_m = hex_gdf_m.reset_index(drop=True)
# %%
# plot to check
# pick a csa to visualize
csa_id = "206" # first csa
csa_hexes = hex_gdf_m[hex_gdf_m.NAME.str.contains("Chicago")].copy()  
ax = csa_hexes.plot(figsize=(10, 6), edgecolor='blue', facecolor='white', alpha=0.01)
ax.set_title(f'H3 Hexagons within CSA {csa_id}')
ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')
# %%
# export csa_hexes
csa_hexes.to_file("/Users/joefish/Downloads/philly_shp/chicago_shp.shp")

# %%
