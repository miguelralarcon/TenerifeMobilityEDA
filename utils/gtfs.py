import os
import pandas as pd
from shapely.geometry import LineString
import geopandas as gpd

class GTFSReadFiles:

    def __init__(self, directory):
        self.directory = directory
        self.dfs = {}
        self.load_data()

    def _load_data(self, subfolder=""):
        folder_path = os.path.join(self.directory, subfolder)
        file_names = ["agency.txt", "stop_times.txt", "calendar_dates.txt", "trips.txt", "stops.txt", "routes.txt"]
        for file_name in file_names:
            file_path = os.path.join(folder_path, file_name)
            if os.path.exists(file_path):
                df_name = file_name.split(".")[0]
                self.dfs[df_name] = pd.concat([self.dfs.get(df_name, pd.DataFrame()), pd.read_csv(file_path)], ignore_index=True)

    def aggregate_data_from_subfolders(self):
        subfolders = [folder for folder in os.listdir(self.directory) if os.path.isdir(os.path.join(self.directory, folder))]
        for subfolder in subfolders:
            self._load_data(subfolder)

    def load_data(self):
        main_files = ["agency.txt", "stop_times.txt", "calendar_dates.txt", "trips.txt", "stops.txt", "routes.txt"]
        files_in_directory = os.listdir(self.directory)
        if all(file in files_in_directory for file in main_files):
            self._load_data()
        else:
            self.aggregate_data_from_subfolders()
            
    def info(self):
        for df_name, df in self.dfs.items():
            print(f"{df_name}.txt")
            print("-" * 40)
            print(df.info())
            print("\n")


class GTFSStops:
    
    def __init__(self, dataframes):
        self.dfs = dataframes
        
    def get_positions(self):
        positions_data = self.dfs["stops"][["stop_id", "stop_name", "stop_lat", "stop_lon"]]
        return positions_data
    
    def get_lines(self, stop_id):
        lines = {'route_id': [], 'route_name': [], 'previous_stop': [], 'next_stop': []}
        for trip in self.dfs["stop_times"][self.dfs["stop_times"]["stop_id"] == stop_id].trip_id.values:
            rid = self.dfs["trips"][self.dfs["trips"]["trip_id"] == trip].route_id.values[0]
            if rid not in lines['route_id']:
                lines['route_id'].append(rid)
                rnam = self.dfs["routes"][self.dfs["routes"]["route_id"] == rid].route_short_name.values[0]
                lines['route_name'].append(rnam)
                rst = self.dfs["stop_times"]
                stop_seq_id = rst.loc[(rst["trip_id"] == trip) & (rst["stop_id"] == stop_id)].stop_sequence.values[0]
                prev_stop = rst.loc[(rst["trip_id"] == trip) & (rst["stop_sequence"] == stop_seq_id - 1)].stop_id.values[0]
                next_stop = rst.loc[(rst["trip_id"] == trip) & (rst["stop_sequence"] == stop_seq_id + 1)].stop_id.values[0]
                lines['previous_stop'].append(prev_stop)
                lines['next_stop'].append(next_stop)

        df = pd.DataFrame(lines)
        return df
    
    def get_plot_df(self):
        df = self.get_positions()
        for i in df.index:
            lines = self.get_lines(df.loc[i, 'stop_id'])
            df.iloc[i, 'lines'] = lines['route_name'].values
        return df

    



class GTFSSegments:
    def compute_segment_durations(self, trip_id):
        stop_times_df = self.dataframes["stop_times"]
        stops_df = self.dataframes["stops"]
        
        sample_trip = stop_times_df[stop_times_df["trip_id"] == trip_id]
        sample_trip_with_stops = sample_trip.merge(stops_df, on="stop_id", how="left")
        
        sample_trip_with_stops["arrival_time"] = pd.to_timedelta(sample_trip_with_stops["arrival_time"])
        sample_trip_with_stops["departure_time"] = pd.to_timedelta(sample_trip_with_stops["departure_time"])

        sample_trip_with_stops["segment_duration"] = sample_trip_with_stops["arrival_time"].shift(-1) - sample_trip_with_stops["departure_time"]
        
        return sample_trip_with_stops
    
    def interpolate_positions(self, trip_data):
        trips_df = self.dataframes["trips"]
        shapes_df = self.dataframes.get("shapes", pd.DataFrame())
        
        sample_shape_id = trips_df[trips_df["trip_id"] == trip_data["trip_id"].iloc[0]]["shape_id"].iloc[0]
        sample_shape = shapes_df[shapes_df["shape_id"] == sample_shape_id]
        
        line = LineString(list(zip(sample_shape["shape_pt_lon"], sample_shape["shape_pt_lat"])))
        
        # Calculate interpolated positions for each segment
        positions = []
        for index, row in trip_data[:-1].iterrows():
            next_row = trip_data.iloc[index + 1]
            fraction = (index + 1) / len(trip_data)
            start_point, end_point = self._interpolate_position(line, index/len(trip_data), fraction)
            positions.append({
                "start_time": row["departure_time"],
                "end_time": next_row["arrival_time"],
                "start_point": start_point,
                "end_point": end_point
            })
        
        return positions
    
    def generate_geodataframe(self, positions):
        gdf_positions = gpd.GeoDataFrame(positions, geometry='start_point')
        gdf_positions.rename(columns={'start_point': 'geometry'}, inplace=True)
        return gdf_positions
    
    def _interpolate_position(self, line, start_fraction, end_fraction):
        start_point = line.interpolate(start_fraction, normalized=True)
        end_point = line.interpolate(end_fraction, normalized=True)
        return start_point, end_point