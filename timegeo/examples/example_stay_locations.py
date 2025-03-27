from timegeo.models.stays import Stays

def main():
    longitude = [-99.3, -98.7]
    latitude = [19.2, 19.7]
    input_path = "/data_1/quadrant/output/filter_partioned/day=2022-11-30"
    output_path = "/data_1/quadrant/output/test_stays.parquet"
    columns = {
        "_c0": "caid",
        "_c2": "latitude",
        "_c3": "longitude",
        "_c5": "utc_timestamp"
    }

    stays = Stays(longitude=longitude, latitude=latitude, columns=columns)
    stays.get_stays(input_path, output_path)
    stays.get_home_work_locations(output_path, "/data_1/quadrant/output")

if __name__ == "__main__":
    main()