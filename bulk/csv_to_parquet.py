import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def csv_to_parquet(input_filename, output_filename: str):
    source_df = pd.read_csv(input_filename)

    source_table = pa.Table.from_pandas(source_df)

    pq.write_table(source_table, output_filename)


if __name__ == '__main__':
    csv_to_parquet("league_data.csv","league_data.parquet")
    csv_to_parquet("performance_data.csv","performance_data.parquet")
    csv_to_parquet("player_data.csv","player_data.parquet")
    csv_to_parquet("team_data.csv","team_data.parquet")
    csv_to_parquet("team_player_data.csv","team_player_data.parquet")