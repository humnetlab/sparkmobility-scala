import pandas as pd

def load_tessellation_from_data(filepath):
    """
    Extract from processed location distribution dataframe the hexagonal shp boundaries and
    precomputed OD matrix
    Returns (tessellation, weights)
    -------
    """
    df = pd.read_parquet(filepath)
    return df


def synthetic_trips(df):
    # Sort by user and time
    df = df.sort_values(['user', 'datetime']).reset_index(drop=True)

    # For each user, shift 'location' by -1 to get the next location in time
    df['destination'] = df.groupby('user')['location'].shift(-1)

    # Rename the original columns for clarity
    df.rename(columns={'time': 'time_loc1', 'location': 'origin'}, inplace=True)

    # Drop rows where the 'location_2' is NaN (i.e., there's no "next" location)
    df.dropna(subset=['destination'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df
