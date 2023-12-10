import pandas as pd


def calculate_distance_matrix(df)->pd.DataFrame():
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    # Write your logic here
    distance_matrix = df.pivot(index='id_start', columns='id_end', values='distance')

    distance_matrix = distance_matrix.fillna(0)

    distance_matrix = distance_matrix + distance_matrix.T

    for index in distance_matrix.index:
        distance_matrix.at[index, index] = 0

    return distance_matrix



def unroll_distance_matrix(df)->pd.DataFrame():
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    # Write your logic here
    distance_matrix = df.reset_index()

    unrolled_df = pd.melt(distance_matrix, id_vars='id_start', var_name='id_end', value_name='distance')

    unrolled_df = unrolled_df[unrolled_df['id_start'] != unrolled_df['id_end']]

    unrolled_df = unrolled_df.sort_values(by=['id_start', 'id_end']).reset_index(drop=True)

    return unrolled_df




def find_ids_within_ten_percentage_threshold(df, reference_id)->pd.DataFrame():
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    # Write your logic here
    reference_avg_distance = df[df['id_start'] == reference_id]['distance'].mean()

    threshold = 0.1 * reference_avg_distance

    filtered_ids = df.groupby('id_start')['distance'].mean()
    filtered_ids = filtered_ids[(filtered_ids >= (reference_avg_distance - threshold)) &
                                (filtered_ids <= (reference_avg_distance + threshold))].index.tolist()

    result_df = pd.DataFrame({'id_start': filtered_ids})

    return result_df



def calculate_toll_rate(df)->pd.DataFrame():
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Wrie your logic here
    rate_coefficients = {'moto': 0.8, 'car': 1.2, 'rv': 1.5, 'bus': 2.2, 'truck': 3.6}

    for vehicle_type, rate_coefficient in rate_coefficients.items():
        df[vehicle_type] = df['distance'] * rate_coefficient

    return df



def calculate_time_based_toll_rates(df)->pd.DataFrame():
    """
    Calculate time-based toll rates for different time intervals within a day.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Write your logic here
    time_ranges = [(datetime.time(0, 0, 0), datetime.time(10, 0, 0)),
                   (datetime.time(10, 0, 0), datetime.time(18, 0, 0)),
                   (datetime.time(18, 0, 0), datetime.time(23, 59, 59))]

    discount_factors_weekdays = [0.8, 1.2, 0.8]
    discount_factor_weekends = 0.7

    for start_time, end_time in time_ranges:
        for day in range(7):
            start_day = (datetime.datetime.strptime('2023-01-01', '%Y-%m-%d') + datetime.timedelta(days=day)).strftime(
                '%A')
            end_day = start_day

            mask = ((df['start_time'] >= start_time) & (df['start_time'] <= end_time) &
                    (df['end_time'] >= start_time) & (df['end_time'] <= end_time) &
                    (df['start_day'] == start_day) & (df['end_day'] == end_day))

            if day < 5:
                df.loc[mask, ['moto', 'car', 'rv', 'bus', 'truck']] *= discount_factors_weekdays[
                    time_ranges.index((start_time, end_time))]
            else:  
                df.loc[mask, ['moto', 'car', 'rv', 'bus', 'truck']] *= discount_factor_weekends

    return df
