import os
import random
from sklearn.mixture import GaussianMixture
import numpy as np
import math
import csv
#import pypr.clustering.gmm as pypr_gmm


# 5_Simulation/1_formatStays.py
def generate_simulation_input(
        input_path='./results/SRFiltered_to_SimInput/FAUsers_Cleaned_Formatted.txt',
        output_path='./results/Simulation/simulation_location.txt'
        ):
    '''
    The objective of this script is to generate input to be
    fed to the simulation script
    The stays are available in the following format:
    [userId, Date, timestamp, locationType, lon, lat, locId]
    The required input to the simulation is of the type:
    [locationId, frequency, lon, lat]
    with a separate line with userId to indicate switch of user
    '''

    with open(input_path, 'r') as f, open(output_path, 'w') as g:
        runningUserId = None
        runningUserLocations = {}
        runningUserLocationsCoordinates = {}

        for line in f:
            line = line.strip().split(' ')
            if len(line) < 7:
                continue  # Skip malformed lines
            userId = line[0]
            locationLabel = line[3]
            longitude = line[4]
            latitude = line[5]
            locationId = line[6]

            # Convert location label
            if locationLabel == 'h':
                locationId = 'h'
            elif locationLabel == 'w':
                locationId = 'w'
            else:
                locationId = str(int(locationId) + 1)

            # If it's a new user, flush previous user's data
            if runningUserId is not None and userId != runningUserId:
                g.write(runningUserId + '\n')
                for loc_id in runningUserLocations:
                    g.write(f"{loc_id} {runningUserLocations[loc_id]} "
                            f"{runningUserLocationsCoordinates[loc_id][0]} "
                            f"{runningUserLocationsCoordinates[loc_id][1]}\n")
                runningUserLocations.clear()
                runningUserLocationsCoordinates.clear()

            # Update current user's data
            runningUserId = userId
            if locationId in runningUserLocations:
                runningUserLocations[locationId] += 1
            else:
                runningUserLocations[locationId] = 1
                runningUserLocationsCoordinates[locationId] = [longitude, latitude]

        # Final flush
        if runningUserId is not None:
            g.write(runningUserId + '\n')
            for loc_id in runningUserLocations:
                g.write(f"{loc_id} {runningUserLocations[loc_id]} "
                        f"{runningUserLocationsCoordinates[loc_id][0]} "
                        f"{runningUserLocationsCoordinates[loc_id][1]}\n")

    print(f"Simulation input written to: {output_path}")




# 5-Simulation/2_formatParameters.py

#def gen_gmm_1sample(cen, cov, mc):
#    while True:
#        s = pypr_gmm.sample_gaussian_mixture(cen, cov, mc, samples=1)
#        if 0 < s[0, 0] < 1440 and 0 < s[0, 1] < 1440:
#            break
#    return [int(s[0, 0]) / 10, int(s[0, 1]) / 10]

def gen_gmm_1sample(cen, cov, mc):
    """
    Replace pypr_gmm.sample_gaussian_mixture using sklearn's GMM.

    Inputs:
        cen: list of 2D means, e.g., [ [x1, y1], [x2, y2], [x3, y3] ]
        cov: list of 2x2 covariance matrices, one per component
        mc: list of mixing coefficients, e.g., [0.3, 0.4, 0.3]

    Output:
        [x, y] where x and y are in range (0, 1440), then divided by 10
    """
    n_components = len(cen)
    gmm = GaussianMixture(n_components=n_components, covariance_type='full')
    
    # sklearn expects:
    # - means_ → array of shape (n_components, 2)
    # - covariances_ → array of shape (n_components, 2, 2)
    # - weights_ → array of shape (n_components,)
    gmm.weights_ = np.array(mc)
    gmm.means_ = np.array(cen)
    gmm.covariances_ = np.array(cov)
    gmm.precisions_cholesky_ = np.linalg.cholesky(np.linalg.inv(gmm.covariances_))

    while True:
        s = gmm.sample(1)[0]  # shape (1, 2)
        if 0 < s[0, 0] < 1440 and 0 < s[0, 1] < 1440:
            break

    return [int(s[0, 0]) / 10, int(s[0, 1]) / 10]


def get_parameters(file_path):
    parameters = []
    with open(file_path, 'r') as f:
        for line in f:
            parts = line.strip().split(' ')
            if len(parts) < 4:
                continue
            b1 = float(parts[0])
            b2 = float(parts[1])
            nw = float(parts[2])
            user = parts[-1]
            if int(user) > 0:
                parameters.append([b1, b2, nw])
    return parameters

def generate_simulation_parameters( 
    commuter_path='./results/Parameters/ParametersCommuters.txt',
    noncommuter_path='./results/Parameters/ParametersNonCommuters.txt',
    output_path='./results/Simulation/simulation_parameter.txt',
    work_prob_weekday=0.829,
    work_prob_weekend=0.354,
    num_days=1,
    reg_prob=0.846, # prob of regular commuters
    gmm_group_index=0   #0: Regular commuters, 1: Flexible commuters
    ):

    """
    # This function builds a single flat file of “simulation-ready” parameter records for both commuter and non-commuter users. It draws each user's time-of-departure and trip-duration from a Gaussian mixture (GMM) model, assigns a random “regular vs. flexible” label, and encodes their daily work schedule over a specified number of days.

    gmm_group_index: int
        Index of GMM behavior group used to generate (ts, dur).
        - 0 = Regular commuters (default)
        - 1 = Flexible commuters (more variance)
    """

    list_par=[
        [
            [([ 474.17242116,  450.36415361]),([ 454.76611398,  540.17150463]),([ 770.23785795,  396.00232714])],
            [
                ([[  8458.74571565,  -9434.51634444], [ -9434.51634444,  36040.22889202]]),
                ([[ 3367.38775228, -1123.19558628], [-1123.19558628,  2680.86063147]]),
                ([[ 48035.69002421, -15435.34143709], [-15435.34143709,  68729.0782976 ]])
            ],
            [0.29480400442746502, 0.53352099305834633, 0.17167500251418938]
        ],
        [
            [([ 453.1255362 ,  544.63138923]),([ 722.8546238 ,  326.65475739]),([ 445.33662957,  550.82705344])],
            [
                ([[ 3748.5636386 , -1087.7059591 ],[-1087.7059591 ,  2962.05884783]]),
                ([[ 53499.35557041,   2503.97833801],[  2503.97833801,  34339.63653221]]),
                ([[  6649.97753593,  -6920.24538877],[ -6920.24538877,  34135.84244881]])
            ],
            [0.47180641829031889, 0.25403472847233199, 0.27415885323734995]
        ]
    ]

    cen, cov, mc = list_par[gmm_group_index] # list_par[0]: Regular commuters; list_par[1]: Flexible commuters

    # Load parameter pools
    COMMUTER_PARAMETERS = get_parameters(commuter_path)
    NONCOMMUTER_PARAMETERS = get_parameters(noncommuter_path)

    WORK_PROB_WEEKDAY = work_prob_weekday
    WORK_PROB_WEEKEND = work_prob_weekend
    NUM_DAYS = num_days

    user_index = 0
    with open(output_path, 'w') as g:

        # Handle commuters
        with open(commuter_path, 'r') as f:
            for line in f:
                parts = line.strip().split(' ')
                if len(parts) < 9:
                    continue
                user_id = parts[8]
                reg = 1 if random.random() < reg_prob else 0
                work_flag = 1
                home_tract = 'homeTract'
                work_tract = 'workTract'

                ts, dur = gen_gmm_1sample(cen, cov, mc)
                beta1, beta2, nw = random.choice(COMMUTER_PARAMETERS)

                user_index += 1
                output = [user_index, user_id, work_flag, home_tract, work_tract, reg, beta1, beta2, nw]

                for i in range(NUM_DAYS):
                    working = random.random()
                    if i % 7 < 5:
                        working = 1 if working < WORK_PROB_WEEKDAY else 0
                    else:
                        working = 1 if working < WORK_PROB_WEEKEND else 0

                    if working:
                        if reg == 1:
                            ts_mean = int(ts)
                            if 0 < ts_mean < 144:
                                ts = random.choice([ts_mean - 1, ts_mean, ts_mean, ts_mean + 1])
                            elif ts_mean == 0:
                                ts = random.choice([ts_mean, ts_mean, ts_mean + 1, ts_mean + 2])
                            elif ts_mean == 144:
                                ts = random.choice([ts_mean, ts_mean, ts_mean - 1, ts_mean - 2])
                            dur_mean = int(dur)
                            dur = random.choice([dur_mean, dur_mean, dur_mean + 1, dur_mean - 1])
                        else:
                            ts, dur = gen_gmm_1sample(cen, cov, mc)
                    else:
                        ts = dur = 0
                    output += [working, round(144 * i + ts), round(dur)]

                g.write(' '.join(str(x) for x in output) + '\n')

        # Handle non-commuters
        with open(noncommuter_path, 'r') as f:
            for line in f:
                parts = line.strip().split(' ')
                if len(parts) < 9:
                    continue
                user_id = parts[8]
                reg = 1 if random.random() < reg_prob else 0
                work_flag = 0
                home_tract = 'homeTract'
                work_tract = 'workTract'
                beta1, beta2, nw = random.choice(NONCOMMUTER_PARAMETERS)

                user_index += 1
                output = [user_index, user_id, work_flag, home_tract, work_tract, reg, beta1, beta2, nw, 0, 0, 0]
                g.write(' '.join(str(x) for x in output) + '\n')

    print(f"Generated simulation parameters at {output_path}")





# 5-Simulation/3_MapInputSynthesis.py
def split_simulation_inputs(
    parameter_path='./results/Simulation/simulation_parameter.txt',
    location_path='./results/Simulation/simulation_location.txt',
    formatted_user_path='./results/SRFiltered_to_SimInput/FAUsers_Cleaned_Formatted.txt',
    output_dir='./results/Simulation',
    num_cpus=16
    ):
    """
    Splits simulation_parameter.txt and simulation_location.txt into chunks by user ID
    for parallel processing (MapReduce-style).

    Parameters:
        parameter_path (str): Path to simulation_parameter.txt
        location_path (str): Path to simulation_location.txt
        formatted_user_path (str): Path to FAUsers_Cleaned_Formatted.txt
        output_dir (str): Root output folder to store chunked input
        num_cpus (int): Number of chunks / CPUs to divide into
    """

    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i+n]

    os.makedirs(os.path.join(output_dir, 'Locations'), exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'Parameters'), exist_ok=True)

    # Step 1: Read userIds from simulation_parameter.txt
    user_ids = []
    with open(parameter_path, 'r') as f:
        for line in f:
            user_ids.append(int(line.split(' ')[1]))

    # Step 2: Read all user IDs from FAUsers_Cleaned_Formatted.txt
    all_user_ids = []
    with open(formatted_user_path, 'r') as f:
        for line in f:
            all_user_ids.append(int(line.split(' ')[0]))

    # Step 3: Intersect and sort
    all_user_ids = sorted(set(all_user_ids) & set(user_ids))
    # prepare two separate lists so we can remove IDs as we process
    all_user_ids_str_loc = [str(k) for k in all_user_ids]
    all_user_ids_str_par = [str(k) for k in all_user_ids]

    # Step 4: Divide user IDs into chunks
    chunk_size = int(math.ceil(float(len(all_user_ids)) / num_cpus))
    user_chunks = list(chunks(all_user_ids, chunk_size))
    user_chunks_set = [list(set(chunk)) for chunk in user_chunks]

    # Step 5: Load locations (only first block per user, like second script)
    user_locations = {}
    with open(location_path, 'r') as f:
        current_user = None
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) == 1:
                uid = parts[0]
                if uid in all_user_ids_str_loc:
                    # remove so we only process the *first* occurrence
                    all_user_ids_str_loc.remove(uid)
                    current_user = uid
                    # initialize and append header line
                    user_locations[current_user] = []
                    user_locations[current_user].append(uid + '\n')
                else:
                    current_user = None
                continue

            if current_user:
                user_locations[current_user].append(' '.join(parts) + '\n')

    # Step 6: Write location chunks
    for i, chunk_set in enumerate(user_chunks_set):
        with open(os.path.join(output_dir, f'Locations/usersLocations_{i}.txt'), 'w') as f:
            for user in chunk_set:
                for loc in user_locations.get(str(user), []):
                    f.write(loc)

    # Step 7: Load parameters (only first line per user)
    user_parameters = {}
    with open(parameter_path, 'r') as f:
        for line in f:
            line_split = line.strip().split(' ')
            uid = line_split[1]
            if uid in all_user_ids_str_par:
                all_user_ids_str_par.remove(uid)
                user_parameters[uid] = ' '.join(line_split) + '\n'

    # Step 8: Write parameter chunks
    for i, chunk_set in enumerate(user_chunks_set):
        with open(os.path.join(output_dir, f'Parameters/usersParameters_{i}.txt'), 'w') as f:
            for user in chunk_set:
                # write exactly the first parameter line (if any)
                f.write(user_parameters.get(str(user), ''))

    print(f"Chunks saved in: {output_dir}")





# 5-Simulation/5_MapReduceInput/dataPreparation.py
"""
Prepare activeness.txt and otherlocation.txt for Mapper.py
__author__ = 'xu'
"""
def activeness(
        noncomm_daily_path='./results/Parameters/NonComm_pt_daily.txt', 
        noncomm_weekly_path='./results/Parameters/NonComm_pt_weekly.txt', 
        comm_daily_path='./results/Parameters/Comm_pt_daily.txt', 
        comm_weekly_path='./results/Parameters/Comm_pt_weekly.txt', 
        output_path='./results/Simulation/activeness.txt'
        ):
    """
    Generate activeness.txt containing activity probabilities for non-commuters and commuters.
    """
    # Non-commuter probabilities
    dailyPt = np.genfromtxt(noncomm_daily_path, delimiter=' ')
    weeklyPt = np.genfromtxt(noncomm_weekly_path, delimiter=' ')
    nonCommuterPt = [d * w for w in weeklyPt for d in dailyPt]

    # Commuter probabilities
    dailyPt = np.genfromtxt(comm_daily_path, delimiter=' ')
    weeklyPt = np.genfromtxt(comm_weekly_path, delimiter=' ')
    
    commuterPt = [d * w for w in weeklyPt for d in dailyPt]

    # Write to output file
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=' ')
        writer.writerow(nonCommuterPt)
        writer.writerow(commuterPt)

def otherLocations(
        input_path='./results/Simulation/simulation_location.txt', 
        output_path='./results/Simulation/otherlocation.txt', 
        sample_fraction=0.02):
    """
    Extract unique user stay locations with a sampling fraction and write to file.

    Parameters:
    - input_path (str): Path to input file containing stay locations.
    - output_path (str): Output file for sampled locations.
    - sample_fraction (float): Probability to include a location (0-1).
    """
    locations = set()
    user_count = 0

    with open(input_path, 'r') as Fin, open(output_path, 'w', newline='') as Fout:
        writer = csv.writer(Fout, delimiter=' ')
        for line in Fin:
            l = line.strip().split(' ')
            if len(l) == 1:
                user_count += 1
                continue
            lat = float(l[3])
            lon = float(l[2])
            if (lat, lon) not in locations:
                locations.add((lat, lon))
                if np.random.rand() <= sample_fraction:
                    writer.writerow([lat, lon])

    print("# of locations:", len(locations))
    print("# of users:", user_count)

