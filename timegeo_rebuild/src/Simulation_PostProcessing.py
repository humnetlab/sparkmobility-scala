# 5-Simulation/5_MapReduceInput/1_Compressor.py
import os
import pickle
import numpy as np
import pandas as pd
import warnings
from scipy.spatial.distance import cdist

# warnings.filterwarnings('ignore')

def compress_simulation_results(
        input_folder='./results/Simulation/Mapped/', 
        output_folder='./results/Simulation/Compressed/', 
        file_prefix='simulationResults_'
    ):
    """
    Compresses simulation result files in the specified folder that start with a given prefix.

    Parameters:
    - input_folder (str): Path to the input folder.
    - output_folder (str): Path to the folder where compressed files will be saved.
    - file_prefix (str): Prefix of the files to process.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    files = [f for f in os.listdir(input_folder) if file_prefix in f]

    for filename in files:
        compressed_results = []
        running_location = [None, None]
        timeslot = 0

        # Read and process the input file line by line
        with open(os.path.join(input_folder, filename), 'r') as f:
            for line in f:
                timeslot += 1
                l = line.strip().split(' ')

                # Check for single-element lines (user identifiers or special markers)
                if len(l) == 1:
                    compressed_results.append(line)
                    running_location = [None, None]
                    timeslot = 0
                # Record changes in location along with timeslot
                elif [l[1], l[2]] != running_location:
                    compressed_results.append(f"{timeslot} {line}")
                    running_location = [l[1], l[2]]

        # Write compressed results to the output file
        with open(os.path.join(output_folder, filename), 'w') as f:
            for c in compressed_results:
                f.write(c)




def closest_node(node, nodes):
    return cdist([node], nodes).argmin()


def process_user_trajectories(
    datatype: str,
    modeltype: str,
    gps_path: str,
    testids_path: str,
    sim_dir: str,
    output_dir_tg: str,
    output_dir_our: str
):
    """
    Preprocess simulated trajectory data: convert raw .txt files into a
    dictionary saved as a .npy file for model consumption.

    Parameters:
    - datatype: identifier for the dataset, used for naming the output file
    - modeltype: 'tg' to save under output_dir_tg, otherwise under output_dir_our
    - gps_path: path to GPS grid nodes file (.npy)
    - testids_path: path to test user ID list file (.npy)
    - sim_dir: directory containing simulated trajectory .txt files
    - output_dir_tg: output directory for 'tg' modeltype
    - output_dir_our: output directory for other model types
    """
    # Load reference GPS nodes and test user IDs
    gps = np.load(gps_path, allow_pickle=True)  # array of shape (N,2): [lat, lon]
    test_id = np.load(testids_path, allow_pickle=True)

    # List all simulation text files in the directory
    simFiles = [os.path.join(sim_dir, f)
                for f in os.listdir(sim_dir)
                if os.path.isfile(os.path.join(sim_dir, f)) and f.endswith('.txt')]
    simFiles = sorted(simFiles)

    userTraj_point = {}
    userTraj_time = {}

    # Parse each simulation file line by line
    for f in simFiles:
        with open(f, 'r') as data:
            for line in data:
                parts = line.strip().split(' ')
                if len(parts) == 1:
                    # New user ID line: format 'prefix-<perID>'
                    perID = int(parts[0].split('-')[1])
                    if perID not in userTraj_point and perID in test_id:
                        userTraj_point[perID] = []
                        userTraj_time[perID] = []
                else:
                    # Only record data for test users
                    if perID in test_id:
                        timestep = int(parts[0])
                        lon = float(parts[2])
                        lat = float(parts[3])
                        userTraj_point[perID].append([lat, lon])
                        userTraj_time[perID].append(timestep)

    userTraj_sim = {}

    # For each user and each day of the week, map points to GPS nodes
    # and compute stay durations and departure times
    for uid in userTraj_point.keys():
        userTraj_sim[uid] = {}
        for day in range(7):
            loc = []
            sta = []
            tim = []
            times = userTraj_time[uid]
            for i in range(len(times)):
                if times[i] > 1:
                    try:
                        # Stay duration: (next_t - current_t) * 10
                        sta.append((times[i+1] - times[i]) * 10)
                        # Departure time in minutes: (t % 144) * 10
                        tim.append((times[i] % 144) * 10)
                        # Closest GPS node index for the point
                        loc.append(closest_node(userTraj_point[uid][i], gps))
                    except IndexError:
                        # Handle final record: use last timestamp and location
                        tim.append((times[-1] % 144) * 10)
                        loc.append(closest_node(userTraj_point[uid][-1], gps))
                        continue
            userTraj_sim[uid][day] = {
                'loc': np.array(loc),
                'sta': np.array(sta),
                'tim': np.array(tim)
            }

    # Save the processed dictionary to the appropriate output directory
    if modeltype == 'tg':
        os.makedirs(output_dir_tg, exist_ok=True)
        np.save(os.path.join(output_dir_tg, f"{datatype}.npy"), userTraj_sim, allow_pickle=True)
    else:
        os.makedirs(output_dir_our, exist_ok=True)
        np.save(os.path.join(output_dir_our, f"{datatype}.npy"), userTraj_sim, allow_pickle=True)
