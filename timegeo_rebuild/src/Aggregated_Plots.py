import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
from matplotlib import rc

from collections import Counter as counter
from datetime import datetime

# 5_MapReduceInput/AggregatedPlots/1-type_of_trip_hourly.py
def get_simulation_trip_counts(mapped_dir='./Mapped/'):
    """
    Fully replicate the original script’s logic:
    - Increment timeslot by 1 for each line and wrap at 144.
    - Detect new-user lines (single field) to reset timeslot and count users.
    - Track runningLocation, runningCoordinates, and work_flag.
    - Increment HBW, HBO, NHB based on state transitions.
    - Record work begin/end timestamps and commuter counts.
    """
    # List only simulationResults_ files
    filenames = [x for x in os.listdir(mapped_dir) if 'simulationResults_' in x]
    hbw = [0] * 24   # Home-to-work counts per hour
    hbo = [0] * 24   # Home-to-other counts per hour
    nhb = [0] * 24   # Non-home/work counts per hour

    numUsers = 0
    numUsersWork = 0
    work_flag = False
    workBeginTimestamps = []
    workEndTimestamps = []

    for filename in filenames:
        path = os.path.join(mapped_dir, filename)
        timeslot = 0
        runningCoordinates = []
        runningLocation = None

        with open(path, 'r') as f:
            for raw in f:
                # 1) Advance and wrap the timeslot
                timeslot = (timeslot + 1) % 144

                parts = raw.strip().split(' ')

                # 2) New user line: only userID present
                if len(parts) == 1:
                    numUsers += 1
                    runningLocation = 'h'
                    timeslot = 0
                    if work_flag:
                        numUsersWork += 1
                        work_flag = False

                # 3) Same location as before
                elif parts[0] == runningLocation:
                    # Initialize runningCoordinates on first visit
                    if not runningCoordinates:
                        runningCoordinates = [parts[1], parts[2]]
                    # If still 'o' but coords changed, count NHB
                    if parts[0] == 'o' and runningCoordinates != [parts[0], parts[1]]:
                        nhb[(timeslot - 1) // 6] += 1
                    runningCoordinates = [parts[0], parts[1]]

                # 4) Transition to 'h' (home)
                elif parts[0] == 'h':
                    if runningLocation == 'o':
                        hbo[(timeslot - 1) // 6] += 1
                    elif runningLocation == 'w':
                        work_flag = True
                        hbw[(timeslot - 1) // 6] += 1
                        workEndTimestamps.append(timeslot / 6.0)

                # 5) Transition to 'w' (work)
                elif parts[0] == 'w':
                    work_flag = True
                    workBeginTimestamps.append(timeslot / 6.0)
                    if runningLocation == 'o':
                        nhb[(timeslot - 1) // 6] += 1
                    elif runningLocation == 'h':
                        hbw[(timeslot - 1) // 6] += 1

                # 6) Transition to 'o' (other)
                elif parts[0] == 'o':
                    if runningLocation == 'h':
                        hbo[(timeslot - 1) // 6] += 1
                    elif runningLocation == 'w':
                        work_flag = True
                        workEndTimestamps.append(timeslot / 6.0)
                        nhb[(timeslot - 1) // 6] += 1

                else:
                    # Unexpected identifier
                    print('Unidentified:', parts[0])

                # Update runningLocation for next iteration
                runningLocation = parts[0]

    total = [hbw[i] + hbo[i] + nhb[i] for i in range(24)]
    return hbw, hbo, nhb, total, numUsers, numUsersWork


def plot_hourly_trip_counts(
    mapped_dir='./Mapped/',
    output='./1-HourlyTripCount.png'
):
    """
    Replicate the original script’s printing and plotting:
    - Print HBW, HBO, NHB, and total arrays.
    - Print total users, commuter users, and non-commuter users.
    - Plot the four series with specified colors and markers.
    """
    hbw, hbo, nhb, total, numUsers, numUsersWork = \
        get_simulation_trip_counts(mapped_dir)

    # Print counts exactly as in the original script
    print('HBW = ' + str(hbw))
    print('HBO = ' + str(hbo))
    print('NHB = ' + str(nhb))
    print('Total = ' + str(total))
    print('Number of users : ' + str(numUsers))
    print('Number of commuter users : ' + str(numUsersWork))
    print('Number of noncommuter users : ' +
          str(numUsers - numUsersWork))

    # Plot each series with the same colors and marker style
    plt.figure()
    plt.plot(hbw, marker='o', color='b', label='HBW')
    plt.plot(hbo, marker='o', color='g', label='HBO')
    plt.plot(nhb, marker='o', color='r', label='NHB')
    plt.plot(total, marker='o', color='k', label='All')
    plt.legend(loc='upper right')
    plt.xlim(0, 24)
    plt.xlabel('Time of day')
    plt.ylabel('Number of trips')
    plt.savefig(output)
    plt.close()


def plot_dept_validation(
    mapped_dir='./results/Simulation/Mapped/',
    nhts_file='./data/NHTSDep.txt',
    mts_file='./data/NHTSDep.txt',
    output='./results/figs/FigS_dept_validation.png',
    fsize=40,
    msize=20,
    alpha_=0.7,
    lw=3,
    figsize=(24, 16)
):
    """
    Plot department-validation comparison subplots (HBW, HBO, NHB, All) against NHTS/MTS baselines.
    """
    # Retrieve simulation counts
    hbw, hbo, nhb, total = get_simulation_trip_counts(mapped_dir)

    # Load NHTS departure data (comma-separated)
    df1_hw = pd.read_csv(nhts_file, sep=',', header=None)
    df1_hw.columns = ['hour', 'hbw', 'hbo', 'nhb', 'hbw_we', 'hbo_we', 'nhb_we']
    df1_hw.drop(['hbw_we', 'hbo_we', 'nhb_we'], axis=1, inplace=True)
    df1_hw['all'] = df1_hw['hbw'] + df1_hw['hbo'] + df1_hw['nhb']

    # Load MTS departure data (space-separated)
    df2_hw = pd.read_csv(mts_file, sep=' ', header=None)
    df2_hw.columns = ['hour', 'hbw_b', 'hbo_b', 'nhb_b', 'all_b']

    # Merge baselines on hour
    df_hw = pd.merge(df1_hw, df2_hw, on='hour', how='inner')

    # Add simulation results to DataFrame
    df_hw['hbw_tg'] = pd.Series(hbw, index=df_hw.index)
    df_hw['hbo_tg'] = pd.Series(hbo, index=df_hw.index)
    df_hw['nhb_tg'] = pd.Series(nhb, index=df_hw.index)
    df_hw['all_tg'] = pd.Series(total, index=df_hw.index)

    # Normalize each series
    for col in ['hbw', 'hbo', 'nhb', 'all']:
        df_hw[f'{col}_r'] = df_hw[col] / df_hw[col].sum()
    for col in ['hbw_b', 'hbo_b', 'nhb_b', 'all_b']:
        df_hw[f'{col}_r'] = df_hw[col] / df_hw[col].sum()
    for col in ['hbw_tg', 'hbo_tg', 'nhb_tg', 'all_tg']:
        df_hw[f'{col}_r'] = df_hw[col] / df_hw[col].sum()

    # Styling for plots
    sns.set_style('ticks')
    rc('font', **{'family': 'serif', 'serif': ['Times']})
    rc('text', usetex=True)

    # Create 2x2 subplots
    fig = plt.figure(figsize=figsize)
    title_ = ['HBW', 'HBO', 'NHB', 'All']
    label_ = ['2009 NHTS data', '2010 MTS data', 'TimeGeo Simulation']

    for i in range(4):
        X = df_hw['hour'].tolist()
        Y0 = df_hw.iloc[:, i + 13].tolist()
        Y1 = df_hw.iloc[:, i + 17].tolist()
        Y2 = df_hw.iloc[:, i + 21].tolist()

        ax = fig.add_subplot(2, 2, i + 1)
        ax.tick_params(axis='both', which='major', labelsize=fsize)
        ax.plot(X, Y0, 'c-o', linewidth=lw, label=label_[0], markersize=msize, alpha=alpha_)
        ax.plot(X, Y1, 'g-o', linewidth=lw, label=label_[1], markersize=msize, alpha=alpha_)
        ax.plot(X, Y2, 'r-d', linewidth=lw, label=label_[2], markersize=msize, alpha=alpha_)
        ax.tick_params(labelsize=fsize)
        ax.set_xticks(list(range(0, 24, 2)))
        ax.set_xlim(0, 24)
        ax.set_ylim(0, 0.15)
        if i in (2, 3):
            ax.set_xlabel('Departure time \\n (hour)', fontsize=fsize)
        if i in (0, 2):
            ax.set_ylabel('Frequency', fontsize=fsize)
        ax.set_title(title_[i], fontsize=fsize * 1.5)
        if i < 3:
            ax.legend(loc=2, prop={'size': fsize * 0.7})

    sns.despine()
    plt.savefig(output, format='png', dpi=300)
    plt.close()



# 5_MapReduceInput/AggregatedPlots/2-StayDurations.py

import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from collections import Counter as counter
import numpy as np
from datetime import datetime


def array_to_cdf(array):
    """
    Convert an array of values into its PDF represented as keys and probabilities.
    """
    c = counter(array)
    k = sorted(c.keys())
    total = sum(c.values())
    v = [float(c[key]) for key in k]
    pdf = [x / total for x in v]
    return [k, pdf]


def load_simulation_cdf(mapped_dir='./results/Simulation/Mapped/', filename_pattern='simulationResults_', max_duration=36, resolution=6):
    """
    Read simulation result files from a directory, compute stay durations, and return their PDF as CDF.

    Parameters:
        mapped_dir (str): Directory containing simulation result files.
        filename_pattern (str): Substring to filter simulation files.
        max_duration (float): Maximum duration (in hours) to include.
        resolution (int): Number of time slots per hour (e.g., 6 for 10-minute slots).

    Returns:
        list: Two lists [durations, pdf] representing the simulation PDF.
    """
    users_stay_durations = []
    running_location = None
    timeslot = 0
    trip_start_slot = None

    filenames = os.listdir(mapped_dir)
    filenames = [x for x in filenames if filename_pattern in x]

    for filename in filenames:
        with open(os.path.join(mapped_dir, filename), 'r') as f:
            for line in f:
                timeslot += 1
                parts = line.strip().split(' ')

                # New sequence if single entry
                if len(parts) == 1:
                    running_location = None
                    timeslot = 0
                    trip_start_slot = None
                elif (parts[1], parts[2]) == running_location:
                    # Same location, continue
                    pass
                else:
                    # Location change: record stay duration
                    if running_location is not None:
                        trip_end_slot = timeslot
                        duration = float(trip_end_slot - trip_start_slot) / resolution
                        if duration < max_duration:
                            users_stay_durations.append(duration)
                    running_location = (parts[1], parts[2])
                    trip_start_slot = timeslot

    return array_to_cdf(users_stay_durations)


def load_cdr_cdf(cdr_file='./results/SRFiltered_to_SimInput/FAUsers_Cleaned_Formatted.txt', max_duration=36, resolution=6):
    """
    Read CDR data file, compute inter-event durations, and return their PDF as CDF.

    Parameters:
        cdr_file (str): Path to the CDR formatted data file.
        max_duration (float): Maximum duration (in hours) to include.
        resolution (int): Rounding resolution per hour (e.g., 6 for 10-minute slots).

    Returns:
        list: Two lists [durations, pdf] representing the CDR PDF.
    """
    cdr_durations = []

    with open(cdr_file, 'r') as f:
        # Read first line
        fields = f.readline().strip().split(' ')
        running_user = fields[0]
        running_day = datetime.strptime(fields[1], '%Y-%m-%d')
        running_time = float(fields[2])

        for line in f:
            parts = line.strip().split(' ')
            day = datetime.strptime(parts[1], '%Y-%m-%d')
            user = parts[0]
            time = float(parts[2])

            if user == running_user:
                num_days = (day - running_day).days
                duration = num_days * 24.0 - running_time + time
                cdr_durations.append(duration)
            running_user = user
            running_day = day
            running_time = time

    # Filter and quantize durations
    cdr_durations = [x for x in cdr_durations if x < max_duration]
    cdr_durations = [x - x % (1.0 / resolution) for x in cdr_durations]

    return array_to_cdf(cdr_durations)


def modify_cdf(cdf):
    """
    Modify a CDF to create a stepwise representation for plotting.

    Parameters:
        cdf (list): [values, pdf] representing an empirical PDF.

    Returns:
        list: [expanded_values, expanded_pdf] for step plot.
    """
    c = [[], []]
    values, pdf = cdf

    for i in range(len(values) - 1):
        c[0].append(values[i])
        c[1].append(pdf[i])
        c[0].append(values[i + 1])
        c[1].append(pdf[i])

    # Prepend zero
    c = [[0] + c[0], [0] + c[1]]
    # Append last point
    c = [c[0] + [values[-1]], c[1] + [pdf[-1]]]

    return c


def plot_stay_durations(sim_cdf, cdr_cdf, 
                        output_file='./results/figs/2-StayDuration_All.png', 
                        xlim=(0, 24), ylim=(0.0001, 1), figsize=(4, 3)):
    """
    Plot simulation and CDR stay duration PDFs on a log-scaled Y axis.

    Parameters:
        sim_cdf (list): [values, pdf] for simulation data.
        cdr_cdf (list): [values, pdf] for CDR data.
        output_file (str): Filename for saving the figure.
        xlim (tuple): X-axis limits.
        ylim (tuple): Y-axis limits.
        figsize (tuple): Figure size in inches.
    """
    # Preserve original CDR CDF and simulation CDF
    modified_cdf = cdr_cdf
    simulation_cdf = sim_cdf

    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(1, 1, 1)

    ax.set_yscale('log')
    plt.xlim(*xlim)

    # Scatter CDR data
    ax.scatter(modified_cdf[0], modified_cdf[1], color='g', marker='o', \
            facecolor='white', label='Observed', s=20)
    # Plot simulation PDF
    ax.plot(simulation_cdf[0], simulation_cdf[1], color='g', label='Simulated')

    # Output durations to console
    print("Stay Durations")
    print(simulation_cdf[0], simulation_cdf[1])

    plt.ylim(*ylim)
    plt.xlabel('Duration of Stay (h)')
    plt.ylabel('PDF')
    ax.legend(loc='upper right')
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()




# 5_MapReduceInput/AggregatedPlots/3-DurationDist.py

from math import radians, cos, sin, asin, sqrt, ceil

def parse_simulation_results(mapped_dir):
    """Parse simulation results from mapped directory and compute daily location counts and trip distances."""
    usersDailyLocationCount = {}
    usersTripDistances = {}
    filenames = [x for x in os.listdir(mapped_dir) if 'simulationResults_' in x]
    for filename in filenames:
        timeslot = 0
        with open(os.path.join(mapped_dir, filename), 'r') as f:
            for line in f:
                timeslot += 1
                line = line.strip()
                day = timeslot // 144  # Integer division for day index
                line = line.split(' ')
                if len(line) == 1:
                    runningDay = 0
                    userId = line[0].split('-')[1]
                    usersDailyLocationCount[userId] = [[]]
                    usersTripDistances[userId] = []
                    runningLocation = None
                    timeslot = 0
                elif [line[1], line[2]] == runningLocation:
                    pass
                else:
                    runningLocation = (line[1], line[2])
                    dloc = (float(line[1]), float(line[2]))
                    if len(usersTripDistances[userId]) == 0:
                        usersTripDistances[userId].append(dloc)
                    elif usersTripDistances[userId][-1] != dloc:
                        usersTripDistances[userId].append(dloc)
                    if runningDay == day:
                        usersDailyLocationCount[userId][-1].append(runningLocation)
                    else:
                        for _ in range(day - runningDay):
                            usersDailyLocationCount[userId].append([])
                        usersDailyLocationCount[userId][-1].append(runningLocation)
                        runningDay = day
    # Finalize daily location counts and convert trip distances
    for k in usersDailyLocationCount.keys():
        usersDailyLocationCount[k] = [len(set(x)) for x in usersDailyLocationCount[k]]
        usersTripDistances[k] = getTripDistances(usersTripDistances[k])
    return usersDailyLocationCount, usersTripDistances

def parse_observed_data(observed_file):
    """Parse observed formatted FAUsers data and compute observed user1 location counts and trip distances."""
    user1ObsLocationCount = [[]]
    user1ObsTripDistances = []
    with open(observed_file, 'r') as f:
        l = f.readline().strip().split(' ')
        currentUser = l[0]
        runningUser = l[0]
        runningDay = datetime.strptime(l[1], '%Y-%m-%d')
        runningTime = float(l[2])
        for l in f:
            l = l.strip().split(' ')
            day = datetime.strptime(l[1], '%Y-%m-%d')
            currentUser = l[0]
            time = float(l[2])
            if currentUser != runningUser:
                runningUser = l[0]
            else:
                numdays = (day - runningDay).days
                duration = numdays * 24. - runningTime + time
                for _ in range(numdays):
                    user1ObsLocationCount.append([])
                loc = (float(l[4]), float(l[5]))
                user1ObsLocationCount[-1].append(loc)
                user1ObsTripDistances.append(loc)
            runningDay = day
            runningTime = time
    user1ObsLocationCount = [len(set(x)) for x in user1ObsLocationCount]
    user1ObsTripDistances = getTripDistances(user1ObsTripDistances)
    return user1ObsLocationCount, user1ObsTripDistances

def haversine(lon1, lat1, lon2, lat2):
    """Calculate the great-circle distance between two points on Earth."""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat * 0.5)**2 + cos(lat1) * cos(lat2) * sin(dlon * 0.5)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of Earth in kilometers. Use 3956 for miles
    return c * r

def getTripDistances(locArray):
    """Compute distances between consecutive locations."""
    dists = []
    for i in range(len(locArray) - 1):
        dists.append(haversine(locArray[i][0], locArray[i][1],
                               locArray[i+1][0], locArray[i+1][1]))
    return dists

def plot_trip_distance(usersTripDistances, user1ObsTripDistances, 
                       output_file='./results/figs/3-TripDistance.png'):
    """Plot trip distance distributions for observed and simulated data."""
    user1ObsTripDistancesCeil = [ceil(x) for x in user1ObsTripDistances]
    u1_obsDist = counter(user1ObsTripDistancesCeil)
    total = sum(u1_obsDist.values())
    for k in u1_obsDist:
        u1_obsDist[k] = float(u1_obsDist[k]) / total

    # Flatten simulated trip distances
    simDistList = []
    for v in usersTripDistances.values():
        simDistList.extend(v)
    simDistList = [ceil(x) for x in simDistList]
    u1_simDist = counter(simDistList)
    total = sum(u1_simDist.values())
    for k in u1_simDist:
        u1_simDist[k] = float(u1_simDist[k]) / total
    m = max(u1_simDist.keys())
    u1_simDist[m+5] = 0

    fig = plt.figure(figsize=(4, 3))
    ax = fig.add_subplot(1, 1, 1)
    ax.set_yscale('log')
    ax.set_xscale('log')

    # observed scatter in green with white fill
    ax.scatter(
        u1_obsDist.keys(), 
        u1_obsDist.values(),
        color='g',
        marker='o',
        facecolor='white',
        s=20,
        label='Observed'
    )
    # simulated line in green
    ax.plot(
        sorted(u1_simDist.keys()),
        [u1_simDist[k] for k in sorted(u1_simDist.keys())],
        color='g',
        label='Simulated'
    )

    ax.legend()

    print("Trip Distance")
    print(sorted(u1_simDist.keys()), [u1_simDist[k] for k in sorted(u1_simDist.keys())])
    plt.xlim(0.9, 100)
    plt.ylim(0.0001, 1)
    plt.xlabel('Trip Distance, r (km)')
    plt.ylabel('P(r)')
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()



##------------
# 5_MapReduceInput/AggregatedPlots/4-VisitedLocCount.py


def plot_daily_visited_locations(
    #simulation_input_path='../../simulationInput.txt',
    mapped_dir='./results/Simulation/Mapped/',
    obs_input_path='./results/SRFiltered_to_SimInput/FAUsers_Cleaned_Formatted.txt',
    output_path='./results/figs/4-numVisitedLocations'
):
    """Plot the distribution of daily visited locations from observed and simulated data."""
    # Read non-commuters from simulation input
    #nonCommuters = []
    #with open(simulation_input_path, 'r') as f:
    #    for line in f:
    #        line = line.split(' ')
    #        if line[1] == '0':
    #            nonCommuters.append(line[0])

    # List simulation result files
    filenames = os.listdir(mapped_dir)
    filenames = [x for x in filenames if 'simulationResults_' in x]

    usersDailyLocationCount = {}

    for filename in filenames:
        #print filename
        with open(os.path.join(mapped_dir, filename), 'r') as f:
            for line in f:
                line = line.strip()
                line = line.split(' ')
                if len(line) == 1:
                    runningDay = 0
                    userId = line[0].split('-')[1]
                    usersDailyLocationCount[userId] = []
                    runningLocation = None
                elif [line[1], line[2]] == runningLocation:
                    pass
                else:
                    runningLocation = (line[1], line[2])
                    usersDailyLocationCount[userId].append(runningLocation)

    for k in usersDailyLocationCount.keys():
        usersDailyLocationCount[k] = len(set(usersDailyLocationCount[k])) - 1

    ############### Simulation Analysis Over ###############

    user1ObsLocationCount = [[]]
    with open(obs_input_path, 'r') as f:
        l = f.readline()
        l = l.strip()
        l = l.split(' ')
        currentUser = l[0]
        runningUser = l[0]
        runningDay = datetime.strptime(l[1], '%Y-%m-%d')
        runningTime = float(l[2])
        for l in f:
            l = l.strip()
            l = l.split(' ')
            day = datetime.strptime(l[1], '%Y-%m-%d')
            currentUser = l[0]
            time = float(l[2])
            if currentUser != runningUser:
                runningUser = l[0]
            else:
                numdays = (day - runningDay).days
                duration = numdays * 24. - runningTime + time
                for i in range(numdays):
                    user1ObsLocationCount.append([])
                loc = (float(l[4]), float(l[5]))
                user1ObsLocationCount[-1].append(loc)
            runningDay = day
            runningTime = time

    user1ObsLocationCount = [len(set(x)) for x in user1ObsLocationCount]

    # Plots for Number of locations visited

    c1_count = counter(user1ObsLocationCount)
    total = sum(c1_count.values())
    for k in c1_count:
        c1_count[k] = float(c1_count[k]) / total

    usersDailyLocationCount1 = []
    for v in usersDailyLocationCount.values():
        usersDailyLocationCount1.append(v)

    usersDailyLocationCount = usersDailyLocationCount1

    c1_simCount = counter(usersDailyLocationCount)
    total = sum(c1_simCount.values())
    for k in c1_simCount:
        c1_simCount[k] = float(c1_simCount[k]) / total

    fig = plt.figure(figsize=(4,3))
    ax = fig.add_subplot(1,1,1)
    ax.set_yscale('log')

    ax.scatter(
        c1_count.keys(), c1_count.values(), color='g', marker='o', \
        facecolor='white', label='Observed', s=20
    )
    ax.plot(
        sorted(c1_simCount.keys()), [c1_simCount[k] for k in sorted(c1_simCount.keys())], \
        color='g', label='Simulated'
    )

    print("Daily Visited locations")
    print(sorted(c1_simCount.keys()), [c1_simCount[k] for k in sorted(c1_simCount.keys())])

    ax.legend()
    plt.xlim(0,10)
    plt.ylim(0.0005, 1)
    plt.xticks(range(1,10))
    plt.xlabel('Daily visited locations, N')
    plt.ylabel('P(N)')
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()




# 5_MapReduceInput/AggregatedPlots/5-frequency.py

def plot_location_rank(
    mapped_dir='./results/Simulation/Mapped/',
    sim_input_file='./results/SRFiltered_to_SimInput/FAUsers_Cleaned_Formatted.txt',
    output_file='./results/figs/5-LocationRank-User1.png',
    top_n=50,
):
    """
    Encapsulated function to compute visit frequency ranks for all users and a specific user group,
    and plot the aggregated rank-frequency distribution on log-log scales.

    Parameters:
        mapped_dir (str): Path to directory containing 'simulationResults_' files.
        sim_input_file (str): Path to filtered simulation input file for specific users.
        output_file (str): Filename for saving the output plot.
        top_n (int): Number of top locations to include in rank-frequency analysis.
    """
    # Gather filenames matching pattern
    filenames = os.listdir(mapped_dir)
    filenames = [x for x in filenames if 'simulationResults_' in x]

    # Compute visit lists per user from simulation output
    usersLocations = {}
    for filename in filenames:
        timeslot = 0
        with open(os.path.join(mapped_dir, filename), 'r') as f:
            for line in f:
                timeslot += 1
                parts = line.strip().split(' ')
                if len(parts) == 1:
                    userId = parts[0].split('-')[1]
                    usersLocations[userId] = []
                    runningLocation = None
                elif (parts[1], parts[2]) == runningLocation:
                    # Skip repeated location entries
                    pass
                else:
                    runningLocation = (parts[1], parts[2])
                    usersLocations[userId].append(runningLocation)

    # Convert counts to sorted frequency lists of length top_n
    for k in usersLocations.keys():
        usersLocations[k] = counter(usersLocations[k])
    for k in usersLocations.keys():
        counts = sorted(usersLocations[k].values(), reverse=True)
        # Ensure list length == top_n
        for i in range(top_n):
            if i >= len(counts):
                counts.append(0)
        usersLocations[k] = counts[:top_n]

    # Aggregate frequencies and normalize
    cum_probs = [0] * top_n
    for v in usersLocations.values():
        cum_probs = [sum(x) for x in zip(cum_probs, v)]
    total = sum(cum_probs)
    cum_probs = [float(x) / total for x in cum_probs]

    # Load and process specific user group data
    user1Locations = {}
    with open(sim_input_file, 'r') as f:
        # Read first line and initialize
        first = f.readline().strip().split(' ')
        currentUser = first[0]
        runningUser = currentUser
        runningLocation = (float(first[4]), float(first[5]))
        user1Locations[currentUser] = [runningLocation]
        for line in f:
            parts = line.strip().split(' ')
            # Parse timestamp and location
            _ = datetime.strptime(parts[1], '%Y-%m-%d')
            _ = float(parts[2])
            loc = (float(parts[4]), float(parts[5]))
            uid = parts[0]
            if uid == runningUser:
                user1Locations[uid].append(loc)
            else:
                user1Locations[uid] = [loc]
                runningUser = uid

    # Convert counts to sorted frequency lists of length top_n
    for k in user1Locations.keys():
        user1Locations[k] = counter(user1Locations[k])
    for k in user1Locations.keys():
        counts = sorted(user1Locations[k].values(), reverse=True)
        for i in range(top_n):
            if i >= len(counts):
                counts.append(0)
        user1Locations[k] = counts[:top_n]

    # Aggregate frequencies and normalize for user group
    cum_probs1 = [0] * top_n
    for v in user1Locations.values():
        cum_probs1 = [sum(x) for x in zip(cum_probs1, v)]
    total1 = sum(cum_probs1)
    cum_probs1 = [float(x) / total1 for x in cum_probs1]

    # Plot on log-log scales
    fig = plt.figure(figsize=(4, 3))
    ax = fig.add_subplot(1, 1, 1)
    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.scatter(range(len(cum_probs1)), cum_probs1, color='g', marker='s', facecolor='white', label='Observed', s=20)
    ax.plot(range(len(cum_probs)), cum_probs, color='g', label='Simulated')
    ax.legend()
    plt.xlim(1, top_n)
    plt.ylim(0.002, 1)
    plt.xlabel('Lth most visited location')
    plt.ylabel('f(L)')
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()


# 5_MapReduceInput/AggregatedPlots/6-TimeOfDay.py
def count_time_periods(mapped_dir='./results/Simulation/Mapped/'):
    """
    Count location transition events falling into time-of-day segments (AM, MD, PM, RD)
    based on simulation results files.

    Parameters:
        mapped_dir (str): Path to directory containing 'simulationResults_' files.
    """
    # List filenames matching the simulation pattern
    filenames = os.listdir(mapped_dir)
    filenames = [x for x in filenames if 'simulationResults_' in x]

    # Initialize counters for time segments
    am = 0
    md = 0
    pm = 0
    rd = 0

    # Process each simulation output file
    for filename in filenames:
        with open(os.path.join(mapped_dir, filename), 'r') as f:
            for line in f:
                line = line.split(' ')
                if len(line) == 1:
                    runningLocation = 'h'
                    runningIndex = 0
                elif line[0] == runningLocation:
                    runningIndex += 1
                else:
                    runningIndex += 1
                    runningIndex = runningIndex % 144
                    # Classify into time segments based on index
                    if runningIndex >= 0 and runningIndex <= 36:
                        rd += 1
                    elif runningIndex > 36 and runningIndex <= 54:
                        am += 1
                    elif runningIndex > 54 and runningIndex <= 90:
                        md += 1
                    elif runningIndex > 90 and runningIndex <= 114:
                        pm += 1
                    elif runningIndex > 114:
                        rd += 1
                    else:
                        print('Unidentified')
                        print(runningIndex)
                    runningLocation = line[0]

    # Compute total events
    total = float(am + pm + md + rd)

    # Print counts and normalized ratios
    print('AM = ' + str(am))
    print('PM = ' + str(pm))
    print('MD = ' + str(md))
    print('RD = ' + str(rd))
    print('Total = ' + str(total))

    print(am / total)
    print(pm / total)
    print(md / total)
    print(rd / total)



# 5_MapReduceInput/AggregatedPlots/7-TypeOfTrip.py
def count_mode_transitions(mapped_dir='./results/Simulation/Mapped/'):
    """
    Count the number of transitions between home (h), work (w), and other (o) locations
    from simulation results files, and print counts for HBW, HBO, and NHB.

    Parameters:
        mapped_dir (str): Path to directory containing 'simulationResults_' files.
    """
    # List filenames matching the simulation pattern
    filenames = os.listdir(mapped_dir)
    filenames = [x for x in filenames if 'simulationResults_' in x]

    # Initialize counters
    hbw = 0
    hbo = 0
    nhb = 0

    # Process each simulation output file
    for filename in filenames:
        with open(os.path.join(mapped_dir, filename), 'r') as f:
            for line in f:
                parts = line.split(' ')
                if len(parts) == 1:
                    runningLocation = 'h'
                elif parts[0] == runningLocation:
                    # No transition
                    pass
                elif parts[0] == 'h':
                    if runningLocation == 'o':
                        hbo += 1
                    elif runningLocation == 'w':
                        hbw += 1
                    runningLocation = parts[0]
                elif parts[0] == 'w':
                    if runningLocation == 'o':
                        nhb += 1
                    elif runningLocation == 'h':
                        hbw += 1
                    runningLocation = parts[0]
                elif parts[0] == 'o':
                    if runningLocation == 'h':
                        hbo += 1
                    elif runningLocation == 'w':
                        nhb += 1
                    runningLocation = parts[0]
                else:
                    print('Unidentified')

    # Compute total transitions
    total = hbw + hbo + nhb

    # Print results
    print('HBW = ' + str(hbw))
    print('HBO = ' + str(hbo))
    print('NHB = ' + str(nhb))
    print('Total = ' + str(total))
