from tqdm import tqdm
from collections import defaultdict
import datetime
import numpy as np
import pandas as pd
import powerlaw
import math
from bidict import bidict
from empirical_class import ODMatrix
import logging

class EPR:
    # Initialization
    def __init__(self, rho=0.6, gamma=0.21, beta=0.8, tau=17, min_wait_time_minutes=20):
        self.rho = rho
        self.gamma = gamma
        self.tau = tau
        self.beta = beta

        self.location2visits = defaultdict(int)
        self.od_matrix = None
        self.mapper = None

        self.starting_loc = None

        # Minimum waiting time (in hours)
        self.min_wait_time = min_wait_time_minutes / 60.0  # minimum waiting time
        self.starting_loc = None
        self.trajectories_ = []
        self.log_file = None

    def generate(self, start_date, end_date, od_matrix_path, n_agents=1,
                 starting_locations=None,
                 random_state=None, log_file=None, show_progress=True):

        # Calculate the normalized OD Matrix
        od = ODMatrix(od_matrix_path)
        od.load_matrix()
        self.od_matrix = od.matrix
        self.mapper = od.mapping

        # Initiate trajectories
        self.trajectories_ = []
        num_locs = len(self.od_matrix)

        # Loop through agents
        loop = tqdm(range(1, n_agents + 1))

        for agent_id in loop:
            self.location2visits = defaultdict(int)
            # Choose a random starting point
            self.starting_loc = np.random.choice(np.fromiter(range(num_locs), dtype=int), size=1)[0]

            # Generate travel diary for particular agent
            current_date = start_date
            self.trajectories_.append((agent_id, current_date, self.mapper[self.starting_loc]))
            self.location2visits[self.starting_loc] += 1

            waiting_time = self._waiting()
            current_date += datetime.timedelta(hours=waiting_time)

            while current_date < end_date:
                # Choose integer location
                next_location = self._next_location()
                self.trajectories_.append((agent_id, current_date, self.mapper[next_location]))
                self.location2visits[next_location] += 1

                waiting_time = self._waiting()
                current_date += datetime.timedelta(hours=waiting_time)

        # Turn into pandas dataframe
        df = pd.DataFrame(self.trajectories_, columns=["user", "datetime", 'location'])
        df = df.sort_values(by=["user", "datetime"])
        return df

    def _waiting(self):
        time_to_wait = powerlaw.Truncated_Power_Law(xmin=self.min_wait_time,
                                     parameters=[1. + self.beta, 1.0 / self.tau]).generate_random()[0]
        return time_to_wait

    def _next_location(self):
        n_visited_locations = len(self.location2visits)  # number of already visited locations

        if n_visited_locations == 0:
            self._starting_loc = self._preferential_exploration(self.starting_loc)
            return self._starting_loc

        agent_id, current_time, current_location = self.trajectories_[-1]  # the last visited location
        current_location = self.mapper.inv[current_location]    # translate to integer form
        # choose a probability to return or explore
        p_new = np.random.uniform(0, 1)

        if (p_new <= self.rho * math.pow(n_visited_locations, -self.gamma) and n_visited_locations != \
            self.od_matrix.shape[0]) or n_visited_locations == 1:  # choose to return or explore
            # PREFERENTIAL EXPLORATION
            next_location = self._preferential_exploration(current_location)
            return next_location

        else:
            # PREFERENTIAL RETURN
            next_location = self._preferential_return(current_location)
            return next_location

    def _preferential_exploration(self, current_location):
        """
                Choose the new location the agent explores, according to the probabilities
                in the origin-destination matrix.

                Parameters
                ----------
                current_location : int
                    the identifier of the current location of the individual.

                Returns
                -------
                int
                    the identifier of the new location the agent has to explore.
                """

        # previously visited locations:
        prev_locations = np.array(self.location2visits.keys())
        locations = np.arange(len(self.od_matrix[current_location]))
        # locations unvisited
        new_locs = np.setdiff1d(locations, prev_locations)

        # Get the subset of weights corresponding to only the new locations
        weights_subset = self.od_matrix[current_location][new_locs]
        # Normalize this subset so it sums to 1
        weights_subset = weights_subset / weights_subset.sum()

        #weights = self.od_matrix[current_location]
        location = np.random.choice(locations, size=1, p=weights_subset)[0]

        return location

    def _preferential_return(self, current_location):
        next_location = self._weighted_random_selection(current_location)
        return next_location


    def _weighted_random_selection(self, current_location):
        """
        Select a random location given the agent's visitation frequency. Used by the return mechanism.

        Parameters
        ----------
        current_location : int
            identifier of a location.

        Returns
        -------
        int
            a location randomly chosen according to its relevance.
        """
        locations = np.fromiter(self.location2visits.keys(), dtype=int)
        weights = np.fromiter(self.location2visits.values(), dtype=float)

        weights = weights / np.sum(weights)
        location = np.random.choice(locations, size=1, p=weights)
        return int(location[0])