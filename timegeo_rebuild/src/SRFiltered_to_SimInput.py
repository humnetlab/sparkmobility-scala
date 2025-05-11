from datetime import datetime

##--------------------------2-Parameters---------------------------
## 3-ParameterValues.py
def decode_and_write_parameters(
    b1_array,
    b2_array,
    commuter_input_path,
    noncommuter_input_path,
    commuter_output_path,
    noncommuter_output_path
    ):
    """
    Decode indexed b1 and b2 values in commuter and non-commuter parameter files,
    replacing them with actual numeric values, and write the results to new files.

    Parameters:
        b1_array (list[int]): Mapping array for b1 values.
        b2_array (list[int]): Mapping array for b2 values.
        commuter_input_path (str): Path to the original commuter parameter file.
        noncommuter_input_path (str): Path to the original non-commuter parameter file.
        commuter_output_path (str): Path to the output commuter parameter file.
        noncommuter_output_path (str): Path to the output non-commuter parameter file.
    """

    def process_file(input_path, output_path, b1_array, b2_array):
        with open(output_path, 'w') as g:
            with open(input_path, 'r') as f:
                for line in f:
                    parts = line.strip().split(' ')
                    if len(parts) < 3:
                        continue  # Skip malformed lines
                    try:
                        # Replace index with actual b1 and b2 values
                        parts[0] = str(b1_array[int(parts[0])])
                        parts[1] = str(b2_array[int(parts[1])])
                    except IndexError:
                        print(f"IndexError in line: {line}")
                        continue
                    g.write(' '.join(parts) + '\n')

    # Process both commuter and non-commuter parameter files
    process_file(commuter_input_path, commuter_output_path, b1_array, b2_array)
    process_file(noncommuter_input_path, noncommuter_output_path, b1_array, b2_array)

    print(f"Decoded parameters written to:\n  {commuter_output_path}\n  {noncommuter_output_path}")






##--------------------------3_SRFiltered_to_SimInput----------------
## 0_removeRedundance.py

def remove_redundant_stays(input_path, output_path):
    '''
    This function removes consecutive stays at the same stay location
    from the file- StayRegionsFiltered.txt
    '''
    with open(output_path, 'w') as g:
        runningUser = ''
        runningLocation = ''
        with open(input_path, 'r') as f:
            for line in f:
                l = line.strip().split(' ') # \t
                #print(l)
                line_parts = line.split(' ') # \t
                line = line_parts[:3] + line_parts[4:]
                #print(line)
                line = ' '.join(line)
                #print('3:', line)
                currentUser = l[0]
                currentLocation = (l[5], l[6])
                #print('currentLocation:', currentLocation)
                if currentUser != runningUser:
                    g.write(line)
                    runningUser = currentUser
                    runningLocation = (l[5], l[6])
                elif currentLocation != runningLocation:
                    g.write(line)
                    runningLocation = currentLocation


## 1_FAUsers.py

def parse_line(line):
    line = line.rstrip().split(' ')
    user = line[0]
    timestamp = int(line[1])
    #location_id = line[2]
    #trip_purpose = line[3]
    location_id = line[3]
    trip_purpose = line[2]
    longitude = line[4]
    latitude = line[5]

    LAtime = datetime.utcfromtimestamp(timestamp)
    date = datetime.strftime(LAtime, '%Y-%m-%d')
    time = float(LAtime.hour) + float(LAtime.minute)/60 + float(LAtime.second)/3600

    return [user, date, time, trip_purpose, longitude, latitude]

def extract_frequent_users(input_path, output_path, num_stays_threshold=15):
    # This function reads a whitespace-delimited stay-region file line by line, counts the number of distinct “stays” per user (incrementing whenever the location coordinates change), and writes out the IDs of all users whose stay count exceeds num_stays_threshold.
    # num_stays_threshold (int, default=15): minimum number of distinct stays a user must have to be included.
    
    frequent_users = []
    linewise_users = []

    with open(input_path, 'r') as f:
        first_line = f.readline()
        if not first_line:
            print("Input file is empty.")
            return

        running_line = parse_line(first_line)
        running_user_stays = 1

        for line in f:
            parsed_line = parse_line(line)
            if parsed_line[0] == running_line[0]:
                # same user
                if (parsed_line[4], parsed_line[5]) != (running_line[4], running_line[5]):
                    running_user_stays += 1
                    linewise_users.append(parsed_line[0])
                running_line = parsed_line[:]
            else:
                if running_user_stays > num_stays_threshold:
                    frequent_users.append(running_line[0])
                running_line = parsed_line[:]
                running_user_stays = 1

    # Write results
    with open(output_path, 'w') as f:
        for user in frequent_users:
            f.write(user + '\n')

    print(f"Saved {len(frequent_users)} users to {output_path}")


## 2_FAUsers_StayRegions.py

def extract_stay_regions_for_frequent_users(fa_users_path, input_path, output_path):
    # This function filters a master stay-region file to include only those records belonging to a predefined list of “frequent” users and writes each user’s unique stay-region entries (no consecutive duplicates) to a new file.
    # [Outputfile] FAUsers_StayRegions.txt: [0-userID, 1-date, 2-time, 3-trip_purpose, 4-lon, 5-lat]

    frequent_users = []
    
    # Read frequent users
    with open(fa_users_path, 'r') as f:
        frequent_users = [line.strip() for line in f]

    if not frequent_users:
        print("No frequent users found.")
        return

    # Process FilteredStayRegions_set.txt
    with open(output_path, 'w') as g:
        with open(input_path, 'r') as f:
            frequent_users_processed = 0
            frequent_user_being_processed = False
            frequent_user = frequent_users[frequent_users_processed]
            previous_line = None

            for line in f:
                parsed = parse_line(line)
                if parsed[0] == frequent_user:
                    frequent_user_being_processed = True
                    if previous_line is None or (previous_line[4], previous_line[5]) != (parsed[4], parsed[5]):
                        g.write(' '.join([str(x) for x in parsed]) + '\n')
                    previous_line = parsed[:]
                else:
                    previous_line = None
                    if frequent_user_being_processed:
                        frequent_user_being_processed = False
                        frequent_users_processed += 1

                    try:
                        frequent_user = frequent_users[frequent_users_processed]
                        if parsed[0] == frequent_user:
                            frequent_user_being_processed = True
                            if previous_line is None or (previous_line[4], previous_line[5]) != (parsed[4], parsed[5]):
                                g.write(' '.join([str(x) for x in parsed]) + '\n')
                            previous_line = parsed[:]
                    except IndexError:
                        # No more users in the list
                        break

    print(f"Filtered stay regions written to: {output_path}")


## 3_Format_SimInput.py

def clean_and_format_fa_users(input_path, output_path):
    # This function reads a pre-filtered stay-region file, re-indexes each user’s unique locations, maps numeric location types to letter codes, removes consecutive duplicate visits, and writes the cleaned, reformatted records to a new file.
    # [Outputfile] FAUsers_Cleaned_Formatted.txt: [0-userID, 1-date, 2-time, 3-trip_purpose['o','h','w'], 4-lon, 5-lat, 6-Location_index]

    running_user = None
    running_user_locations = {}
    running_location_index = 0
    previous_location_index = None

    with open(output_path, 'w') as g:
        with open(input_path, 'r') as f:
            for line in f:
                line = line.strip().split(' ')
                #print(line)
                output_line = line[:3]
                current_user = line[0]
                current_location_type = line[3]
                current_location = (float(line[4]), float(line[5]))

                # Detect user change
                if current_user != running_user:
                    running_user = current_user
                    running_user_locations = {}
                    running_location_index = 0
                    previous_location_index = None

                # Assign location index
                if current_location in running_user_locations:
                    location_index = running_user_locations[current_location]
                else:
                    location_index = running_location_index
                    running_user_locations[current_location] = location_index
                    running_location_index += 1

                # Map trip purpose
                if current_location_type == '0':
                    output_line.append('o')
                elif current_location_type == '1':
                    output_line.append('h')
                elif current_location_type == '2':
                    output_line.append('w')
                else:
                    raise ValueError(f"Unknown location type: {current_location_type}")

                # Append lon, lat, index
                output_line.extend(line[4:])
                output_line.append(str(location_index))

                # Avoid writing duplicate consecutive locations
                if location_index != previous_location_index:
                    g.write(' '.join(output_line) + '\n')
                previous_location_index = location_index

    print(f"Formatted data written to: {output_path}")