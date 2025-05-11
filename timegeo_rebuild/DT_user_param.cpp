//usage: g++ -O3 DT.cpp -o DT
// ./DT > DT.txt &

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <iostream>
#include <string>
#include <utility>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm> 
#include <math.h>
#include <stdlib.h>
#include <chrono>
#include <filesystem>

using namespace std;

namespace fs = std::filesystem;

void run_DT_simulation(
    const string& input_path,
    const string& output_dir,
    bool commuter_mode,
    int min_num_stay = 2,
    int max_num_stay = 3000,
    double nw_thres = 1.0,
    int slot_interval = 600,
    double rho = 0.6,
    double gamma = -0.21
) {
    // Constants
    const int NaNUM = 8;
    const int LaNUM = 10;
    const int NBINS = 10;
    const int secOneDay = 86400;
    const int winterTimeStart = 1967552400;
    
    // Parameters
    double week_num_thres = 0;   // Could filter out people with too short observation
    
    double n1_arr[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    double n1 = 0;
    int n1_num = 20;
    
    double n2_arr[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    double n2 = 0;
    int n2_num = 20;
    
    // Variables for time tracking if running non-commuter mode
    std::chrono::duration<double> totalDuration = std::chrono::duration<double>::zero();
    
    bool has_work;
    int period = secOneDay * 7; // period
    int rand_range = slot_interval - 50;
    int day_end_at_home = 0;
    int day_not_end_at_home = 0;
    int best_day_end_at_home;
    int best_day_not_end_at_home;
    double best_nw;
    int day_end_at_home_total = 0;
    int day_not_end_at_home_total = 0;
    double home_lat, home_lon, work_lat, work_lon;
    
    vector<double> original_real_dt;
    vector<double> rescaled_real_dt;
    vector<double> original_simu_dt;
    vector<double> rescaled_simu_dt;
    vector<double> original_real_burst;
    vector<double> rescaled_real_burst;
    vector<double> original_simu_burst;
    vector<double> rescaled_simu_burst;
    double original_real_burst_sum = 0;
    double rescaled_real_burst_sum = 0;
    int real_burst_count = 0;
    double original_simu_burst_sum = 0;
    double rescaled_simu_burst_sum = 0;
    int simu_burst_count = 0;
    
    int burst_t;
    int num1;
    int num2;
    int num3;
    int num4 = 0;
    long long num5;
    double num6;
    double num7;
    vector<int> times;
    vector<int> locs;
    vector<int> days;
    vector<long long> locids;
    vector<double> lons;
    vector<double> lats;
    vector<double> daily_activeness;
    vector<double> weekly_activeness;
    vector<double> daily_weekly_activeness;
    vector<int> period_begin_time;
    vector<int> simulated_time;
    vector<int> simulated_at_home;
    string tline;
    int stay_num = 0;
    int person_id = 0;
    int daily_slot_num = secOneDay / slot_interval;
    int slot_num = period / slot_interval; 
    int daily_slot;
    int weekly_slot;
    double nw;
    int na;
    double week_num;
    int current_time;
    int end_time;
    int period_number;
    double randnum;
    int active_time;
    int active_time_limit;
    int addition_times[5] = {0};
    int counter = 0;
    vector<int> work_start;
    vector<int> work_end;
    
    bool at_home = false;
    bool at_work = false;
    
    // Function to correct timestamp to LA time
    auto timeCorrect = [](int timestamp) -> int {
        int timeOff = 0;
        // Time offset logic commented out in original code
        // if (timestamp < winterTimeStart) {
        //     timeOff = 4*3600;
        // }
        // else {
        //     timeOff = 5*3600;
        // }
        int newTime = timestamp - timeOff;
        return newTime;
    };
    
    // Function to calculate probability of other move
    auto getOtherMovePr = [&](int daily_slot, int weekly_slot, int index, double nw) -> double {
        double p_move;
        p_move = n1_arr[index] * nw * daily_activeness[daily_slot] * weekly_activeness[weekly_slot];
        return p_move;
    };
    
    // Function to calculate probability of moving to home
    auto getOtherMoveToHomePr = [&](int daily_slot, int weekly_slot, int index, double nw) -> double {
        double p_move_to_home;
        p_move_to_home = 1 - n2_arr[index] * nw * daily_activeness[daily_slot] * weekly_activeness[weekly_slot];
        return p_move_to_home;
    };
    
    // Calculate burstiness from a vector of times
    auto Burstness = [](vector<double> times) -> double {
        int stay_num = times.size();
        double sum_squared_dev = 0;
        double sum_time = 0;
        double mean_time;
        double mean_squared_dev;
        
        for (int i = 0; i < stay_num; i++) {
            sum_time += times[i];
        }
        
        mean_time = sum_time / stay_num;
        
        for (int i = 0; i < stay_num; i++) {
            sum_squared_dev += (times[i] - mean_time) * (times[i] - mean_time);
        }
        
        mean_squared_dev = sum_squared_dev / stay_num;
        return (sqrt(mean_squared_dev) - mean_time) / (sqrt(mean_squared_dev) + mean_time);
    };
    
    // Rescale time function
    auto RescaleTime = [&](vector<int> times, bool real_time) {
        vector<double> slot_activeness;
        int stay_num = times.size();
        double rescaled_time;
        int record_slot;
        int slot1;
        int slot2;
        int interval_num;
        double dt;
        vector<double> personal_original_real_dt;
        vector<double> personal_rescaled_real_dt;
        vector<double> personal_original_simu_dt;
        vector<double> personal_rescaled_simu_dt;
        
        for (int i = 0; i < slot_num; i++) {
            slot_activeness.push_back(0);
        }
        
        for (int i = 0; i < stay_num; i++) {
            record_slot = (int)((timeCorrect(times[i])) % period / slot_interval);
            slot_activeness[record_slot] += 1;
        }
        
        for (int i = 1; i < stay_num; i++) {
            slot1 = (int)((timeCorrect(times[i-1])) / slot_interval);
            slot2 = (int)((timeCorrect(times[i])) / slot_interval);
            interval_num = (times[i] - times[i-1]) / slot_interval;
            dt = times[i] - times[i-1];
            
            int day1 = (int)((slot1 + 0.0) / daily_slot_num);
            int day2 = (int)((slot2 + 0.0) / daily_slot_num);
            
            if (day2 - day1 > 1) {
                interval_num -= (day2 - day1 - 1) * daily_slot_num;
                dt -= (day2 - day1 - 1) * secOneDay;
                continue; // Don't consider intervals that are too long
            }
            
            rescaled_time = 0;
            for (int j = slot1; j < slot1 + interval_num; j++) {
                rescaled_time += slot_activeness[j % slot_num];
            }
            
            if (real_time) {
                original_real_dt.push_back(dt);
                rescaled_real_dt.push_back(rescaled_time);
                personal_original_real_dt.push_back(dt);
                personal_rescaled_real_dt.push_back(rescaled_time);
            } else {
                original_simu_dt.push_back(dt);
                rescaled_simu_dt.push_back(rescaled_time);
                personal_original_simu_dt.push_back(dt);
                personal_rescaled_simu_dt.push_back(rescaled_time);
            }
        }
        
        if (real_time) {
            original_real_burst_sum += Burstness(personal_original_real_dt);
            rescaled_real_burst_sum += Burstness(personal_rescaled_real_dt);
            real_burst_count++;
        } else {
            original_simu_burst_sum += Burstness(personal_original_simu_dt);
            rescaled_simu_burst_sum += Burstness(personal_rescaled_simu_dt);
            simu_burst_count++;
        }
    };
    
    // Calculate area test statistic between real and simulated time distributions
    auto AreaTestStat = [&](vector<int> real_time, vector<int> simu_time, int n_bins) -> double {
        int slot1;
        int slot2;
        double dt;
        int m1 = real_time.size();
        int m2 = simu_time.size();
        int real_stay_num = m1;
        int simu_stay_num = m2;
        double days_real = (real_time[m1-1] - real_time[0] - 0.0) / double(secOneDay);
        double days_simu = (simu_time[m2-1] - simu_time[0] - 0.0) / double(secOneDay);
        int day_num1 = 1;
        int day_num2 = 1;
        
        // Count number of days in real data
        for (int dd = 1; dd < m1; dd++) {
            int prev_day = (timeCorrect(real_time[dd-1])) / secOneDay;
            int curr_day = (timeCorrect(real_time[dd])) / secOneDay;
            if (prev_day != curr_day) {
                day_num1++;
            }
        }
        
        // Count number of days in simulated data
        for (int dd = 1; dd < m2; dd++) {
            int prev_day = (timeCorrect(simu_time[dd-1])) / secOneDay;
            int curr_day = (timeCorrect(simu_time[dd])) / secOneDay;
            if (prev_day != curr_day) {
                day_num2++;
            }
        }
        
        vector<double> dt_real;
        vector<double> dt_simu;
        
        // Calculate time differences in real data
        for (int i = 0; i < m1 - 1; i++) {
            dt = real_time[i+1] - real_time[i];
            slot1 = (int)((timeCorrect(real_time[i])) % period / slot_interval);
            slot2 = (int)((timeCorrect(real_time[i+1])) % period / slot_interval);
            int day1 = (int)((slot1 + 0.0) / daily_slot_num);
            int day2 = (int)((slot2 + 0.0) / daily_slot_num);
            
            if (day2 - day1 > 1) {
                dt -= (day2 - day1 - 1) * secOneDay;
                days_real = days_real - (day2 - day1 - 1);
                continue; // Don't consider these intervals
            }
            
            dt_real.push_back(dt);
        }
        
        // Calculate time differences in simulated data
        for (int i = 0; i < m2 - 1; i++) {
            dt = simu_time[i+1] - simu_time[i];
            slot1 = (int)((timeCorrect(simu_time[i])) % period / slot_interval);
            slot2 = (int)((timeCorrect(simu_time[i+1])) % period / slot_interval);
            int day1 = (int)((slot1 + 0.0) / daily_slot_num);
            int day2 = (int)((slot2 + 0.0) / daily_slot_num);
            
            if (day2 - day1 > 1) {
                dt -= (day2 - day1 - 1) * secOneDay;
                days_simu = days_simu - (day2 - day1 - 1);
                continue; // Don't consider these intervals
            }
            
            dt_simu.push_back(dt);
        }
        
        m1 = dt_real.size();
        m2 = dt_simu.size();
        
        // Find min and max log times
        double min_lnt = 999;
        double max_lnt = -999;
        
        for (int i = 0; i < m1; i++) {
            if (log(dt_real[i]) > max_lnt) {
                max_lnt = log(dt_real[i]);
            }
            if (log(dt_real[i]) < min_lnt) {
                min_lnt = log(dt_real[i]);
            }
        }
        
        for (int i = 0; i < m2; i++) {
            if (log(dt_simu[i]) > max_lnt) {
                max_lnt = log(dt_simu[i]);
            }
            if (log(dt_simu[i]) < min_lnt) {
                min_lnt = log(dt_simu[i]);
            }
        }
        
        max_lnt += 0.0001;
        min_lnt -= 0.0001;
        double lnt_range = max_lnt - min_lnt;
        
        // Calculate frequency distributions
        vector<double> real_freq;
        vector<double> simu_freq;
        
        for (int i = 0; i < n_bins; i++) {
            real_freq.push_back(0);
            simu_freq.push_back(0);
        }
        
        for (int i = 0; i < m1; i++) {
            int index = (int)((log(dt_real[i]) - min_lnt) / lnt_range * n_bins);
            if (index >= n_bins) index = n_bins - 1;
            real_freq[index] += 1.00 / m1;
        }
        
        for (int i = 0; i < m2; i++) {
            int index = (int)((log(dt_simu[i]) - min_lnt) / lnt_range * n_bins);
            if (index >= n_bins) index = n_bins - 1;
            simu_freq[index] += 1.00 / m2;
        }
        
        // Calculate area test statistic
        double ats = 0;
        for (int i = 0; i < n_bins; i++) {
            ats += fabs(simu_freq[i] - real_freq[i]);
        }
        
        ats += 0.035 * fabs((real_stay_num + 0.0) / day_num1 - (simu_stay_num + 0.0) / day_num2);
        return ats;
    };
    
    // Ensure output sub‑directories exist
    if (commuter_mode) {
        fs::create_directories(output_dir + "/Commuters");
    } else {
        fs::create_directories(output_dir + "/NonCommuters");
    }
    // Set file paths
    string file_name0 = input_path;
    string file_name1, file_name2, file_name3, file_name4, file_name5, file_name6, file_name7;
    
    if (commuter_mode) {
        file_name1 = output_dir + "/Commuters/DTRealCommuters.txt";
        file_name2 = output_dir + "/Commuters/DTSimuCommuters.txt";
        file_name3 = output_dir + "/Commuters/DNRealCommuters.txt";
        file_name4 = output_dir + "/Commuters/DNSimuCommuters.txt";
        file_name5 = output_dir + "/Commuters/SimuLocCommuters.txt";
        file_name6 = output_dir + "/Commuters/ParametersCommuters.txt";
        file_name7 = output_dir + "/Commuters/RealLocCommuters.txt";
    } else {
        file_name1 = output_dir + "/NonCommuters/DTRealNonCommuters.txt";
        file_name2 = output_dir + "/NonCommuters/DTSimuNonCommuters.txt";
        file_name3 = output_dir + "/NonCommuters/DNRealNonCommuters.txt";
        file_name4 = output_dir + "/NonCommuters/DNSimuNonCommuters.txt";
        file_name5 = output_dir + "/NonCommuters/SimuLocNonCommuters.txt";
        file_name6 = output_dir + "/NonCommuters/ParametersNonCommuters.txt";
        file_name7 = output_dir + "/NonCommuters/RealLocNonCommuters.txt";
    }
    
    // Open input and output files
    ifstream fid_in;
    fid_in.open(file_name0.c_str(), ifstream::in);
    
    FILE *fout_id1 = fopen(file_name1.c_str(), "w");
    FILE *fout_id2 = fopen(file_name2.c_str(), "w");
    FILE *fout_id3 = fopen(file_name3.c_str(), "w");
    FILE *fout_id4 = fopen(file_name4.c_str(), "w");
    FILE *fout_id5 = fopen(file_name5.c_str(), "w");
    FILE *fout_id6 = fopen(file_name6.c_str(), "w");
    FILE *fout_id7 = fopen(file_name7.c_str(), "w");
    
    // Initialize activity distributions
    daily_activeness.clear();
    for (int i = 0; i < daily_slot_num; i++) {
        daily_activeness.push_back(0);
    }
    
    weekly_activeness.clear();
    for (int i = 0; i < 7; i++) {
        weekly_activeness.push_back(0);
    }
    
    daily_weekly_activeness.clear();
    for (int i = 0; i < 7 * daily_slot_num; i++) {
        daily_weekly_activeness.push_back(0);
    }
    
    // First pass to calculate activity distributions
    int activity_count = 0;
    has_work = false;
    
    while (getline(fid_in, tline)) {
        stringstream parse_line(tline);
        parse_line >> num1 >> num2 >> num3 >> num4 >> num5 >> num6 >> num7;
        
        // Same person
        if (num1 == person_id) {
            times.push_back(num2);
            locs.push_back(num3);
            days.push_back(num4);
            locids.push_back(num5);
            lons.push_back(num6);
            lats.push_back(num7);
            if (num3 == 2) {
                has_work = true;
            }
        } else {
            // Deal with the old person
            stay_num = times.size();
            if (stay_num > min_num_stay && stay_num < max_num_stay && has_work == commuter_mode) {
                bool valid_sign = true;
                week_num = (times[stay_num-1] - times[0]) / 604800;
                if (week_num < week_num_thres) {
                    valid_sign = false;
                }
                
                for (int i = 0; i < stay_num-1; i++) {
                    if (times[i+1] <= times[i]) {
                        valid_sign = false;
                    }
                }
                
                valid_sign = true;  // This line is in the original code
                
                if (valid_sign == false) {
                    person_id = num1;
                    times.clear();
                    locs.clear();
                    days.clear();
                    locids.clear();
                    lons.clear();
                    lats.clear();
                    has_work = false;
                    continue;
                }
                
                if (++counter > 100000) {
                    break;
                }
                
                // Calculate the weekly and daily trend of non-work activities
                for (int i = 0; i < stay_num; i++) {
                    if (locs[i] != 2) {
                        daily_slot = (int)((timeCorrect(times[i])) % secOneDay / slot_interval);
                        daily_activeness[daily_slot] += 1;
                        
                        weekly_slot = ((int)((timeCorrect(times[i])) / secOneDay)) % 7;
                        weekly_activeness[weekly_slot] += 1;
                        
                        daily_weekly_activeness[weekly_slot * daily_slot_num + daily_slot] += 1;
                        activity_count++;
                    }
                }
            }
            
            // Begin the new person
            person_id = num1;
            times.clear();
            locs.clear();
            locids.clear();
            lons.clear();
            lats.clear();
            days.clear();
            has_work = false;
            
            times.push_back(num2);
            locs.push_back(num3);
            days.push_back(num4);
            locids.push_back(num5);
            lons.push_back(num6);
            lats.push_back(num7);
            if (num3 == 2) {
                has_work = true;
            }
        }
    }
    
    // Normalize activity distributions
    for (int i = 0; i < daily_slot_num; i++) {
        daily_activeness[i] /= activity_count;
    }
    
    for (int i = 0; i < 7; i++) {
        weekly_activeness[i] /= activity_count;
    }
    
    for (int i = 0; i < 7 * daily_slot_num; i++) {
        daily_weekly_activeness[i] /= activity_count;
    }
    
    fid_in.close();
    
    // Write activity patterns to file
    string file_name_daily = output_dir + (commuter_mode ? "/Comm_pt_daily.txt" : "/NonComm_pt_daily.txt");
    FILE *fout_daily = fopen(file_name_daily.c_str(), "w");
    
    string file_name_weekly = output_dir + (commuter_mode ? "/Comm_pt_weekly.txt" : "/NonComm_pt_weekly.txt");
    FILE *fout_weekly = fopen(file_name_weekly.c_str(), "w");
    
    string file_name_daily_weekly = output_dir + (commuter_mode ? "/Comm_pt_daily_weekly.txt" : "/NonComm_pt_daily_weekly.txt");
    FILE *fout_daily_weekly = fopen(file_name_daily_weekly.c_str(), "w");
    
    for (int i = 0; i < daily_activeness.size(); i++) {
        fprintf(fout_daily, "%f\n", daily_activeness[i]);
    }
    
    for (int i = 0; i < weekly_activeness.size(); i++) {
        fprintf(fout_weekly, "%f\n", weekly_activeness[i]);
    }
    
    for (int i = 0; i < daily_weekly_activeness.size(); i++) {
        fprintf(fout_daily_weekly, "%f\n", daily_weekly_activeness[i]);
    }
    
    fclose(fout_daily);
    fclose(fout_weekly);
    fclose(fout_daily_weekly);
    
    // Second pass to simulate trajectories
    ifstream fid_in1;
    fid_in1.open(file_name0.c_str(), ifstream::in);
    person_id = 0;
    counter = 0;
    has_work = false;
    
    while (getline(fid_in1, tline)) {
        stringstream parse_line(tline);
        parse_line >> num1 >> num2 >> num3 >> num4 >> num5 >> num6 >> num7;
        
        // Same person
        if (num1 == person_id) {
            times.push_back(num2);
            locs.push_back(num3);
            days.push_back(num4);
            locids.push_back(num5);
            lons.push_back(num6);
            lats.push_back(num7);
            if (num3 == 2) {
                has_work = true;
            }
        } else {
            // Deal with the old person
            stay_num = times.size();
            if (stay_num > min_num_stay && stay_num < max_num_stay && has_work == commuter_mode) {
                double work_duration = 0;
                double total_duration = times[stay_num-1] - times[0];
                bool valid_sign = true;
                week_num = (times[stay_num-1] - times[0]) / 604800;
                
                if (week_num < week_num_thres) {
                    valid_sign = false;
                }
                
                for (int i = 0; i < stay_num-1; i++) {
                    if (times[i+1] <= times[i]) {
                        valid_sign = false;
                    }
                }
                
                // Identify home and work locations
                home_lon = 0;
                home_lat = 0;
                work_lon = 0;
                work_lat = 0;
                
                for (int i = 0; i < stay_num; i++) {
                    if (locs[i] == 1) {
                        home_lon = lons[i];
                        home_lat = lats[i];
                        break;
                    }
                }
                
                for (int i = 0; i < stay_num; i++) {
                    if (locs[i] == 2) {
                        work_lon = lons[i];
                        work_lat = lats[i];
                        break;
                    }
                }
                
                int day_num = 1;
                int home_based_trip = 1;
                
                for (int dd = 1; dd < stay_num; dd++) {
                    int prev_day = (timeCorrect(times[dd-1])) / secOneDay;
                    int curr_day = (timeCorrect(times[dd])) / secOneDay;
                    if (prev_day != curr_day) {
                        day_num++;
                    }
                }
                
                work_end.clear();
                work_start.clear();
                
                for (int dd = 0; dd < stay_num-1; dd++) {
                    // Trips not going to work
                    if (locs[dd] == 1 && locs[dd+1] != 2) {
                        home_based_trip++;
                    }
                    
                    if (locs[dd] == 2) {
                        work_start.push_back(times[dd]);
                        work_end.push_back(times[dd+1]);
                        work_duration += times[dd+1] - times[dd];
                    }
                }
                
                int max_work_index = work_start.size() - 1;
                
                nw = home_based_trip / (day_num / 7.000);
                if (commuter_mode && total_duration > work_duration) {
                    nw = nw * total_duration / (total_duration - work_duration);
                }
                
                if (nw < nw_thres) {
                    valid_sign = false;
                }
                
                if (valid_sign == false) {
                    person_id = num1;
                    times.clear();
                    locs.clear();
                    days.clear();
                    lons.clear();
                    lats.clear();
                    has_work = false;
                    continue;
                }
                
                if (++counter > 100000) {
                    break;
                }
                
                // Best fit parameter search
                double best_ats = 999;
                double ats;
                int best_index1;
                int best_index2;
                int best_sf;
                int index1_num;
                int index2_num;
                bool need_move = false;
                vector<int> best_simu_time;
                vector<int> best_simu_at_home;
                index1_num = n1_num;
                index2_num = n2_num;
                
                // For non-commuter mode, measure time
                auto start = std::chrono::high_resolution_clock::now();
                
                for (int index1 = 0; index1 < index1_num; index1++) {
                    for (int index2 = 0; index2 < index2_num; index2++) {
                        day_end_at_home = 0;
                        day_not_end_at_home = 0;
                        current_time = times[0];
                        end_time = times[stay_num-1];
                        period_begin_time.clear(); // The time of the first event in each period
                        simulated_time.clear(); 
                        simulated_at_home.clear();
                        at_home = true;
                        at_work = false;
                        int current_work_index = 0;
                        
                        simulated_time.push_back(current_time);
                        simulated_at_home.push_back(1);
                        double pt;
                        double p_other_move;
                        double p_other_home;
                        
                        // The movement logic of the Markovian model
                        while (current_time < end_time) {

                            current_time += slot_interval + rand() % rand_range - ((int)(0.5 * rand_range));
                            
                            if (commuter_mode && current_work_index > max_work_index) {
                                need_move = false;
                                at_work = false;
                            }
                            
                            if (commuter_mode && current_work_index <= max_work_index) {
                                need_move = false;
                                if (current_time >= work_end[current_work_index]) {
                                    current_work_index++;
                                    if (at_work) {
                                        need_move = true;
                                    }
                                    at_work = false;
                                }
                                
                                if (current_work_index <= max_work_index && current_time > work_start[current_work_index]) {
                                    if (at_work == false) {
                                        // Go to work
                                        simulated_time.push_back(current_time);
                                        at_home = false;
                                        at_work = true;
                                        simulated_at_home.push_back(2);
                                        continue;
                                    }
                                }
                            }
                            
                            daily_slot = (int)((timeCorrect(current_time)) % secOneDay / slot_interval);
                            weekly_slot = ((int)((timeCorrect(current_time)) / secOneDay)) % 7;
                            
                            if (daily_slot == daily_slot_num - 1) {
                                if (at_home) {
                                    day_end_at_home++;
                                } else {
                                    day_not_end_at_home++;
                                }
                            }
                            
                            if (at_home) {
                                pt = nw * daily_activeness[daily_slot] * weekly_activeness[weekly_slot];
                                // From home to other
                                if ((rand() % 100000) / 100000.0 < pt) {
                                    simulated_time.push_back(current_time);
                                    at_home = false;
                                    simulated_at_home.push_back(0);
                                }
                                // Keep at home, do nothing


                            } else {
                                if (need_move) {
                                    p_other_move = 1;
                                } else {
                                    p_other_move = getOtherMovePr(daily_slot, weekly_slot, index1, nw);
                                }
                                
                                if ((rand() % 100000) / 100000.0 < p_other_move) {
                                    // Move to other place
                                    simulated_time.push_back(current_time);
                                    // Move to home or another other
                                    p_other_home = getOtherMoveToHomePr(daily_slot, weekly_slot, index2, nw);
                                    
                                    if ((rand() % 100000) / 100000.0 < p_other_home) {
                                        at_home = true;
                                        simulated_at_home.push_back(1);
                                    } else {
                                        simulated_at_home.push_back(0);
                                    }
                                }
                                // Else keep at the current other place, do nothing
                            }
                        }
                        
                        // Calculate the goodness of fit
                        ats = AreaTestStat(times, simulated_time, NBINS);
                        if (ats < best_ats) {
                            best_ats = ats;
                            best_index1 = index1;
                            best_index2 = index2;
                            best_simu_time = simulated_time;
                            best_simu_at_home = simulated_at_home;
                            best_day_end_at_home = day_end_at_home;
                            best_day_not_end_at_home = day_not_end_at_home;
                            best_nw = nw;
                        }
                    }
                }
                
                // Get execution time for non-commuter mode
                if (!commuter_mode) {
                    auto end = std::chrono::high_resolution_clock::now();
                    totalDuration = totalDuration + end - start;
                }
                
                // Run the simulation again with the best parameters
                int index1 = best_index1;
                int index2 = best_index2;
                
                day_end_at_home = 0;
                day_not_end_at_home = 0;
                current_time = times[0];
                end_time = times[stay_num-1];
                period_begin_time.clear();
                simulated_time.clear();
                simulated_at_home.clear();
                at_home = true;
                at_work = false;
                int current_work_index = 0;
                max_work_index = work_start.size() - 1;
                simulated_time.push_back(current_time);
                simulated_at_home.push_back(1);
                double pt;
                double p_other_move;
                double p_other_home;
                
                // The movement logic of the Markovian model
                while (current_time < end_time) {
                    current_time += slot_interval + rand() % rand_range - ((int)(0.5 * rand_range));
                    
                    if (commuter_mode && current_work_index > max_work_index) {
                        need_move = false;
                        at_work = false;
                    }
                    
                    if (commuter_mode && current_work_index <= max_work_index) {
                        need_move = false;
                        if (current_time >= work_end[current_work_index]) {
                            current_work_index++;
                            if (at_work) {
                                need_move = true;
                            }
                            at_work = false;
                        }
                        
                        if (current_work_index <= max_work_index && current_time > work_start[current_work_index]) {
                            if (at_work == false) {
                                // Go to work
                                simulated_time.push_back(current_time);
                                at_home = false;
                                at_work = true;
                                simulated_at_home.push_back(2);
                                continue;
                            }
                        }
                    }
                    
                    daily_slot = (int)((timeCorrect(current_time)) % secOneDay / slot_interval);
                    weekly_slot = ((int)((timeCorrect(current_time)) / secOneDay)) % 7;
                    
                    if (daily_slot == daily_slot_num - 1) {
                        if (at_home) {
                            day_end_at_home++;
                        } else {
                            day_not_end_at_home++;
                        }
                    }
                    
                    if (at_home) {
                        pt = nw * daily_activeness[daily_slot] * weekly_activeness[weekly_slot];
                        // From home to other
                        if ((rand() % 100000) / 100000.0 < pt) {
                            simulated_time.push_back(current_time);
                            at_home = false;
                            simulated_at_home.push_back(0);
                        }
                        // Keep at home, do nothing
                    } else {
                        if (need_move) {
                            p_other_move = 1;
                        } else {
                            p_other_move = getOtherMovePr(daily_slot, weekly_slot, index1, nw);
                        }
                        
                        if ((rand() % 100000) / 100000.0 < p_other_move) {
                            // Move to other place
                            simulated_time.push_back(current_time);
                            // Move to home or another other
                            p_other_home = getOtherMoveToHomePr(daily_slot, weekly_slot, index2, nw);
                            
                            if ((rand() % 100000) / 100000.0 < p_other_home) {
                                at_home = true;
                                simulated_at_home.push_back(1);
                            } else {
                                simulated_at_home.push_back(0);
                            }
                        }
                        // Else keep at the current other place, do nothing
                    }
                }
                
                best_simu_time = simulated_time;
                best_simu_at_home = simulated_at_home;
                best_day_end_at_home = day_end_at_home;
                best_day_not_end_at_home = day_not_end_at_home;
                best_nw = nw;
                
                day_end_at_home_total += best_day_end_at_home;
                day_not_end_at_home_total += best_day_not_end_at_home;
                
                // Count the daily number of activities for simulated data
                int current_day;
                int daily_count;
                int daily_location_count;
                int previous_day;
                
                int stay_num_simu;
                bool was_home;
                bool was_work;
                stay_num_simu = best_simu_time.size();
                current_day = double(timeCorrect(best_simu_time[0])) / secOneDay;
                daily_count = 1;
                daily_location_count = 1;
                
                if (best_simu_at_home[0] == 1) {
                    was_home = true;
                } else {
                    was_home = false;
                }
                
                if (best_simu_at_home[0] == 2) {
                    was_work = true;
                } else {
                    was_work = false;
                }
                
                int gap_days;
                for (int i = 1; i < stay_num_simu; i++) {
                    previous_day = current_day;
                    current_day = double(timeCorrect(best_simu_time[i])) / secOneDay;
                    
                    if (current_day == previous_day) {
                        daily_count++;
                        if (was_home == true && best_simu_at_home[i] == 1) {
                            // Already visited home
                        } else if(was_work == true && best_simu_at_home[i] == 2) {
                            // Already visited work
                        } else {
                            daily_location_count++;
                        }
                        
                        if (best_simu_at_home[i] == 1) {
                            was_home = true;
                        }
                        
                        if (best_simu_at_home[i] == 2) {
                            was_work = true;
                        }
                    } else {
                        gap_days = current_day - previous_day - 1;
                        if (gap_days > 0) {
                            for (int j = 0; j < gap_days; j++) {
                                fprintf(fout_id4, "%d %d\n", 0, 1);
                            }
                        }
                        
                        fprintf(fout_id4, "%d %d\n", daily_count, daily_location_count);
                        daily_count = 1;
                        daily_location_count = 1;
                        
                        if (best_simu_at_home[i] == 1) {
                            was_home = true;
                        } else {
                            was_home = false;
                        }
                        
                        if (best_simu_at_home[i] == 2) {
                            was_work = true;
                        } else {
                            was_work = false;
                        }
                    }
                }
                
                // Count the daily number of activities for real data
                current_day = double(timeCorrect(times[0])) / secOneDay;
                daily_count = 1;
                daily_location_count = 1;
                double total_daily_loc_count = 0;
                double total_day = 0;
                
                if (locs[0] == 1) {
                    was_home = true;
                } else {
                    was_home = false;
                }
                
                if (locs[0] == 2) {
                    was_work = true;
                } else {
                    was_work = false;
                }
                
                for (int i = 1; i < stay_num; i++) {
                    previous_day = current_day;
                    current_day = double(timeCorrect(times[i])) / secOneDay;
                    
                    if (current_day == previous_day) {
                        daily_count++;
                        if (was_home == true && locs[i] == 1) {
                            // Already visited home
                        } else if (was_work == true && locs[i] == 2) {
                            // Already visited work
                        } else {
                            daily_location_count++;
                        }
                        
                        if (locs[i] == 1) {
                            was_home = true;
                        }
                        
                        if (locs[i] == 2) {
                            was_work = true;
                        }
                    } else {
                        gap_days = current_day - previous_day - 1;
                        if (gap_days > 0) {
                            for (int j = 0; j < gap_days; j++) {
                                fprintf(fout_id3, "%d %d\n", 0, 1);
                            }
                        }
                        
                        fprintf(fout_id3, "%d %d\n", daily_count, daily_location_count);
                        total_daily_loc_count += daily_location_count;
                        total_day += 1;
                        daily_count = 1;
                        daily_location_count = 1;
                        
                        if (locs[i] == 1) {
                            was_home = true;
                        } else {
                            was_home = false;
                        }
                        
                        if (locs[i] == 2) {
                            was_work = true;
                        } else {
                            was_work = false;
                        }
                    }
                }
                
                // Output the empirical data
                for (int i = 0; i < stay_num; i++) {
                    fprintf(fout_id7, "%d %d %d %f %lld\n", person_id, times[i], locs[i], 
                            double(timeCorrect(times[i])) / secOneDay, locids[i]);
                }
                
                // Output the location id based on preferential return
                // Person id, time, if home, day, location id
                // Loc 0 is home, 1 is the first other place
                int visited_location = 0; // Home
                vector<int> visit_count;
                double visit_count_sum = 0;
                vector<double> visit_cdf;
                double p_return;
                double rand_num;
                // Use parameters rho and gamma from function arguments
                int prev_loc = -1;
                
                for (int i = 1; i < stay_num_simu; i++) {
                    if (best_simu_at_home[i] == 1) {
                        fprintf(fout_id5, "%d %d %d %f %d\n", person_id, best_simu_time[i], 1, 
                                double(timeCorrect(best_simu_time[i])) / secOneDay, 0);
                        prev_loc = 0;
                    } else if (best_simu_at_home[i] == 2) {
                        fprintf(fout_id5, "%d %d %d %f %d\n", person_id, best_simu_time[i], 2, 
                                double(timeCorrect(best_simu_time[i])) / secOneDay, 1);
                        prev_loc = 0;
                    } else {
                        if (visited_location < 2) {
                            visited_location++;
                            fprintf(fout_id5, "%d %d %d %f %d\n", person_id, best_simu_time[i], 0, 
                                    double(timeCorrect(best_simu_time[i])) / secOneDay, 2 + visited_location);
                            visit_count.push_back(1);
                            prev_loc = 1;
                        } else {
                            double r_return_unconditioned = 0.6 * (1 - rho * pow(visited_location, gamma));
                            p_return = r_return_unconditioned / (r_return_unconditioned + rho * pow(visited_location, gamma));
                            
                            if ((rand() % 9999999) / 9999999.0 < p_return) {
                                // Return
                                visit_count_sum = 0;
                                for (int j = 0; j < visited_location; j++) {
                                    visit_count_sum += visit_count[j];
                                }
                                
                                visit_cdf.clear();
                                visit_cdf.push_back(visit_count[0] / visit_count_sum);
                                
                                if (visited_location > 1) {
                                    for (int j = 1; j < visited_location; j++) {
                                        visit_cdf.push_back(visit_cdf[j-1] + visit_count[j] / visit_count_sum);
                                    }
                                }
                                
                                bool found_sign = false;
                                while (found_sign == false) {
                                    rand_num = (rand() % 9999999) / 9999999.0;
                                    for (int j = 0; j < visited_location; j++) {
                                        if (rand_num < visit_cdf[j] && j != prev_loc - 1) {
                                            ++visit_count[j];
                                            fprintf(fout_id5, "%d %d %d %f %d\n", person_id, best_simu_time[i], 0, 
                                                    double(timeCorrect(best_simu_time[i])) / secOneDay, j + 1 + 2);
                                            prev_loc = j + 1;
                                            found_sign = true;
                                            break;
                                        }
                                    }
                                }
                            } else {
                                // New location
                                visited_location++;
                                fprintf(fout_id5, "%d %d %d %f %d\n", person_id, best_simu_time[i], 0, 
                                        double(timeCorrect(best_simu_time[i])) / secOneDay, visited_location + 2);
                                visit_count.push_back(1);
                            }
                        }
                    }
                }
                
                if (person_id > 0) {
                    fprintf(fout_id6, "%d %d %f %f %f %f %f %f %d\n", best_index1, best_index2, best_nw,
                            total_daily_loc_count / total_day, home_lon, home_lat, work_lon, work_lat, person_id);
                }
                
                // Rescale real and best simulated times
                RescaleTime(times, true);
                RescaleTime(best_simu_time, false);
            }
            
            // Begin the new person
            person_id = num1;
            times.clear();
            locs.clear();
            days.clear();
            lons.clear();
            lats.clear();
            locids.clear();
            has_work = false;
            
            times.push_back(num2);
            locs.push_back(num3);
            days.push_back(num4);
            locids.push_back(num5);
            lons.push_back(num6);
            lats.push_back(num7);
            if (num3 == 2) {
                has_work = true;
            }
        }
    }
    
    // Output the results
    for (int i = 0; i < original_real_dt.size(); i++) {
        fprintf(fout_id1, "%f %f\n", original_real_dt[i], rescaled_real_dt[i]); 
    }
    
    for (int i = 0; i < original_simu_dt.size(); i++) {
        fprintf(fout_id2, "%f %f\n", original_simu_dt[i], rescaled_simu_dt[i]); 
    }
    
    fclose(fout_id1);
    fclose(fout_id2);
    fclose(fout_id3);
    fclose(fout_id4);
    fclose(fout_id5);
    fclose(fout_id6);
    fclose(fout_id7);
    
    // If non-commuter mode, output execution time
    if (!commuter_mode) {
        cout << "Total execution time for iterations: " << totalDuration.count() << " seconds" << endl;
    }
}

#ifdef DT_STANDALONE_EXEC
int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <StayRegionsFiltered.txt> <output_dir> <commuter_mode 0|1>\n";
        return 1;
    }
    std::string input_path  = argv[1];
    std::string output_dir  = argv[2];
    bool commuter_flag      = std::stoi(argv[3]) != 0;

    run_DT_simulation(input_path, output_dir, commuter_flag);
    return 0;
}
#endif  // DT_STANDALONE_EXEC

namespace py = pybind11;

PYBIND11_MODULE(DT_user_param, m) {
    m.doc() = "Python bindings for DT Model with extended parameters";
    m.def("run_DT_simulation", &run_DT_simulation,
        "Run the DT simulation model with extended parameter control",
        py::arg("input_path"),
        py::arg("output_dir"),
        py::arg("commuter_mode") = false,
        py::arg("min_num_stay") = 2,
        py::arg("max_num_stay") = 3000,
        py::arg("nw_thres") = 1.0,
        py::arg("slot_interval") = 600,
        py::arg("rho") = 0.6,
        py::arg("gamma") = -0.21
    );
}