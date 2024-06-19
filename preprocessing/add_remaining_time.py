import pandas as pd
import numpy as np
import os
import pm4py

input_data_folder = r"C:\Users\49170\Documents\FAU\Diehl Seminar\Daten"
output_data_folder = r"C:\Users\49170\Documents\FAU\Diehl Seminar\results"

filenames = ["combined_file.csv"]
out_name="pre_processed.csv"
timestamp_col = 'insert_date'
columns_to_remove = ["update_date"]
case_id_col = "pcb_serial_number_str"

workplace_mapping = {
    "L1010103": "Laser(L1010103)",#
    "L2020101": "Testing(L2020101)",
    "L2020102": "Repair(L2020102)",
    "L2030101": "Assembly(L2030101)",
    "L2031101": "Assembly(L2031101)",
    "L2032101": "Assembly(L2032101)",
    "L2033101": "Assembly(L2033101)",
    "L2040101": "Soldering(L2040101)",
    "L2040111": "Soldering(L2040111)",
    "L2050101": "Inspection(L2050101)",
    "L2050102": "Repair(L2050102)",
    "L2060101": "Testing(L2060101)",
    "L2060102": "Repair(L2060102)",
    "L2080101": "Cutting(L2080101)",
    "L2080102": "Rebook(L2080102)",
    "L2080111": "Burn-in(L2080111)",
    "L2090101": "L2090101",
    "L3010101": "Assembly(L3010101)",
    "L3025101": "Assembly(L3025101)",
    "L3030101": "Assembly(L3030101)",
    "L3040101": "Inspection(L3040101)",
    "L3050101": "Assembly(L3050101)",
    "L3060101": "Scan(L3060101)",
    "L3070101": "Soldering Flux(L3070101)",
    "L3080100": "Soldering Heating(L3080100)",
    "L3080101": "Soldering Heating(L3080101)",
    "L3080102": "Soldering(L3080102)",
    "L3090101": "Inspection(L3090101)",
    "L3095101": "Assembly(L3095101)",
    "L3090102": "Repair(L3090102)",
    "L3040102": "Repair(L3040102)",
    "L3100101": "Testing(L3100101)",
    "L3100102": "Repair(L3100102)",
    "L3110101": "Potting(L3110101)",
    "L3120101": "Potting(L3120101)",
    "L3130101": "OvenIN(L3130101)",
    "L3140101": "OvenOUT(L3140101)",
    "L3150101": "Testing(L3150101)",
    "L3150102": "Repair(L3150102)",
    "L3160101": "Packing(L3160101)",
    "L3170101": "Packing(L3170101)",#
    'Unknown': 'Unknown'
}

#filter ganze cases (erste und letzte aktivität und alle cases die result state 2 habe)
#unknown untersuchen welcher zeitraum

def add_remtime_column(group):
    group = group.sort_values(timestamp_col, ascending=False)
    start_date = group[timestamp_col].iloc[-1]
    end_date = group[timestamp_col].iloc[0]

    elapsed = group[timestamp_col] - start_date
    elapsed = elapsed.fillna(0)
    group["elapsed"] = elapsed.apply(lambda x: float(x / np.timedelta64(1, 's')))  # s is for seconds

    remtime = end_date - group[timestamp_col]
    remtime = remtime.fillna(0)
    group["remtime"] = remtime.apply(lambda x: float(x / np.timedelta64(1, 's'))) # s is for seconds

    return group

for filename in filenames:
    print(filename)
    data = pd.read_csv(os.path.join(input_data_folder, filename), sep=",")
    data = data.drop(columns_to_remove, axis=1)
    data[timestamp_col] = pd.to_datetime(data[timestamp_col])
    
    data['workplace_number'] = data['workplace_number'].fillna('Unknown')
    data['workplace_number'] = data['workplace_number'].map(workplace_mapping)
    data.dropna(subset=['workplace_number'], inplace=True)
    
    result_2 = pm4py.filter_event_attribute_values(data,level="case",case_id_key="pcb_serial_number_str",attribute_key="result_state",values=[2],retain=True)
    data = pm4py.filter_start_activities(data,['Laser(L1010103)'],activity_key="workplace_number",case_id_key="pcb_serial_number_str",timestamp_key="insert_date")
    data = pm4py.filter_end_activities(data,['Packing(L3160101)','Packing(L3170101)'],activity_key="workplace_number",case_id_key="pcb_serial_number_str",timestamp_key="insert_date")

    data = pd.concat([data, result_2]).drop_duplicates().reset_index(drop=True)
    
    data = data.groupby(case_id_col).apply(add_remtime_column)
    data.to_csv(os.path.join(output_data_folder, out_name), sep=",", index=False)

