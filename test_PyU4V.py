import PyU4V
import datetime

conn = PyU4V.U4VConn()

print("Collecting Unisphere Version")
print("-----------------------------------")
version = conn.common.get_uni_version()
print(f"Unisphere version {version[0]}\n")

print("Collecting Array List")
print("-----------------------------------")
array_list = conn.common.get_array_list()
for i in array_list:
    print(f"- {i}")
print()

print("Confirming diagnostic data collection is enabled")
print("---------------------------------------------------")
test_data = []
fall_back = False
for i in array_list:
    try:
        pe = conn.performance.is_array_diagnostic_performance_registered()
    except AttributeError:  # Handle if we're using an older module version
        fall_back = True
        if "92" in version[1]:
            print("WARNING - Module version and Unisphere Version Mismatch")
        pe = conn.performance.is_array_performance_registered()

    print(f"Checking {i} - ", end='')
    if not pe:
        if fall_back:
            print("FAILED - Manually confirm diagnostics (version mismatch)")
        else:
            print("FAILED - Check if diagnostics are enabled")
    else:
        print("OK")

    test_data.append(i)
print()

print("Confirming Most Recent Data Point")
print("---------------------------------------------------")
for i in test_data:
    print(f"Checking {i} - ", end='')

    try:
        recent_ts = conn.performance.get_last_available_timestamp(array_id=i)
    except PyU4V.utils.exception.ResourceNotFoundException:
        print("Data not found, possibly remote array")
        continue
    recent = conn.performance.is_timestamp_current(recent_ts, minutes=10)
    (s, ms) = divmod(recent_ts, 1000)
    stamp = datetime.datetime.fromtimestamp(s)
    if recent:
        recency = "Recency acceptable"
    else:
        recency = ("Recency not within 10 minutes, "
                   "please run again in 5 minutes.")

    print(f"{stamp} - {recency}")

print()
print("Testing Completed")
