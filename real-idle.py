import time
from datetime import datetime

rpm_IDLE_CONST = 1000

# This simulates RPM values changing every 10 seconds
#Remove and replace with actual RPM reading when available
def get_simulated_rpm():

    current_time = int(time.time())

    # Every 10 seconds switch between idle and driving for testing
    if (current_time // 10) % 2 == 0:
        return 800   # idle
    else:
        return 2000  # driving


def isIdle():

    rpm = get_simulated_rpm()

    print(f"Current RPM: {rpm}")

    return rpm <= rpm_IDLE_CONST


def format_time(ms):
    return datetime.fromtimestamp(ms / 1000).strftime("%H:%M:%S")


idle_start_time = None
car_currently_idle = False


while True:

    current_ms = time.time_ns() // 1000000

    idle_status = isIdle()

    RPM = get_simulated_rpm()
    # Car just started idling
    if idle_status and not car_currently_idle:
        idle_start_time = current_ms
        car_currently_idle = True
        print("\nCar started idling")
        print(f"Current RPM: {RPM}")

    # Car stopped idling
    elif not idle_status and car_currently_idle:

        idle_end_time = current_ms

        start_clock = format_time(idle_start_time)
        end_clock = format_time(idle_end_time)

        print(f"\nIdle from {start_clock} to {end_clock}\n")
        print("Car started moving")
        print(f"Current RPM: {RPM}")
        car_currently_idle = False
        idle_start_time = None

    time.sleep(1)