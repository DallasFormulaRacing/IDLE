import redis as redis
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# isIdle: Return true if the car is idle and false if not
#         Takes the events from the past x seconds as a parameter
def isIdle(buffer):
    # Constant used to check if idle, change if necessary (may need to make funct to calculate this num later).
    rpm_IDLE_CONST = 1000

    rpm_buffer = r.xrange("ecu", min=buffer)
    rpm_sum = 0 
    rpm_count = 0

    if len(rpm_buffer) > 0:
        for message in rpm_buffer:
            data = message[1]
            if "rpm" in data:
                rpm_sum += int(data["rpm"])
                rpm_count += 1

    # Check if there are any buffered rpm events to check
    # Otherwise, obviously don't start dividing shit, and just assume it's idle
    if rpm_count > 0:
        rpm_average = rpm_sum / rpm_count
        print(f"Earliest event in RPM buffer: {rpm_buffer[0][0]}")
        print(f"Length of RPM buffer: {len(rpm_buffer)}")
        print(f"Average rpm: {rpm_average}")
        # Checks if idle by seeing if rpm average is below the specified const amount.
        print(f"Idling: {rpm_average <= rpm_IDLE_CONST}\n")
        return rpm_average <= rpm_IDLE_CONST

    return True

idling_since = 0
while True:
    current_ms = time.time_ns() // 1000000
    # rpm buffer should be 5 seconds long
    rpm_buffer_range = current_ms - (5 * 1000)
    idle_status = isIdle(rpm_buffer_range)
  
    # If the car was not idling for the past 5 seconds, continue to send data to destination stream.
    if idle_status == False:  # stops idling
        idling_since = 0
        r.xadd("events", {"event_name": "start", "time": current_ms})
        # Event buffer range should be 10 seconds long
        # And we forward this to the destination stream after the car finishes idling
        event_buffer_range = current_ms - (10 * 1000)
        event_buffer = r.xrange("ecu", min=event_buffer_range)
        for event in event_buffer:
            event_data = event[1]
            # Make sure to save the original timestamp somewhere
            # That's probably important
            event_data["og_timestamp"] = event[0]
            r.xadd("destination_stream", event_data)
    else:
        if idling_since != 0:
            idling_since = current_ms
        elif (current_ms - idling_since) > 15000:
            r.xadd("events", {"event_name": "stop", "time": idling_since})