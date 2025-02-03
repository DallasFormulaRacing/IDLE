# IDLE
A program to determine whether or not a car is idling given a stream of ECU data
## Setup
Install all program dependencies with `pip install -r requirements.txt`
## Usage
This program (contained in `real-idle.py`) assumes that all ECU data will be coming from a Redis stream called `ecu`, and while the car is not idling, these entries will be forwarded to a stream called `destination_stream`. It also will append a key called `og_timestamp` to each forwarded entry, with the value preserving the original timestamp for when the entry was received.

Additionally, when the car's idle status changes, an event will be sent to a stream called `events`, with keys called `event_name` and `timestamp`. The values for `event_name` are `start` for when the car stops idling, and `stop` for when the car begins idling again. 
## Testing
To specify a file to send test data from, place the CSV in the same directory, and change the value of the `can_file_path` variable in `send-test-data.py` to the CSV filename.

Then, simply run `python real-idle.py` and `python send-test-data.py` simultaneously, and the latter will simulate streaming data from the CSV as if it were being inserted into the stream in real time.


## Notes
The program **MOSTLY** does not really care about the key names of the events received from the `ecu` stream. However, since RPM is used to determine the idle state, events including the current RPM are assumed to provide it under the key name `rpm`.