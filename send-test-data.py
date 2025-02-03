from can import CSVReader
import time
import redis as redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
can_file_path = 'can-_ecu_76.csv'
last_message_timestamp = 0

# Wipe all old data in the test stream
def delete_all_entries(stream_name: str) -> None:
    response = r.xread(streams={stream_name: 0})
    for stream in response:
        stream_name, messages = stream
        for message in messages:
            r.xdel(stream_name, message[0])
delete_all_entries("ecu")

for message in CSVReader(can_file_path):
    if last_message_timestamp != 0:
        time.sleep(message.timestamp - last_message_timestamp)

    match message.arbitration_id:
        case 0x0CFFF048:
            rpm = int.from_bytes(message.data[0:2], byteorder='little')
            tps = int.from_bytes(message.data[2:4], byteorder='little') * 0.1
            fuel_open_time = int.from_bytes(message.data[4:6], byteorder='little') * 0.1
            ignition_angle = int.from_bytes(message.data[6:8], byteorder='little') * 0.1
            r.xadd("ecu", {"rpm": rpm, "tps": tps, "fuel_open_time": fuel_open_time, "ignition_angle": ignition_angle})

        case 0x0CFFF148:
            barometer = int.from_bytes(message.data[0:2], byteorder='little') * 0.01
            map = int.from_bytes(message.data[2:4], byteorder='little') * 0.01
            ecu_lambda = int.from_bytes(message.data[4:6], byteorder='little') * 0.01
            pressure_type = int.from_bytes(message.data[6:7], byteorder='little')
            r.xadd("ecu", {"barometer": barometer, "map": map, "ecu_lambda": ecu_lambda, "pressure_type": pressure_type})

        case 0x0CFFF248:
            analog_input_1 = int.from_bytes(message.data[0:2], byteorder='little') * 0.001
            analog_input_2 = int.from_bytes(message.data[2:4], byteorder='little') * 0.001
            analog_input_3 = int.from_bytes(message.data[4:6], byteorder='little') * 0.001
            analog_input_4 = int.from_bytes(message.data[6:8], byteorder='little') * 0.001
            r.xadd("ecu", {"analog_input_1": analog_input_1, "analog_input_2": analog_input_2, "analog_input_3": analog_input_3, "analog_input_4": analog_input_4})
        
        case 0x0CFFF348:
            analog_input_5 = int.from_bytes(message.data[0:2], byteorder='little') * 0.001
            analog_input_6 = int.from_bytes(message.data[2:4], byteorder='little') * 0.001
            analog_input_7 = int.from_bytes(message.data[4:6], byteorder='little') * 0.001
            analog_input_8 = int.from_bytes(message.data[6:8], byteorder='little') * 0.001
            r.xadd("ecu", {"analog_input_5": analog_input_5, "analog_input_6": analog_input_6, "analog_input_7": analog_input_7, "analog_input_8": analog_input_8})

        case 0x0CFFF448:
            frequency_1 = int.from_bytes(message.data[0:2], byteorder='little') * 0.2
            frequency_2 = int.from_bytes(message.data[2:4], byteorder='little') * 0.2
            frequency_3 = int.from_bytes(message.data[4:6], byteorder='little') * 0.2
            frequency_4 = int.from_bytes(message.data[6:8], byteorder='little') * 0.2
            r.xadd("ecu", {"frequency_1": frequency_1, "frequency_2": frequency_2, "frequency_3": frequency_3, "frequency_4": frequency_4})

        case 0x0CFFF548:
            battery_volt = int.from_bytes(message.data[0:2], byteorder='little') * 0.01
            air_temp = int.from_bytes(message.data[2:4], byteorder='little') * 0.1
            coolant_temp = int.from_bytes(message.data[4:6], byteorder='little') * 0.1
            temp_type = int.from_bytes(message.data[6:7], byteorder='little')
            r.xadd("ecu", {"battery_volt": battery_volt, "air_temp": air_temp, "coolant_temp": coolant_temp, "temp_type": temp_type})
        
        case 0x0CFFF648:
            analog_input_5_thermistor = int.from_bytes(message.data[0:2], byteorder='little') * 0.1
            analog_input_7_thermistor = int.from_bytes(message.data[2:4], byteorder='little') * 0.1
            version_major = int.from_bytes(message.data[4:5], byteorder='little')
            version_minor = int.from_bytes(message.data[5:6], byteorder='little')
            version_build = int.from_bytes(message.data[6:7], byteorder='little')
            r.xadd("ecu", {"analog_input_5_thermistor": analog_input_5_thermistor, "analog_input_7_thermistor": analog_input_7_thermistor, "version_major": version_major, "version_minor": version_minor, "version_build": version_build})

        case 0x0CFFF748:
            rpm_rate = int.from_bytes(message.data[0:2], byteorder='little')
            tps_rate = int.from_bytes(message.data[2:4], byteorder='little')
            map_rate = int.from_bytes(message.data[4:6], byteorder='little')
            maf_load_rate = int.from_bytes(message.data[6:8], byteorder='little') * 0.1
            r.xadd("ecu", {"rpm_rate": rpm_rate, "tps_rate": tps_rate, "map_rate": map_rate, "maf_load_rate": maf_load_rate})

        case 0x0CFFF848:
            lambda_1_measure = int.from_bytes(message.data[0:2], byteorder='little') * 0.01
            lambda_2_measure = int.from_bytes(message.data[2:4], byteorder='little') * 0.01
            target_lambda = int.from_bytes(message.data[4:6], byteorder='little') * 0.01
            r.xadd("ecu", {"lambda_1_measure": lambda_1_measure, "lambda_2_measure": lambda_2_measure, "target_lambda": target_lambda})

        case 0x0CFFF948:
            pwm_duty_cycles = []
            result = {}
            for duty_cycle in range(8):
                pwm_duty_cycles.append(int.from_bytes(message.data[duty_cycle:(duty_cycle + 2)], byteorder='little') * 0.5)
            for x in range (len(pwm_duty_cycles)):
                result[f'pwm_duty_cycle_{x + 1}'] = pwm_duty_cycles[x]
            r.xadd('ecu', result)

        case 0x0CFFFA48:
            percent_slip = int.from_bytes(message.data[0:2], byteorder='little') * 0.1
            driven_wheel_rate_of_change = int.from_bytes(message.data[2:4], byteorder='little') * 0.1
            desired_value = int.from_bytes(message.data[4:6], byteorder='little') * 0.1
            r.xadd("ecu", {"percent_slip": percent_slip, "driven_wheel_rate_of_change": driven_wheel_rate_of_change, "desired_value": desired_value})

        case 0x0CFFFB48:
            driven_avg_wheel_speed = int.from_bytes(message.data[0:2], byteorder='little') * 0.1
            non_driven_avg_wheel_speed = int.from_bytes(message.data[2:4], byteorder='little') * 0.1
            ignition_compensation = int.from_bytes(message.data[4:6], byteorder='little') * 0.1
            ignition_cut_percent = int.from_bytes(message.data[6:8], byteorder='little') * 0.1
            r.xadd("ecu", {"driven_avg_wheel_speed": driven_avg_wheel_speed, "non_driven_avg_wheel_speed": non_driven_avg_wheel_speed, "ignition_compensation": ignition_compensation, "ignition_cut_percent": ignition_cut_percent })

        case 0x0CFFFC48:
            driven_wheel_speed_1 = int.from_bytes(message.data[0:2], byteorder='little') * 0.1
            driven_wheel_speed_2 = int.from_bytes(message.data[2:4], byteorder='little') * 0.1
            non_driven_wheel_speed_1 = int.from_bytes(message.data[4:6], byteorder='little') * 0.1
            non_driven_wheel_speed_2 = int.from_bytes(message.data[6:8], byteorder='little') * 0.1
            r.xadd("ecu", {"driven_wheel_speed_1": driven_wheel_speed_1, "driven_wheel_speed_2": driven_wheel_speed_2, "non_driven_wheel_speed_1": non_driven_wheel_speed_1, "non_driven_wheel_speed_2": non_driven_wheel_speed_2})

        case 0x0CFFFD48:
            fuel_comp_accel = int.from_bytes(message.data[0:2], byteorder='little') * 0.1
            fuel_comp_starting = int.from_bytes(message.data[2:4], byteorder='little') * 0.1
            fuel_comp_air_temp = int.from_bytes(message.data[4:6], byteorder='little') * 0.1
            fuel_comp_coolant_temp = int.from_bytes(message.data[6:8], byteorder='little') * 0.1
            r.xadd("ecu", {"fuel_comp_accel": fuel_comp_accel, "fuel_comp_starting": fuel_comp_starting, "fuel_comp_air_temp": fuel_comp_air_temp, "fuel_comp_coolant_temp": fuel_comp_coolant_temp})

        case 0x0CFFFE48:
            fuel_comp_barometer = int.from_bytes(message.data[0:2], byteorder='little') * 0.1
            fuel_comp_map = int.from_bytes(message.data[2:4], byteorder='little') * 0.1
            r.xadd("ecu", {"fuel_comp_barometer": fuel_comp_barometer, "fuel_comp_map": fuel_comp_map})

        case 0x0CFFD048:
            ignition_comp_air_temp = int.from_bytes(message.data[0:2], byteorder='little') * 0.1
            ignition_comp_coolant_temp = int.from_bytes(message.data[2:4], byteorder='little') * 0.1
            ingition_comp_barometer = int.from_bytes(message.data[4:6], byteorder='little') * 0.1
            ignition_comp_map = int.from_bytes(message.data[6:8], byteorder='little') * 0.1
            r.xadd("ecu", {"ignition_comp_air_temp": ignition_comp_air_temp, "ignition_comp_coolant_temp": ignition_comp_coolant_temp, "ingition_comp_barometer": ingition_comp_barometer, "ignition_comp_map": ignition_comp_map})

    last_message_timestamp = message.timestamp