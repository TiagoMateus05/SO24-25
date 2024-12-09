import random
import string
import os
import glob

def random_key(length=5):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

def random_value(length=8):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def generate_read_command():
    num_keys = random.randint(1, 5)
    keys = [random_key() for _ in range(num_keys)]
    return f"READ [{','.join(keys)}]"

def generate_write_command():
    num_pairs = random.randint(1, 5)
    pairs = [(random_key(), random_value()) for _ in range(num_pairs)]
    pairs_str = ''.join([f"({k},{v})" for k, v in pairs])
    return f"WRITE [{pairs_str}]"

def generate_delete_command():
    num_keys = random.randint(1, 5)
    keys = [random_key() for _ in range(num_keys)]
    return f"DELETE [{','.join(keys)}]"

def generate_backup_command():
    return "BACKUP"

def generate_show_command():
    return "SHOW"

def generate_script(num_commands=10):
    commands = []
    for _ in range(num_commands):
        command_type = random.choice(['READ', 'WRITE', 'DELETE', 'BACKUP', 'SHOW'])
        if command_type == 'READ':
            commands.append(generate_read_command())
        elif command_type == 'WRITE':
            commands.append(generate_write_command())
        elif command_type == 'DELETE':
            commands.append(generate_delete_command())
        elif command_type == 'BACKUP':
            commands.append(generate_backup_command())
        elif command_type == 'SHOW':
            commands.append(generate_show_command())
    return '\n'.join(commands)

def remove_files(patterns):
    for pattern in patterns:
        for file_path in glob.glob(pattern):
            try:
                os.remove(file_path)
                print(f"Removed {file_path}")
            except OSError as e:
                print(f"Error removing {file_path}: {e}")

def write_scripts(num_files=5, num_commands_per_file=10):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Remove existing .job, .out, and .bck files
    remove_files(["*.job", "*.out", "*.bck"])
    
    for i in range(1, num_files + 1):
        script = generate_script(num_commands_per_file)
        file_path = os.path.join(script_dir, f"input{i}.job")
        with open(file_path, "w") as file:
            file.write(script)
            file.write("\n")
        print(f"Generated script for {file_path}:")
        print(script)

if __name__ == "__main__":
    write_scripts(num_files=5, num_commands_per_file=60)