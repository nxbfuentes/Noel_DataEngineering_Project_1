import os


def print_env_vars():
    env_vars = [
        "SERVER_NAME",
        "DATABASE_NAME",
        "DB_USERNAME",
        "DB_PASSWORD",
        "PORT",
        "LOGGING_SERVER_NAME",
        "LOGGING_DATABASE_NAME",
        "LOGGING_USERNAME",
        "LOGGING_PASSWORD",
        "LOGGING_PORT",
    ]

    for var in env_vars:
        print(f"{var}: {os.getenv(var)}")


if __name__ == "__main__":
    print_env_vars()
