import random
import time


def generate_temp():
    low = 10
    high = 40
    city = ["delhi", "mumbai", "bangalore", "chennai", "kochi"]
    return {"city": random.choice(city), "temperature": random.uniform(low, high),
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')}
