import sys, time, json, os, copy
import pandas as pd
import openai

# Exponential Backoff Decorator
def cautious_fetch(max_retries=5, wait_time=7):
    def decorator_retry(func):
        def wrapper_retry(*args, **kwargs):
            retries, current_wait_time = 0, wait_time
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"An error occurred: {e}")
                    print(f"Retrying in {current_wait_time} seconds...")
                    time.sleep(current_wait_time)
                    retries += 1
                    current_wait_time *= 3
            print("Exceeded maximum number of retries. Aborting.")
            return None
        return wrapper_retry
    return decorator_retry


# OpenAI
@cautious_fetch(max_retries=5, wait_time=7)
def chatgpt(message):
    messages = [{
        'role': 'user',
        'content': message,
    }]

    with openai.OpenAI() as client:
        response = client.chat.completions.create(
            # model = "gpt-3.5-turbo-1106",
            # model = "gpt-4-1106-preview",
            model = "gpt-4-turbo-2024-04-09",
            messages = messages,
            temperature = 0.8,
            # max_tokens = 1,
        )
        response = response.choices[0].message.content
    return response


