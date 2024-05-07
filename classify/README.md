---
title: 
--- 

# HowTo

## Run Classification Process

```bash
./run
```

- first, we trigger the `insert.py` script which takes data from the rhetoric tables, breaks each text entry into chunks (1-2 sentences), and inserts each chunk into `classifications` table
- then, we trigger the `classify.py` script that sends each chunk through a prompt; a response to the prompt is generated vy chatgpt (via openai's API)

# Change Log

## 2024 Jan 10

- went from one text-based prompt that outputs "Yes/No" for each category, to a more complex prompting system that combines the categories into 3 sets (attack, policy outcome) and outputs a json response

## 2024 Apr 24

- slight modification to prompts (removed placeholder text from sample json)



