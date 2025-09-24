'''2/16/2025 | Working | This will run the LLM automatically and extract the JSON 
and convert into a dataframe'''
import urllib
from sqlalchemy import create_engine, text
import pandas as pd
import json
import re
import os
from openai import OpenAI

def prompt_openai(prompt_template):
    print("Step/1: Initialize API")
    print("Setting hardcoded API key")
    api_key = "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"
    print("Creating OpenAI client")
    client = OpenAI(api_key=api_key)
    
    print("Step/2: Send Query")
    print(f"Sending prompt: {prompt_template}")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful AI assistant."},
            {"role": "user", "content": prompt_template}
        ],
        temperature=0.2,
        max_tokens=5000
    )
    
    print("Step/3: Get Raw Response")
    result = response.choices[0].message.content.strip()
    print(f"Raw text length: {len(result)}")
    print(f"Raw text: {result}")
    return result

